import asyncio
import json
import logging
import os
import re
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from core import get_client, get_instances, proxy_parallel, templates

logger = logging.getLogger(__name__)
router = APIRouter()
templates.env.add_extension("jinja2.ext.do")

# ─────────────────────────────────────────────
#  設定
# ─────────────────────────────────────────────
RAPIDAPI_KEYS: List[str] = [k.strip() for k in os.environ.get("RAPIDAPI_KEYS", "").split(",") if k.strip()]
RAPIDAPI_HOST = os.environ.get("RAPIDAPI_HOST", "")
ZERNIO_BASE = os.environ.get("ZERNIO_BASE", "")
SIA_STREAM_BASE = os.environ.get("SIA_STREAM_BASE", "")
SIA_INFO_BASE = os.environ.get("SIA_INFO_BASE", "https://siatube.com").rstrip("/")
SENNIN_BASE = os.environ.get(
    "SENNIN_BASE", "https://discerning-adventure-production-ebfc.up.railway.app"
).rstrip("/")
INV_LOCAL = os.environ.get("INVIDIOUS_LOCAL", "1") != "0"

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")

TTL_INFO = 300.0
TTL_COMMENTS = 600.0
TTL_STREAMS = 60.0      # 署名付きURLは短命

ZERNIO_FORMATS = {
    2: {"quality": "360p", "kind": "combined", "codec": "H.264"},
    4: {"quality": "720p", "kind": "video-only", "codec": "H.264"},
    5: {"quality": "1080p", "kind": "video-only", "codec": "H.264"},
}

_QORDER = {"2160p": 0, "1440p": 1, "1080p": 2, "720p": 3, "480p": 4, "360p": 5, "240p": 6, "144p": 7,
           "hd2160": 0, "hd1440": 1, "hd1080": 2, "hd720": 3, "large": 4, "medium": 5, "small": 6, "tiny": 7}

_STATS: Dict[str, Dict[str, Any]] = {
    n: {"success": 0, "failure": 0, "avg_time": 0.0}
    for n in ("invidious", "piped", "sia", "sennin", "rapidapi", "zernio", "sia_stream")
}
_STATS_LOCK = asyncio.Lock()


async def _record(api: str, ok: bool, dur: float) -> None:
    async with _STATS_LOCK:
        st = _STATS.get(api)
        if st is None:
            return
        if ok:
            st["success"] += 1
            n = st["success"]
            st["avg_time"] = st["avg_time"] * (n - 1) / n + dur / n
        else:
            st["failure"] += 1


# ─────────────────────────────────────────────
#  キャッシュ / 同時リクエスト集約 / 競争
# ─────────────────────────────────────────────
_CACHE: Dict[str, Tuple[Any, float]] = {}
_CACHE_MAX = 400
_INFLIGHT: Dict[str, "asyncio.Future"] = {}
_rr = 0


def _now() -> float:
    return time.time()


def _cget(key: str) -> Optional[Any]:
    e = _CACHE.get(key)
    if not e:
        return None
    if e[1] < _now():
        _CACHE.pop(key, None)
        return None
    return e[0]


def _cset(key: str, val: Any, ttl: float) -> None:
    if len(_CACHE) >= _CACHE_MAX:
        _CACHE.pop(min(_CACHE, key=lambda k: _CACHE[k][1]), None)
    _CACHE[key] = (val, _now() + ttl)


async def _dedupe(key: str, factory):
    fut = _INFLIGHT.get(key)
    if fut is not None:
        try:
            return await asyncio.shield(fut)
        except Exception:
            pass
    fut = asyncio.get_event_loop().create_future()
    _INFLIGHT[key] = fut
    try:
        res = await factory()
        if not fut.done():
            fut.set_result(res)
        return res
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
            fut.exception()
        raise
    finally:
        _INFLIGHT.pop(key, None)


async def _race(tasks: List["asyncio.Task"], timeout: float, valid) -> Optional[Any]:
    """最初に valid を満たした結果を返し、残りはキャンセルする。"""
    pending = set(tasks)
    result = None
    deadline = _now() + timeout
    try:
        while pending and result is None:
            remain = deadline - _now()
            if remain <= 0:
                break
            done, pending = await asyncio.wait(pending, timeout=remain, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            for t in done:
                try:
                    r = t.result()
                except Exception:
                    continue
                if r is not None and valid(r):
                    result = r
                    break
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return result


async def _get_json(url: str, timeout: float = 8.0, **kw) -> Any:
    cl = await get_client()
    r = await cl.get(url, timeout=httpx.Timeout(timeout), **kw)
    r.raise_for_status()
    return r.json()


# ─────────────────────────────────────────────
#  小物ヘルパー
# ─────────────────────────────────────────────
def _int(x: Any) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


def _codec(vc: str) -> str:
    vc = (vc or "").lower()
    if vc.startswith("avc1") or vc == "h264":
        return "H.264"
    if vc.startswith("vp9") or vc.startswith("vp09"):
        return "VP9"
    if vc.startswith("av01") or vc == "av1":
        return "AV1"
    if vc.startswith("mp4a"):
        return "AAC"
    if vc == "opus":
        return "Opus"
    return vc


def _parse_mime(mime: str) -> Tuple[str, str]:
    container = "webm" if "webm" in mime else ("m4a" if ("audio/mp4" in mime or "m4a" in mime) else "mp4")
    m = re.search(r'codecs=["\']?([^"\',;]+)', mime)
    return container, _codec(m.group(1).strip().lower() if m else "")


def _qkey(f: dict) -> int:
    for k in ("qualityLabel", "quality"):
        val = f.get(k) or ""
        m = re.match(r"(\d+p)", val)
        if m and m.group(1) in _QORDER:
            return _QORDER[m.group(1)]
        if val in _QORDER:
            return _QORDER[val]
    return 99


def _sub_text(n: int) -> str:
    if not n:
        return ""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{round(n / 1_000)}K"
    return str(n)


def _rel_date(s: str) -> str:
    try:
        d = (date.today() - date.fromisoformat(s[:10])).days
        if d < 0:
            return s
        if d == 0:
            return "今日"
        if d < 30:
            return f"{d} 日前"
        if d < 365:
            return f"{d // 30} ヶ月前"
        return f"{d // 365} 年前"
    except Exception:
        return s


def _abs(url: str, base: str) -> str:
    if url and url.startswith("/") and base:
        return base.rstrip("/") + url
    return url


def _entry(url, itag, mime, quality, label, fps, size, bitrate, container, enc, **extra) -> dict:
    d = {"url": url, "itag": str(itag), "type": mime, "quality": quality, "qualityLabel": label,
         "fps": fps, "size": size, "bitrate": str(bitrate), "container": container, "encoding": enc}
    d.update(extra)
    return d


def _thumb(video_id: str, given: str = "") -> str:
    if given and not given.startswith("/"):
        return given
    return f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg" if video_id else ""


# ─────────────────────────────────────────────
#  動画情報 ①: Invidious（core.proxy_parallel）
# ─────────────────────────────────────────────
def _override(force_instance: Optional[str]) -> Optional[list]:
    return [force_instance.rstrip("/")] if force_instance else None


async def _info_invidious(v: str, force_instance: Optional[str] = None) -> Optional[dict]:
    t0 = _now()
    try:
        res = await proxy_parallel("video", f"/api/v1/videos/{v}", override_instances=_override(force_instance))
        d = res.get("data")
        if isinstance(d, dict) and not d.get("error") and (d.get("title") or d.get("videoId")):
            d = dict(d)
            d["api_used"] = "invidious"
            d["_instance"] = res.get("used_instance", "")
            await _record("invidious", True, _now() - t0)
            return d
    except Exception as e:
        logger.debug(f"invidious info {v}: {e}")
    await _record("invidious", False, _now() - t0)
    return None


# ─────────────────────────────────────────────
#  動画情報 ②: Piped（生JSONは情報・ストリームで共用）
# ─────────────────────────────────────────────
async def _piped_raw(v: str) -> Optional[dict]:
    key = f"piped_raw:{v}"
    hit = _cget(key)
    if hit is not None:
        return hit

    async def _one(inst: str):
        try:
            d = await _get_json(f"{inst}/streams/{v}", timeout=12.0)
            if isinstance(d, dict) and not d.get("error") and d.get("title"):
                return d
        except Exception:
            pass
        return None

    tasks = [asyncio.create_task(_one(i)) for i in PIPED_INSTANCES]
    data = await _race(tasks, 12.0, lambda d: isinstance(d, dict))
    if data:
        _cset(key, data, TTL_INFO)
    return data


def _piped_to_info(p: dict) -> dict:
    up = p.get("uploaderUrl", "") or ""
    author_id = ""
    if "/channel/" in up:
        author_id = up.split("/channel/")[-1].strip("/")
    elif up.startswith("/@"):
        author_id = up[1:]
    elif up.startswith("/c/"):
        author_id = up[3:]
    avatar = p.get("uploaderAvatar", "")
    thumb = p.get("thumbnailUrl", "")

    related = []
    for s in p.get("relatedStreams") or []:
        if s.get("type") != "stream":
            continue
        u = s.get("url", "")
        vid = u.split("?v=")[-1].split("&")[0] if "?v=" in u else ""
        if not vid:
            continue
        sup = s.get("uploaderUrl", "") or ""
        related.append({
            "videoId": vid, "video_id": vid, "title": s.get("title", ""),
            "author": s.get("uploaderName", ""),
            "authorId": sup.split("/channel/")[-1].strip("/") if "/channel/" in sup else "",
            "lengthSeconds": s.get("duration", 0) or 0, "viewCount": s.get("views", 0) or 0,
            "view_count_text": s.get("uploadedDate") or "",
            "thumbnail": s.get("thumbnail", ""),
        })
    sub = p.get("uploaderSubscriberCount") or 0
    ud = p.get("uploadDate", "")
    return {
        "title": p.get("title", ""), "author": p.get("uploader", "") or "", "authorId": author_id,
        "authorIcon": avatar, "viewCount": p.get("views", 0) or 0, "likeCount": p.get("likes", 0) or 0,
        "publishedText": _rel_date(ud) if ud else "", "description": "",
        "descriptionHtml": p.get("description", ""), "lengthSeconds": p.get("duration", 0) or 0,
        "subCount": sub, "subCountText": _sub_text(sub) or "非公開",
        "authorThumbnails": [{"url": avatar, "width": 48, "height": 48}] if avatar else [],
        "thumbnail": thumb, "recommendedVideos": related, "api_used": "piped",
    }


async def _info_piped(v: str) -> Optional[dict]:
    t0 = _now()
    raw = await _piped_raw(v)
    await _record("piped", raw is not None, _now() - t0)
    return _piped_to_info(raw) if raw else None


# ─────────────────────────────────────────────
#  動画情報 ③④: Sia / Sennin
# ─────────────────────────────────────────────
def _related_norm(raw_rel: List[Any]) -> List[Dict[str, Any]]:
    out = []
    for i in raw_rel:
        if not isinstance(i, dict):
            continue
        th = i.get("thumbnail", "")
        if not th and isinstance(i.get("thumbnails"), list) and i["thumbnails"]:
            th = i["thumbnails"][0].get("url", "")
        out.append({"video_id": i.get("videoId") or i.get("id"), "title": i.get("title"),
                    "author": i.get("channelName") or i.get("author"),
                    "view_count_text": i.get("viewCountText"), "thumbnail": th})
    return out


def _sennin_norm(d: dict) -> dict:
    ai = d.get("author", {}) if isinstance(d.get("author"), dict) else {}
    desc = d.get("description", {})
    if isinstance(desc, dict):
        text = desc.get("text", "")
        html = desc.get("formatted") or text.replace("\n", "<br>")
    else:
        text = str(desc or "")
        html = text.replace("\n", "<br>")
    rel = d.get("Related-videos", {})
    raw_rel = rel.get("relatedVideos", []) if isinstance(rel, dict) else []
    return {
        "title": d.get("title", ""), "author": ai.get("name") or "", "authorId": ai.get("id") or "",
        "authorIcon": ai.get("thumbnail") or "", "subCountText": ai.get("subscribers") or "非公開",
        "viewCount": d.get("views") or d.get("extended_stats", {}).get("views_original", 0),
        "likeCount": d.get("likes", 0), "description": text, "descriptionHtml": html,
        "recommendedVideos": _related_norm(raw_rel), "thumbnail": d.get("thumbnail", ""),
    }


async def _info_sennin(v: str) -> Optional[dict]:
    t0 = _now()
    try:
        d = await _get_json(f"{SENNIN_BASE}/api/video/{v}", timeout=4.0)
        if d and not d.get("unavailable"):
            n = _sennin_norm(d)
            if n.get("title"):
                n["api_used"] = "sennin"
                await _record("sennin", True, _now() - t0)
                return n
    except Exception as e:
        logger.debug(f"sennin info {v}: {e}")
    await _record("sennin", False, _now() - t0)
    return None


async def _info_sia(v: str) -> Optional[dict]:
    t0 = _now()
    try:
        d = await _get_json(f"{SIA_INFO_BASE}/api/video/{v}", timeout=3.5)
        ai = d.get("author", {}) if isinstance(d.get("author"), dict) else {}
        name = ai.get("name") or d.get("uploader") or ""
        if name:
            desc = d.get("description", {})
            text = desc.get("text", "") if isinstance(desc, dict) else str(desc or "")
            rel = d.get("Related-videos") or d.get("relatedVideos") or {}
            raw_rel = rel.get("relatedVideos", []) if isinstance(rel, dict) else (rel if isinstance(rel, list) else [])
            await _record("sia", True, _now() - t0)
            return {
                "title": d.get("title", ""), "author": name, "authorId": ai.get("id", ""),
                "authorIcon": ai.get("thumbnail", ""), "subCountText": ai.get("subscribers", "非公開"),
                "viewCount": d.get("views", 0), "likeCount": d.get("likes", 0),
                "description": text, "descriptionHtml": text.replace("\n", "<br>"),
                "recommendedVideos": _related_norm(raw_rel), "thumbnail": d.get("thumbnail", ""),
                "api_used": "sia",
            }
    except Exception as e:
        logger.debug(f"sia info {v}: {e}")
    await _record("sia", False, _now() - t0)
    return None


_INFO_SOURCES = {
    "invidious": lambda v, fi: _info_invidious(v, fi),
    "piped": lambda v, fi: _info_piped(v),
    "sia": lambda v, fi: _info_sia(v),
    "sennin": lambda v, fi: _info_sennin(v),
}


async def fetch_video_info(v: str, force_instance: Optional[str] = None,
                           api: Optional[str] = None, nocache: bool = False) -> Optional[dict]:
    """api 指定ならそのソースのみ。未指定なら全ソース並行で最速採用。"""
    ckey = f"info:{v}:{force_instance or ''}:{api or 'auto'}"
    if not nocache:
        hit = _cget(ckey)
        if hit is not None:
            return hit

    async def _do():
        if api in _INFO_SOURCES:
            return await _INFO_SOURCES[api](v, force_instance)
        tasks = [asyncio.create_task(f(v, force_instance)) for f in _INFO_SOURCES.values()]
        return await _race(tasks, 8.0,
                           lambda r: isinstance(r, dict) and (r.get("title") or r.get("videoId")))

    res = await (_do() if nocache else _dedupe(ckey, _do))
    if res:
        _cset(ckey, res, TTL_INFO)
    return res


# ─────────────────────────────────────────────
#  ストリーム: 各ソース → 共通形式
#    {"formatStreams": [音付き], "adaptiveFormats": [映像のみ/音声のみ], "_source": str}
# ─────────────────────────────────────────────
def _norm_invidious(d: dict, base: str = "") -> dict:
    fs = [dict(f, url=_abs(f.get("url", ""), base), itag=str(f.get("itag", "")))
          for f in d.get("formatStreams", []) if f.get("url")]
    af = [dict(f, url=_abs(f.get("url", ""), base), itag=str(f.get("itag", "")))
          for f in d.get("adaptiveFormats", []) if f.get("url")]
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "invidious"}


def _norm_rapidapi(raw: dict) -> dict:
    fs, af = [], []
    for key, dst, is_adaptive in (("formats", fs, False), ("adaptiveFormats", af, True)):
        for f in raw.get(key, []):
            if not f.get("url"):
                continue
            mime = f.get("mimeType", "")
            c, e = _parse_mime(mime)
            w, h = f.get("width", 0), f.get("height", 0)
            label = f.get("qualityLabel") or ("" if is_adaptive else f.get("quality", ""))
            dst.append(_entry(f["url"], f.get("itag", ""), mime, f.get("quality", ""), label,
                              f.get("fps", 0 if is_adaptive else 30), f"{w}x{h}" if w and h else "",
                              f.get("bitrate", 0), c, e))
    fs.sort(key=_qkey)
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "rapidapi"}


def _norm_sia_stream(raw: dict) -> dict:
    fs, af = [], []
    st = raw.get("streams", {})
    for s in st.get("muxed", []):
        if s.get("streamUrl"):
            note, ext = s.get("formatNote", ""), s.get("ext", "mp4")
            w, h = s.get("width", 0), s.get("height", 0)
            fs.append(_entry(s["streamUrl"], s.get("formatId", ""), f"video/{ext}", note, note,
                             s.get("fps", 25), f"{w}x{h}" if w and h else "", _int(s.get("tbr")),
                             ext, _codec(s.get("vcodec", ""))))
    for s in st.get("videoOnly", []):
        if s.get("streamUrl"):
            note, ext = s.get("formatNote", ""), s.get("ext", "mp4")
            w, h = s.get("width", 0), s.get("height", 0)
            af.append(_entry(s["streamUrl"], s.get("formatId", ""), f"video/{ext}", note, note,
                             s.get("fps", 30), f"{w}x{h}" if w and h else "", _int(s.get("tbr")),
                             ext, _codec(s.get("vcodec", ""))))
    for s in st.get("audioOnly", []):
        if s.get("streamUrl"):
            ext = s.get("ext", "webm")
            af.append(_entry(s["streamUrl"], s.get("formatId", ""), f"audio/{ext}", s.get("formatNote", ""),
                             "", 0, "", _int(s.get("tbr")), "m4a" if ext in ("m4a", "mp4") else ext,
                             _codec(s.get("acodec", ""))))
    if not fs:  # ライブ HLS
        seen = set()
        for s in raw.get("m3u8", {}).get("list", []):
            url, h, w, fid = s.get("streamUrl", ""), s.get("height") or 0, s.get("width") or 0, s.get("formatId")
            if not url or not h or not fid or (fid, h) in seen:
                continue
            seen.add((fid, h))
            fs.append(_entry(url, fid, f"video/{s.get('ext') or 'mp4'}", f"{h}p", f"{h}p", s.get("fps") or 30,
                             f"{w}x{h}" if w else "", _int(s.get("tbr")), s.get("ext") or "mp4",
                             _codec(s.get("vcodec", "")), isHls=True))
    fs.sort(key=_qkey)
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "sia_stream"}


def _norm_piped_stream(d: dict) -> dict:
    fs, af = [], []
    for s in d.get("videoStreams", []) or []:
        url = s.get("url")
        if not url:
            continue
        fmt = (s.get("format") or "").upper()
        if fmt == "HLS":
            continue
        ext = "webm" if "WEBM" in fmt else "mp4"
        q = str(s.get("quality", ""))
        w, h = s.get("width", 0), s.get("height", 0)
        e = _entry(url, s.get("itag", ""), f"video/{ext}", q, q, s.get("fps", 30),
                   f"{w}x{h}" if w and h else "", s.get("bitrate", 0), ext, _codec(s.get("codec", "")))
        (af if s.get("videoOnly", True) else fs).append(e)
    for s in d.get("audioStreams", []) or []:
        url = s.get("url")
        if not url:
            continue
        fmt = (s.get("format") or "").upper()
        ext = "m4a" if ("M4A" in fmt or "MP4" in fmt) else "webm"
        af.append(_entry(url, s.get("itag", ""), f"audio/{ext}", s.get("quality", ""), "", 0, "",
                         s.get("bitrate", 0), ext, _codec(s.get("codec", "")),
                         language=(s.get("audioTrackLocale") or "")[:2]))
    if d.get("hls") and not fs:
        fs.append(_entry(d["hls"], "hls", "application/x-mpegURL", "auto", "auto", 30, "", 0, "m3u8", "", isHls=True))
    fs.sort(key=_qkey)
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "piped"}


async def _stream_invidious(v: str, force_instance: Optional[str]) -> Optional[dict]:
    t0 = _now()
    path = f"/api/v1/videos/{v}" + ("?local=true" if INV_LOCAL else "")
    try:
        res = await proxy_parallel("video", path, override_instances=_override(force_instance),
                                   prefer_valid_stream=True, no_cache=True)
        n = _norm_invidious(res["data"], res.get("used_instance", ""))
        if n["formatStreams"] or n["adaptiveFormats"]:
            await _record("invidious", True, _now() - t0)
            return n
    except Exception as e:
        logger.debug(f"invidious stream {v}: {e}")
    await _record("invidious", False, _now() - t0)
    return None


async def _stream_piped(v: str) -> Optional[dict]:
    raw = await _piped_raw(v)
    if not raw:
        return None
    n = _norm_piped_stream(raw)
    return n if (n["formatStreams"] or n["adaptiveFormats"]) else None


async def _stream_rapidapi(v: str) -> Optional[dict]:
    global _rr
    if not RAPIDAPI_KEYS or not RAPIDAPI_HOST:
        return None
    t0 = _now()
    tried = set()
    for _ in range(len(RAPIDAPI_KEYS)):
        k = RAPIDAPI_KEYS[_rr % len(RAPIDAPI_KEYS)]
        _rr = (_rr + 1) % len(RAPIDAPI_KEYS)
        if k in tried:
            break
        tried.add(k)
        try:
            cl = await get_client()
            rp = await cl.get(f"https://{RAPIDAPI_HOST}/dl", params={"id": v},
                              headers={"X-RapidAPI-Key": k, "X-RapidAPI-Host": RAPIDAPI_HOST},
                              timeout=httpx.Timeout(18.0))
            if rp.status_code == 429 or rp.status_code >= 500:
                continue
            rp.raise_for_status()
            raw = rp.json()
            if raw.get("status") not in ("OK", None) and "formats" not in raw and "adaptiveFormats" not in raw:
                continue
            n = _norm_rapidapi(raw)
            if n["formatStreams"] or n["adaptiveFormats"]:
                await _record("rapidapi", True, _now() - t0)
                return n
        except Exception as e:
            logger.debug(f"rapidapi {v}: {e}")
    await _record("rapidapi", False, _now() - t0)
    return None


async def _stream_zernio(v: str) -> Optional[dict]:
    if not ZERNIO_BASE:
        return None
    t0 = _now()

    async def _one(cl: httpx.AsyncClient, fid: int):
        ck = f"zernio:{v}:{fid}"
        hit = _cget(ck)
        if hit:
            return hit
        try:
            rp = await cl.get(f"{ZERNIO_BASE}{v}&formatId={fid}", headers={"User-Agent": UA})
            loc = rp.headers.get("location", "")
            if not loc:
                return None
            m = ZERNIO_FORMATS[fid]
            e = _entry(loc, fid, "video/mp4", m["quality"], m["quality"], 30, "", 0, "mp4", m["codec"])
            _cset(ck, e, TTL_STREAMS)
            return e
        except Exception:
            return None

    async with httpx.AsyncClient(timeout=httpx.Timeout(18.0), follow_redirects=False) as cl:
        ents = await asyncio.gather(*[_one(cl, f) for f in ZERNIO_FORMATS])
    fs, af = [], []
    for e, fid in zip(ents, ZERNIO_FORMATS):
        if e:
            (fs if ZERNIO_FORMATS[fid]["kind"] == "combined" else af).append(e)
    ok = bool(fs or af)
    await _record("zernio", ok, _now() - t0)
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "zernio"} if ok else None


async def _stream_sia(v: str) -> Optional[dict]:
    if not SIA_STREAM_BASE:
        return None
    t0 = _now()
    try:
        d = await _get_json(SIA_STREAM_BASE + v, timeout=18.0, headers={"User-Agent": UA})
        n = _norm_sia_stream(d)
        if n["formatStreams"] or n["adaptiveFormats"]:
            await _record("sia_stream", True, _now() - t0)
            return n
    except Exception as e:
        logger.debug(f"sia stream {v}: {e}")
    await _record("sia_stream", False, _now() - t0)
    return None


_STREAM_SOURCES = ("invidious", "piped", "rapidapi", "zernio", "sia")


def _launch_stream(name: str, v: str, fi: Optional[str]):
    return {
        "invidious": lambda: _stream_invidious(v, fi),
        "piped": lambda: _stream_piped(v),
        "rapidapi": lambda: _stream_rapidapi(v),
        "zernio": lambda: _stream_zernio(v),
        "sia": lambda: _stream_sia(v),
    }[name]()


# ─────────────────────────────────────────────
#  共通形式 → テンプレ用 streamUrls / videoUrls
# ─────────────────────────────────────────────
def _pick_audio(af: List[dict], video_container: str) -> str:
    audios = [f for f in af if (f.get("type") or "").startswith("audio") and f.get("url")]
    if not audios:
        return ""
    want = "m4a" if video_container == "mp4" else "webm"
    pool = [a for a in audios if a.get("container") == want] or audios

    def _is_ja(a: dict) -> bool:
        if (a.get("language") or "").startswith("ja"):
            return True
        tr = a.get("audioTrack")
        return isinstance(tr, dict) and str(tr.get("id", "")).startswith("ja")

    pool = [a for a in pool if _is_ja(a)] or pool
    return max(pool, key=lambda a: _int(a.get("bitrate"))).get("url", "")


def build_stream_lists(norm: dict) -> Dict[str, List]:
    fs = norm.get("formatStreams", [])
    af = norm.get("adaptiveFormats", [])
    stream_urls: List[dict] = []
    for f in sorted(fs, key=_qkey):
        if f.get("url"):
            stream_urls.append({"url": f["url"], "resolution": f.get("qualityLabel") or f.get("quality"),
                                "format": "hls" if f.get("isHls") else "mp4/mixed", "audioUrl": ""})
    videos = sorted([f for f in af if (f.get("type") or "").startswith("video") and f.get("url")], key=_qkey)
    for f in videos:
        cont = "webm" if f.get("container") == "webm" else "mp4"
        audio = _pick_audio(af, cont)
        if audio:
            stream_urls.append({"url": f["url"], "resolution": f.get("qualityLabel") or f.get("quality"),
                                "format": f"{cont}/videoOnly", "audioUrl": audio})
    video_urls = [f["url"] for f in fs if f.get("url")] or [f["url"] for f in videos]
    return {"streamUrls": stream_urls, "videoUrls": video_urls}


def extract_invidious_streams(v_data: Dict[str, Any]) -> Dict[str, List]:
    if not v_data:
        return {"streamUrls": [], "videoUrls": []}
    return build_stream_lists(_norm_invidious(v_data, v_data.get("_instance", "")))


async def fetch_fastest_stream_urls(v: str, api: Optional[str] = None,
                                    force_instance: Optional[str] = None) -> Dict[str, Any]:
    """api 指定ならそのソースのみ。未指定なら全ソース並行で最速採用。失敗時は RuntimeError。"""
    ckey = f"streams:{v}:{api or 'auto'}:{force_instance or ''}"
    hit = _cget(ckey)
    if hit is not None:
        return hit

    async def _do():
        if api in _STREAM_SOURCES:
            return await _launch_stream(api, v, force_instance)
        tasks = [asyncio.create_task(_launch_stream(n, v, force_instance)) for n in _STREAM_SOURCES]
        return await _race(tasks, 15.0,
                           lambda r: isinstance(r, dict) and (r.get("formatStreams") or r.get("adaptiveFormats")))

    norm = await _dedupe(f"stream:{ckey}", _do)
    if not norm:
        raise RuntimeError("all stream sources failed")
    result = {**build_stream_lists(norm), "stream_api_used": norm.get("_source", "unknown")}
    if result["streamUrls"] or result["videoUrls"]:
        _cset(ckey, result, TTL_STREAMS)
    return result


# ─────────────────────────────────────────────
#  コメント
# ─────────────────────────────────────────────
def _norm_sennin_comments(data: dict) -> List[dict]:
    out = []
    for c in data.get("comments", []) if isinstance(data, dict) else []:
        if not isinstance(c, dict):
            continue
        ai = c.get("author", {}) if isinstance(c.get("author"), dict) else {}
        li = c.get("likes", {}) if isinstance(c.get("likes"), dict) else {}
        ri = c.get("replies", {}) if isinstance(c.get("replies"), dict) else {}
        name = ai.get("name") or (c.get("author") if isinstance(c.get("author"), str) else "")
        icon = ai.get("avatar") or c.get("authorIcon") or ""
        text = c.get("text") or c.get("content") or ""
        out.append({
            "commentId": c.get("commentId", ""), "author": name,
            "authorId": ai.get("channelId") or c.get("authorId") or "",
            "authorIcon": icon, "authorThumbnail": icon, "avatar": icon,
            "authorThumbnails": [{"url": icon}] if icon else [],
            "content": text, "contentHtml": text.replace("\n", "<br>"),
            "publishedTime": c.get("publishedTime", ""), "publishedText": c.get("publishedTime", ""),
            "likeCount": li.get("count", 0) if li else c.get("likes", 0),
            "replyCount": ri.get("count", 0) if ri else c.get("replies", 0),
            "isCreator": ai.get("creator", False), "isVerified": ai.get("verified", False),
        })
    return out


def _norm_comment(c: dict) -> dict:
    item = dict(c)
    ao = item.get("author")
    icon = ""
    if isinstance(ao, dict):
        item["author"] = ao.get("name", "")
        icon = ao.get("avatar") or ao.get("authorIcon") or item.get("avatar", "")
        item["authorId"] = ao.get("channelId", "")
    else:
        th = item.get("authorThumbnails", [])
        if th and isinstance(th, list):
            icon = th[-1].get("url", "")
    icon = icon or item.get("authorIcon") or item.get("avatar", "")
    item["authorIcon"] = item["authorThumbnail"] = item["avatar"] = icon
    if not isinstance(item.get("authorThumbnails"), list):
        item["authorThumbnails"] = [{"url": icon}] if icon else []
    text = item.get("text") or item.get("content") or ""
    item.setdefault("contentHtml", text.replace("\n", "<br>"))
    item.setdefault("content", text)
    pub = item.get("publishedTime") or item.get("published") or item.get("publishedText", "")
    item["publishedTime"] = item["publishedText"] = pub
    lk = item.get("likes")
    if isinstance(lk, dict):
        item["likeCount"] = lk.get("count", 0)
    return item


def process_comments(data: Any) -> List[dict]:
    if isinstance(data, Exception) or not data:
        return []
    if isinstance(data, dict) and data.get("success") is True and isinstance(data.get("comments"), list):
        return _norm_sennin_comments(data)
    comments = data.get("comments", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    return [_norm_comment(c) for c in comments if isinstance(c, dict)]


async def fetch_comments(v: str, force_instance: Optional[str] = None, api: Optional[str] = None) -> Any:
    ckey = f"comments:{v}:{api or 'auto'}:{force_instance or ''}"
    hit = _cget(ckey)
    if hit is not None:
        return hit

    async def _inv():
        try:
            res = await proxy_parallel("comments", f"/api/v1/comments/{v}",
                                       override_instances=_override(force_instance))
            d = res.get("data")
            if isinstance(d, dict) and not d.get("error") and d.get("comments"):
                return d
        except Exception as e:
            logger.debug(f"invidious comments {v}: {e}")
        return None

    async def _sen():
        try:
            d = await _get_json(f"{SENNIN_BASE}/api/comments/{v}", timeout=5.0)
            if isinstance(d, dict) and d.get("comments"):
                return d
        except Exception as e:
            logger.debug(f"sennin comments {v}: {e}")
        return None

    if api == "sennin":
        res = await _sen()
    elif api == "invidious":
        res = await _inv()
    else:
        res = await _race([asyncio.create_task(_inv()), asyncio.create_task(_sen())], 7.0,
                          lambda r: isinstance(r, dict))
    if res:
        _cset(ckey, res, TTL_COMMENTS)
    return res or {}


async def _fetch_playlist(list_id: Optional[str], force_instance: Optional[str]) -> Optional[dict]:
    if not list_id:
        return None
    try:
        res = await proxy_parallel("playlist", f"/api/v1/playlists/{list_id}",
                                   override_instances=_override(force_instance))
        d = res.get("data")
        if isinstance(d, dict) and not d.get("error"):
            return d
    except Exception as e:
        logger.debug(f"playlist {list_id}: {e}")
    return None


# ─────────────────────────────────────────────
#  ルーター
# ─────────────────────────────────────────────
async def _error_page(request: Request, timeout: bool):
    if timeout:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    try:
        insts = await get_instances("video")
    except Exception:
        insts = []
    return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": insts})


def _desc(v_data: dict) -> str:
    return v_data.get("descriptionHtml") or (v_data.get("description") or "").replace("\n", "<br>")


@router.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts_player(
    request: Request, v: str,
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None),
    stream_api: Optional[str] = Query(None),
    api: Optional[str] = Query(None),
):
    r_info, r_stream = info_api or api, stream_api or api
    try:
        video_data, stream_data, comment_data = await asyncio.gather(
            fetch_video_info(v, force_instance, r_info),
            fetch_fastest_stream_urls(v, r_stream, force_instance),
            fetch_comments(v, force_instance, r_info),
            return_exceptions=True,
        )
        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_data if isinstance(stream_data, dict) else {}
        if not v_data and not s_data:
            raise RuntimeError("info and streams both failed")

        video_urls = s_data.get("videoUrls") or extract_invidious_streams(v_data).get("videoUrls", [])
        info_used = v_data.get("api_used", "unknown")
        return templates.TemplateResponse("short.html", {
            "request": request, "videoid": v,
            "video_title": v_data.get("title", ""), "videourls": video_urls,
            "author": v_data.get("author", ""), "view_count": v_data.get("viewCount", 0),
            "like_count": v_data.get("likeCount", 0), "description": _desc(v_data),
            "comments": process_comments(comment_data),
            "info_api_used": info_used, "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": info_used,
        })
    except httpx.TimeoutException:
        logger.error(f"timeout in shorts {v}")
        return await _error_page(request, True)
    except Exception as e:
        logger.error(f"error in shorts {v}: {e}")
        return await _error_page(request, False)


@router.get("/watch", response_class=HTMLResponse)
async def watch(
    request: Request,
    v: str = Query(...),
    list_id: Optional[str] = Query(None, alias="list"),
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None),
    stream_api: Optional[str] = Query(None),
    api: Optional[str] = Query(None),
    nocache: bool = Query(False),
):
    r_info, r_stream = info_api or api, stream_api or api
    try:
        video_data, stream_res, comment_data, playlist_data = await asyncio.gather(
            fetch_video_info(v, force_instance, r_info, nocache),
            fetch_fastest_stream_urls(v, r_stream, force_instance),
            fetch_comments(v, force_instance, r_info),
            _fetch_playlist(list_id, force_instance),
            return_exceptions=True,
        )
        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_res if isinstance(stream_res, dict) else {}
        p_data = playlist_data if isinstance(playlist_data, dict) else {}
        if not v_data and not s_data:
            raise RuntimeError("info and streams both failed")

        playlist_videos = [{"videoId": i.get("videoId"), "title": i.get("title"), "author": i.get("author")}
                           for i in p_data.get("videos", []) if isinstance(i, dict)]

        stream_urls = s_data.get("streamUrls", [])
        video_urls = s_data.get("videoUrls", [])
        if not stream_urls and v_data:
            inv = extract_invidious_streams(v_data)
            stream_urls, video_urls = inv["streamUrls"], inv["videoUrls"]

        recommended = []
        for rec in v_data.get("recommendedVideos", []) or []:
            if not isinstance(rec, dict):
                continue
            vid = rec.get("video_id") or rec.get("videoId")
            th = rec.get("thumbnail") or ((rec.get("videoThumbnails") or [{}])[0].get("url", ""))
            recommended.append({
                "video_id": vid, "title": rec.get("title"), "author": rec.get("author"),
                "view_count_text": rec.get("view_count_text") or rec.get("viewCountText"),
                "thumbnail": _thumb(vid, th),
            })

        author_icon = v_data.get("authorIcon") or ""
        if not author_icon:
            th = v_data.get("authorThumbnails") or []
            author_icon = _abs(th[-1].get("url", ""), v_data.get("_instance", "")) if th else ""

        info_used = v_data.get("api_used", "unknown")
        response = templates.TemplateResponse("watch.html", {
            "request": request, "videoid": v,
            "video_title": v_data.get("title", ""),
            "videourls": video_urls, "streamUrls": stream_urls,
            "author": v_data.get("author", ""), "author_id": v_data.get("authorId", ""),
            "author_icon": author_icon,
            "subscribers_count": v_data.get("subCountText") or "非公開",
            "view_count": v_data.get("viewCount", 0), "like_count": v_data.get("likeCount", 0),
            "description": _desc(v_data), "recommended_videos": recommended,
            "comments": process_comments(comment_data),
            "youtube_url": f"https://www.youtube.com/watch?v={v}",
            "info_api_used": info_used, "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": info_used,
            "playlist_id": list_id, "playlist_title": p_data.get("title", ""),
            "playlist_videos": playlist_videos,
        })

        try:
            history = json.loads(request.cookies.get("history", "[]"))
            history = [h for h in history if h.get("videoId") != v]
            history.append({"videoId": v, "title": v_data.get("title", ""),
                            "author": v_data.get("author", ""), "added_at": datetime.now().isoformat()})
            response.set_cookie("history", json.dumps(history[-50:]), max_age=2592000,
                                httponly=True, samesite="Lax")
        except Exception as e:
            logger.debug(f"history update failed: {e}")
        return response

    except httpx.TimeoutException:
        logger.error(f"timeout in watch {v}")
        return await _error_page(request, True)
    except Exception as e:
        logger.error(f"error in watch {v}: {e}")
        return await _error_page(request, False)


@router.get("/api/watch-info/{video_id}")
async def api_watch_info(video_id: str, api: Optional[str] = None, nocache: bool = False):
    data = await fetch_video_info(video_id, api=api, nocache=nocache)
    if not data:
        return JSONResponse({"error": "動画情報の取得に失敗しました"}, status_code=502)
    return JSONResponse(data)


@router.get("/api/watch-streams/{video_id}")
async def api_watch_streams(video_id: str, api: Optional[str] = None):
    try:
        return JSONResponse(await fetch_fastest_stream_urls(video_id, api=api))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@router.get("/api/stats")
async def get_api_stats():
    async with _STATS_LOCK:
        return {"stats": {k: dict(v) for k, v in _STATS.items()}, "timestamp": datetime.now().isoformat()}
