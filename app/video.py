import asyncio
import json
import logging
import os
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from core import (
    get_client,
    proxy_parallel,
    get_video_back_instances,
    templates,
)

logger = logging.getLogger(__name__)
router = APIRouter()
try:
    templates.env.add_extension("jinja2.ext.do")
except Exception:
    pass

# ─────────────────────────────────────────────
#  設定（環境変数。未設定のソースは自動スキップ）
#   RAPIDAPI_KEYS / RAPIDAPI_HOST : RapidAPI (/dl)
#   ZERNIO_BASE     : 例 "https://.../?id="
#   SIA_STREAM_BASE : 例 "https://.../"  (末尾に video_id)
# ─────────────────────────────────────────────
RAPIDAPI_KEYS = [k.strip() for k in os.environ.get("RAPIDAPI_KEYS", "").split(",") if k.strip()]
RAPIDAPI_HOST = os.environ.get("RAPIDAPI_HOST", "")
ZERNIO_BASE = os.environ.get("ZERNIO_BASE", "")
SIA_STREAM_BASE = os.environ.get("SIA_STREAM_BASE", "")
SIA_INFO_BASE = os.environ.get("SIA_INFO_BASE", "https://siatube.com").rstrip("/")
SENNIN_BASE = os.environ.get(
    "SENNIN_BASE", "https://discerning-adventure-production-ebfc.up.railway.app"
).rstrip("/")

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")

TTL_INFO, TTL_STREAM, TTL_COMMENTS = 300, 60, 600

ZERNIO_FORMATS = {
    2: ("360p", "combined", "H.264"),
    4: ("720p", "video-only", "H.264"),
    5: ("1080p", "video-only", "H.264"),
}
QORDER = {"2160p": 0, "1440p": 1, "1080p": 2, "720p": 3, "480p": 4, "360p": 5,
          "240p": 6, "144p": 7, "hd2160": 0, "hd1440": 1, "hd1080": 2, "hd720": 3,
          "large": 4, "medium": 5, "small": 6, "tiny": 7}

_STATS: Dict[str, Dict[str, Any]] = {
    n: {"success": 0, "failure": 0, "avg_time": 0.0}
    for n in ("invidious", "piped", "sia", "sennin", "rapidapi", "zernio", "sia_stream")
}
_STATS_LOCK = asyncio.Lock()
_CACHE: Dict[str, Tuple[Any, float]] = {}
_rr = 0


# ─────────────────────────────────────────────
#  基盤
# ─────────────────────────────────────────────
def _cget(k):
    e = _CACHE.get(k)
    if not e:
        return None
    if e[1] < time.time():
        _CACHE.pop(k, None)
        return None
    return e[0]


def _cset(k, v, ttl):
    if len(_CACHE) >= 400:
        _CACHE.pop(min(_CACHE, key=lambda x: _CACHE[x][1]), None)
    _CACHE[k] = (v, time.time() + ttl)


async def _record(api: str, ok: bool, dt: float):
    async with _STATS_LOCK:
        s = _STATS.get(api)
        if not s:
            return
        if ok:
            s["success"] += 1
            n = s["success"]
            s["avg_time"] = s["avg_time"] * (n - 1) / n + dt / n
        else:
            s["failure"] += 1


async def _race(coros, timeout: float, valid):
    """全コルーチンを並行実行し、最初に valid を満たした結果を返す。残りはキャンセル。"""
    tasks = [asyncio.ensure_future(c) for c in coros]
    pending = set(tasks)
    deadline = time.time() + timeout
    result = None
    try:
        while pending and result is None:
            remain = deadline - time.time()
            if remain <= 0:
                break
            done, pending = await asyncio.wait(pending, timeout=remain,
                                               return_when=asyncio.FIRST_COMPLETED)
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


async def _timed(name: str, coro):
    t0 = time.time()
    try:
        r = await coro
    except Exception as e:
        logger.debug(f"{name} error: {e}")
        r = None
    await _record(name, r is not None, time.time() - t0)
    return r


def _codec(vc: str) -> str:
    vc = (vc or "").lower()
    if vc.startswith("avc1") or vc == "h264": return "H.264"
    if vc.startswith("vp9") or vc.startswith("vp09"): return "VP9"
    if vc.startswith("av01") or vc == "av1": return "AV1"
    if vc.startswith("mp4a"): return "AAC"
    if vc == "opus": return "Opus"
    return vc


def _mime(mime: str) -> Tuple[str, str]:
    c = "webm" if "webm" in mime else ("m4a" if ("audio/mp4" in mime or "m4a" in mime) else "mp4")
    cs = ""
    for q in ('codecs="', "codecs='"):
        if q in mime:
            cs = mime.split(q)[1].rstrip(q[-1]).split(",")[0].strip()
            break
    return c, _codec(cs)


def _entry(url, itag, mime, quality, label, fps, size, bitrate, container, enc, **extra):
    d = {"url": url, "itag": str(itag), "type": mime, "quality": quality,
         "qualityLabel": label, "fps": fps, "size": size, "bitrate": str(bitrate),
         "container": container, "encoding": enc}
    d.update(extra)
    return d


def _sub_text(n: int) -> str:
    if not n: return ""
    if n >= 1_000_000: return f"{n / 1_000_000:.1f}M"
    if n >= 1_000: return f"{round(n / 1_000)}K"
    return str(n)


def _rel_date(s: str) -> str:
    try:
        d = (date.today() - date.fromisoformat(s[:10])).days
        if d < 0: return s
        if d == 0: return "今日"
        if d < 30: return f"{d} 日前"
        if d < 365: return f"{d // 30} ヶ月前"
        return f"{d // 365} 年前"
    except Exception:
        return s


# ─────────────────────────────────────────────
#  Invidious（core.proxy_parallel 経由）
#  1回のリクエストで 情報 + ストリーム + おすすめ が取れる
# ─────────────────────────────────────────────
def _valid_info(d) -> bool:
    return isinstance(d, dict) and not d.get("error") and bool(d.get("title") or d.get("videoId"))


async def _invidious_raw(v: str, force_instance: Optional[str] = None) -> Optional[dict]:
    key = f"inv_raw:{v}:{force_instance or ''}"
    hit = _cget(key)
    if hit is not None:
        return hit
    override = [force_instance.rstrip("/")] if force_instance else None
    try:
        res = await asyncio.wait_for(
            proxy_parallel(
                "video", f"/api/v1/videos/{v}",
                prefer_valid_stream=True,       # 再生可能URLを持つインスタンスを優先
                override_instances=override,
            ),
            timeout=15.0,
        )
        data = res.get("data")
        if _valid_info(data):
            data = dict(data)
            data["api_used"] = "invidious"
            _cset(key, data, TTL_INFO)
            return data
    except Exception as e:
        logger.debug(f"invidious raw error {v}: {e}")
    return None


# ─────────────────────────────────────────────
#  Piped（生JSON1回取得 → 情報/ストリーム共用）
# ─────────────────────────────────────────────
async def _piped_raw(v: str) -> Optional[dict]:
    key = f"piped_raw:{v}"
    hit = _cget(key)
    if hit is not None:
        return hit

    async def one(inst: str):
        cl = await get_client()
        r = await cl.get(f"{inst}/streams/{v}", timeout=httpx.Timeout(12.0))
        r.raise_for_status()
        d = r.json()
        return d if isinstance(d, dict) and not d.get("error") and d.get("title") else None

    data = await _race([one(i) for i in PIPED_INSTANCES], 12.0, lambda d: isinstance(d, dict))
    if data:
        _cset(key, data, TTL_INFO)
    return data


def _piped_info(p: dict) -> dict:
    up = p.get("uploaderUrl", "") or ""
    aid = ""
    if "/channel/" in up: aid = up.split("/channel/")[-1].strip("/")
    elif up.startswith("/@"): aid = up[1:]
    elif up.startswith("/c/"): aid = up[3:]
    avatar, thumb = p.get("uploaderAvatar", ""), p.get("thumbnailUrl", "")
    rel = []
    for s in p.get("relatedStreams") or []:
        if s.get("type") != "stream": continue
        u = s.get("url", "")
        vid = u.split("?v=")[-1].split("&")[0] if "?v=" in u else ""
        if not vid: continue
        th = s.get("thumbnail", "")
        rel.append({"video_id": vid, "videoId": vid, "title": s.get("title", ""),
                    "author": s.get("uploaderName", ""),
                    "view_count_text": s.get("uploadedDate") or "", "thumbnail": th})
    sub = p.get("uploaderSubscriberCount") or 0
    return {
        "title": p.get("title", ""), "author": p.get("uploader", "") or "",
        "authorId": aid, "authorIcon": avatar,
        "viewCount": p.get("views", 0) or 0, "likeCount": p.get("likes", 0) or 0,
        "publishedText": _rel_date(p.get("uploadDate", "")),
        "description": "", "descriptionHtml": p.get("description", ""),
        "subCountText": _sub_text(sub) or "非公開", "thumbnail": thumb,
        "recommendedVideos": rel, "api_used": "piped",
    }


def _piped_streams(d: dict) -> dict:
    fs, af = [], []
    for s in d.get("videoStreams") or []:
        url = s.get("url")
        if not url: continue
        fmt = (s.get("format") or "").upper()
        q = str(s.get("quality", ""))
        w, h = s.get("width", 0), s.get("height", 0)
        ext = "mp4" if ("MP4" in fmt or "MPEG" in fmt) else "webm"
        e = _entry(url, s.get("itag", ""), f"video/{ext}", q, q, s.get("fps", 30),
                   f"{w}x{h}" if w and h else "", s.get("bitrate", 0), ext, _codec(s.get("codec", "")))
        if s.get("videoOnly", True): af.append(e)
        elif fmt != "HLS": fs.append(e)
    for s in d.get("audioStreams") or []:
        url = s.get("url")
        if not url: continue
        fmt = (s.get("format") or "").upper()
        ext = "m4a" if ("M4A" in fmt or "MP4" in fmt) else "webm"
        af.append(_entry(url, s.get("itag", ""), f"audio/{ext}", s.get("quality", ""), "", 0, "",
                         s.get("bitrate", 0), ext, _codec(s.get("codec", "")),
                         language=(s.get("audioTrackLocale") or "")[:2]))
    if d.get("hls") and not fs:
        fs.append(_entry(d["hls"], "hls", "application/x-mpegURL", "auto", "auto", 30, "", 0,
                         "m3u8", "", isHls=True))
    fs.sort(key=lambda f: QORDER.get(f["quality"], 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "piped"}


# ─────────────────────────────────────────────
#  Sia / Sennin（情報）
# ─────────────────────────────────────────────
def _related(raw_rel) -> List[dict]:
    out = []
    for i in raw_rel or []:
        if not isinstance(i, dict): continue
        th = i.get("thumbnail", "")
        if not th and isinstance(i.get("thumbnails"), list) and i["thumbnails"]:
            th = i["thumbnails"][0].get("url", "")
        out.append({"video_id": i.get("videoId") or i.get("id"), "title": i.get("title"),
                    "author": i.get("channelName") or i.get("author"),
                    "view_count_text": i.get("viewCountText"), "thumbnail": th})
    return out


def _flex_info(d: dict, api: str) -> Optional[dict]:
    """Sia / Sennin 共通形式（author: {name,id,thumbnail,subscribers}）を正規化。"""
    if not isinstance(d, dict) or d.get("unavailable"): return None
    ai = d.get("author", {}) if isinstance(d.get("author"), dict) else {}
    name = ai.get("name") or d.get("uploader") or ""
    if not name or not d.get("title"): return None
    desc = d.get("description", {})
    if isinstance(desc, dict):
        text = desc.get("text", "")
        html = desc.get("formatted") or text.replace("\n", "<br>")
    else:
        text = str(desc or "")
        html = text.replace("\n", "<br>")
    rel = d.get("Related-videos") or d.get("relatedVideos") or {}
    raw_rel = rel.get("relatedVideos", []) if isinstance(rel, dict) else (rel if isinstance(rel, list) else [])
    return {
        "title": d.get("title", ""), "author": name, "authorId": ai.get("id", ""),
        "authorIcon": ai.get("thumbnail", ""),
        "subCountText": ai.get("subscribers") or "非公開",
        "viewCount": d.get("views") or d.get("extended_stats", {}).get("views_original", 0),
        "likeCount": d.get("likes", 0), "description": text, "descriptionHtml": html,
        "recommendedVideos": _related(raw_rel), "thumbnail": d.get("thumbnail", ""),
        "api_used": api,
    }


async def _get_json(url: str, timeout: float, **kw):
    cl = await get_client()
    r = await cl.get(url, timeout=httpx.Timeout(timeout), **kw)
    r.raise_for_status()
    return r.json()


async def _sia_info(v: str):
    return _flex_info(await _get_json(f"{SIA_INFO_BASE}/api/video/{v}", 4.0), "sia")


async def _sennin_info(v: str):
    return _flex_info(await _get_json(f"{SENNIN_BASE}/api/video/{v}", 5.0), "sennin")


# ─────────────────────────────────────────────
#  動画情報 統合
# ─────────────────────────────────────────────
async def fetch_video_info(v: str, force_instance: Optional[str] = None,
                           api: Optional[str] = None, nocache: bool = False) -> Optional[dict]:
    ckey = f"info:{v}:{api or 'auto'}:{force_instance or ''}"
    if not nocache:
        hit = _cget(ckey)
        if hit is not None:
            return hit

    async def piped():
        raw = await _piped_raw(v)
        return _piped_info(raw) if raw else None

    src = {
        "invidious": lambda: _timed("invidious", _invidious_raw(v, force_instance)),
        "piped": lambda: _timed("piped", piped()),
        "sia": lambda: _timed("sia", _sia_info(v)),
        "sennin": lambda: _timed("sennin", _sennin_info(v)),
    }
    if api in src:
        result = await src[api]()
    else:
        result = await _race([f() for f in src.values()], 10.0, _valid_info)

    if result:
        _cset(ckey, result, TTL_INFO)
    return result


# ─────────────────────────────────────────────
#  ストリーム各ソース → 共通形式
# ─────────────────────────────────────────────
def _inv_streams(d: dict) -> dict:
    def fix(f):
        f = dict(f)
        f["itag"] = str(f.get("itag", ""))
        f.setdefault("qualityLabel", f.get("quality", ""))
        return f
    return {"formatStreams": [fix(f) for f in d.get("formatStreams", []) if f.get("url")],
            "adaptiveFormats": [fix(f) for f in d.get("adaptiveFormats", []) if f.get("url")],
            "_source": "invidious"}


def _rapid_streams(raw: dict) -> dict:
    fs, af = [], []
    for key, dst in (("formats", fs), ("adaptiveFormats", af)):
        for f in raw.get(key, []):
            m = f.get("mimeType", "")
            c, e = _mime(m)
            w, h = f.get("width", 0), f.get("height", 0)
            dst.append(_entry(f.get("url", ""), f.get("itag", ""), m, f.get("quality", ""),
                              f.get("qualityLabel", f.get("quality", "")), f.get("fps", 30),
                              f"{w}x{h}" if w and h else "", f.get("bitrate", 0), c, e))
    fs.sort(key=lambda f: QORDER.get(f["quality"], 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "rapidapi"}


def _sia_streams(raw: dict) -> dict:
    fs, af = [], []
    st = raw.get("streams", {})
    for s in st.get("muxed", []):
        if not s.get("streamUrl"): continue
        n, ext = s.get("formatNote", ""), s.get("ext", "mp4")
        w, h = s.get("width", 0), s.get("height", 0)
        fs.append(_entry(s["streamUrl"], s.get("formatId", ""), f"video/{ext}", n, n, s.get("fps", 25),
                         f"{w}x{h}" if w and h else "", int(s.get("tbr", 0) or 0), ext, _codec(s.get("vcodec", ""))))
    for s in st.get("videoOnly", []):
        if not s.get("streamUrl"): continue
        n, ext = s.get("formatNote", ""), s.get("ext", "mp4")
        w, h = s.get("width", 0), s.get("height", 0)
        af.append(_entry(s["streamUrl"], s.get("formatId", ""), f"video/{ext}", n, n, s.get("fps", 30),
                         f"{w}x{h}" if w and h else "", int(s.get("tbr", 0) or 0), ext, _codec(s.get("vcodec", ""))))
    for s in st.get("audioOnly", []):
        if not s.get("streamUrl"): continue
        ext = s.get("ext", "webm")
        af.append(_entry(s["streamUrl"], s.get("formatId", ""), f"audio/{ext}", s.get("formatNote", ""), "",
                         0, "", int(s.get("tbr", 0) or 0), ext, _codec(s.get("acodec", ""))))
    m3 = raw.get("m3u8", {}).get("list", [])
    if m3 and not fs:
        seen = set()
        for s in m3:
            url, h, w, fid = s.get("streamUrl", ""), s.get("height") or 0, s.get("width") or 0, s.get("formatId")
            if not url or not h or not fid or (fid, h) in seen: continue
            seen.add((fid, h))
            q = f"{h}p"
            fs.append(_entry(url, fid, f"video/{s.get('ext') or 'mp4'}", q, q, s.get("fps") or 30,
                             f"{w}x{h}" if w else "", int(s.get("tbr", 0) or 0), s.get("ext") or "mp4",
                             _codec(s.get("vcodec", "")), isHls=True))
    fs.sort(key=lambda f: QORDER.get(f["quality"], 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "sia_stream"}


def _has_streams(n) -> bool:
    return isinstance(n, dict) and bool(n.get("formatStreams") or n.get("adaptiveFormats"))


async def _s_invidious(v, force_instance):
    d = await _invidious_raw(v, force_instance)
    n = _inv_streams(d) if d else None
    return n if _has_streams(n) else None


async def _s_piped(v):
    raw = await _piped_raw(v)
    n = _piped_streams(raw) if raw else None
    return n if _has_streams(n) else None


async def _s_sia(v):
    if not SIA_STREAM_BASE: return None
    cl = await get_client()
    r = await cl.get(SIA_STREAM_BASE + v, headers={"User-Agent": UA}, timeout=httpx.Timeout(18.0))
    if r.status_code != 200: return None
    n = _sia_streams(r.json())
    return n if _has_streams(n) else None


async def _s_rapid(v):
    global _rr
    if not RAPIDAPI_KEYS or not RAPIDAPI_HOST: return None
    cl = await get_client()
    for _ in range(len(RAPIDAPI_KEYS)):
        k = RAPIDAPI_KEYS[_rr % len(RAPIDAPI_KEYS)]
        _rr = (_rr + 1) % len(RAPIDAPI_KEYS)
        try:
            r = await cl.get(f"https://{RAPIDAPI_HOST}/dl", params={"id": v},
                             headers={"X-RapidAPI-Key": k, "X-RapidAPI-Host": RAPIDAPI_HOST},
                             timeout=httpx.Timeout(18.0))
            if r.status_code == 429 or r.status_code >= 500: continue
            r.raise_for_status()
            raw = r.json()
            n = _rapid_streams(raw)
            if _has_streams(n): return n
        except Exception as e:
            logger.debug(f"rapidapi error: {e}")
    return None


async def _s_zernio(v):
    if not ZERNIO_BASE: return None

    async def one(cl, fid):
        q, kind, codec = ZERNIO_FORMATS[fid]
        try:
            r = await cl.get(f"{ZERNIO_BASE}{v}&formatId={fid}", headers={"User-Agent": UA})
            loc = r.headers.get("location", "")
            if not loc: return None
            return kind, _entry(loc, fid, "video/mp4", q, q, 30, "", 0, "mp4", codec)
        except Exception:
            return None

    async with httpx.AsyncClient(timeout=httpx.Timeout(18.0), follow_redirects=False) as cl:
        res = await asyncio.gather(*[one(cl, f) for f in ZERNIO_FORMATS])
    fs = [e for r in res if r and r[0] == "combined" for e in [r[1]]]
    af = [e for r in res if r and r[0] == "video-only" for e in [r[1]]]
    n = {"formatStreams": fs, "adaptiveFormats": af, "_source": "zernio"}
    return n if _has_streams(n) else None


# ─────────────────────────────────────────────
#  共通形式 → テンプレ用 streamUrls / videoUrls
# ─────────────────────────────────────────────
def _pick_audio(af: List[dict]) -> str:
    audios = [f for f in af if "audio" in (f.get("type") or "") and f.get("url")]
    if not audios: return ""
    for f in audios:
        if (f.get("language") or "") .startswith("ja"):
            return f["url"]

    def score(f):
        try: br = int(f.get("bitrate") or 0)
        except Exception: br = 0
        is_m4a = "mp4" in (f.get("type") or "") or f.get("container") == "m4a"
        return (1 if is_m4a else 0, br)
    return max(audios, key=score)["url"]


def build_stream_lists(norm: dict) -> Dict[str, List]:
    fs, af = norm.get("formatStreams", []), norm.get("adaptiveFormats", [])
    audio = _pick_audio(af)
    su = [{"url": f["url"], "resolution": f.get("qualityLabel") or f.get("quality"),
           "format": "hls" if f.get("isHls") else "mp4/mixed", "audioUrl": ""}
          for f in fs if f.get("url")]
    vids = [f for f in af if "video" in (f.get("type") or "") and f.get("url")]
    vids.sort(key=lambda f: QORDER.get(f.get("qualityLabel") or f.get("quality") or "", 99))
    if audio:
        for f in vids:
            web = f.get("container") == "webm" or "webm" in (f.get("type") or "")
            su.append({"url": f["url"], "resolution": f.get("qualityLabel") or f.get("quality"),
                       "format": f"{'webm' if web else 'mp4'}/videoOnly", "audioUrl": audio})
    vu = [f["url"] for f in fs if f.get("url")] or [f["url"] for f in vids]
    return {"streamUrls": su, "videoUrls": vu}


def extract_invidious_streams(v_data: dict) -> Dict[str, List]:
    if not v_data: return {"streamUrls": [], "videoUrls": []}
    return build_stream_lists(_inv_streams(v_data))


async def fetch_fastest_stream_urls(v: str, api: Optional[str] = None,
                                    force_instance: Optional[str] = None) -> dict:
    ckey = f"streams:{v}:{api or 'auto'}:{force_instance or ''}"
    hit = _cget(ckey)
    if hit is not None:
        return hit

    src = {
        "invidious": lambda: _timed("invidious", _s_invidious(v, force_instance)),
        "piped": lambda: _timed("piped", _s_piped(v)),
        "sia": lambda: _timed("sia_stream", _s_sia(v)),
        "rapidapi": lambda: _timed("rapidapi", _s_rapid(v)),
        "zernio": lambda: _timed("zernio", _s_zernio(v)),
    }
    if api in src:
        norm = await src[api]()
    else:
        norm = await _race([f() for f in src.values()], 15.0, _has_streams)
    if not norm:
        raise RuntimeError("all stream sources failed")

    result = {**build_stream_lists(norm), "stream_api_used": norm.get("_source", "unknown")}
    _cset(ckey, result, TTL_STREAM)
    return result


# ─────────────────────────────────────────────
#  コメント
# ─────────────────────────────────────────────
def _norm_comment(c: dict) -> Optional[dict]:
    if not isinstance(c, dict): return None
    it = dict(c)
    ao = it.get("author")
    icon = ""
    if isinstance(ao, dict):
        it["author"] = ao.get("name", "")
        icon = ao.get("avatar") or ao.get("authorIcon") or it.get("avatar", "")
        it["authorId"] = ao.get("channelId", "")
        it["isCreator"] = ao.get("creator", False)
        it["isVerified"] = ao.get("verified", False)
    else:
        th = it.get("authorThumbnails") or []
        if isinstance(th, list) and th:
            icon = th[-1].get("url", "")
    icon = icon or it.get("authorIcon") or it.get("avatar", "")
    it["authorIcon"] = it["authorThumbnail"] = it["avatar"] = icon
    if not isinstance(it.get("authorThumbnails"), list):
        it["authorThumbnails"] = [{"url": icon}] if icon else []
    text = it.get("text") or it.get("content") or ""
    it.setdefault("content", text)
    it.setdefault("contentHtml", text.replace("\n", "<br>"))
    pub = it.get("publishedTime") or it.get("published") or it.get("publishedText", "")
    it["publishedTime"] = it["publishedText"] = pub
    lk = it.get("likes")
    if isinstance(lk, dict): it["likeCount"] = lk.get("count", 0)
    rp = it.get("replies")
    if isinstance(rp, dict): it["replyCount"] = rp.get("count", 0)
    return it


def process_comments(data: Any) -> List[dict]:
    if isinstance(data, Exception) or not data: return []
    items = data.get("comments", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    return [n for n in (_norm_comment(c) for c in items) if n]


async def fetch_comments(v: str, force_instance: Optional[str] = None,
                         api: Optional[str] = None) -> dict:
    ckey = f"comments:{v}:{api or 'auto'}"
    hit = _cget(ckey)
    if hit is not None:
        return hit

    async def inv():
        res = await asyncio.wait_for(proxy_parallel("comments", f"/api/v1/comments/{v}"), timeout=12.0)
        d = res.get("data")
        return d if isinstance(d, dict) and d.get("comments") else None

    async def sennin():
        d = await _get_json(f"{SENNIN_BASE}/api/comments/{v}", 6.0)
        return d if isinstance(d, dict) and d.get("comments") else None

    if api == "sennin": res = await _timed("sennin", sennin())
    elif api == "invidious": res = await _timed("invidious", inv())
    else: res = await _race([inv(), sennin()], 12.0, lambda r: isinstance(r, dict))
    if res:
        _cset(ckey, res, TTL_COMMENTS)
    return res or {}


# ─────────────────────────────────────────────
#  ルーター
# ─────────────────────────────────────────────
async def _error_response(request: Request, exc: Exception, where: str, v: str):
    if isinstance(exc, httpx.TimeoutException):
        logger.error(f"Timeout in {where} for {v}")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    logger.error(f"Error in {where} for {v}: {exc}")
    try:
        inst = await get_video_back_instances()
    except Exception:
        inst = []
    return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": inst})


@router.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts_player(request: Request, v: str,
                        force_instance: Optional[str] = Query(None),
                        info_api: Optional[str] = Query(None),
                        stream_api: Optional[str] = Query(None),
                        api: Optional[str] = Query(None)):
    r_info, r_stream = info_api or api, stream_api or api
    try:
        vd, sd, cd = await asyncio.gather(
            fetch_video_info(v, force_instance, r_info),
            fetch_fastest_stream_urls(v, r_stream, force_instance),
            fetch_comments(v, force_instance, r_info),
            return_exceptions=True,
        )
        if isinstance(vd, Exception) and isinstance(sd, Exception):
            raise vd
        v_data = vd if isinstance(vd, dict) else {}
        s_data = sd if isinstance(sd, dict) else {}
        video_urls = s_data.get("videoUrls") or extract_invidious_streams(v_data).get("videoUrls", [])
        used = v_data.get("api_used", "unknown")
        return templates.TemplateResponse("short.html", {
            "request": request, "videoid": v,
            "video_title": v_data.get("title", ""), "videourls": video_urls,
            "author": v_data.get("author", ""),
            "view_count": v_data.get("viewCount", 0), "like_count": v_data.get("likeCount", 0),
            "description": v_data.get("descriptionHtml") or (v_data.get("description") or "").replace("\n", "<br>"),
            "comments": process_comments(cd),
            "info_api_used": used, "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": used,
        })
    except Exception as e:
        return await _error_response(request, e, "shorts_player", v)


@router.get("/watch", response_class=HTMLResponse)
async def watch(request: Request, v: str = Query(...),
                list: Optional[str] = Query(None),
                force_instance: Optional[str] = Query(None),
                info_api: Optional[str] = Query(None),
                stream_api: Optional[str] = Query(None),
                api: Optional[str] = Query(None),
                nocache: bool = Query(False)):
    r_info, r_stream = info_api or api, stream_api or api
    try:
        async def playlist():
            if not list: return None
            try:
                res = await asyncio.wait_for(proxy_parallel("playlist", f"/api/v1/playlists/{list}"), 8.0)
                d = res.get("data")
                return d if isinstance(d, dict) and not d.get("error") else None
            except Exception as e:
                logger.debug(f"playlist error: {e}")
                return None

        vd, sd, cd, pd = await asyncio.gather(
            fetch_video_info(v, force_instance, r_info, nocache),
            fetch_fastest_stream_urls(v, r_stream, force_instance),
            fetch_comments(v, force_instance, r_info),
            playlist(),
            return_exceptions=True,
        )
        if isinstance(vd, Exception) and isinstance(sd, Exception):
            raise vd
        v_data = vd if isinstance(vd, dict) else {}
        s_data = sd if isinstance(sd, dict) else {}
        p_data = pd if isinstance(pd, dict) else {}

        stream_urls, video_urls = s_data.get("streamUrls", []), s_data.get("videoUrls", [])
        if not stream_urls and v_data:
            inv = extract_invidious_streams(v_data)
            stream_urls, video_urls = inv["streamUrls"], inv["videoUrls"]

        recommended = []
        for r in v_data.get("recommendedVideos", []):
            if not isinstance(r, dict): continue
            th = r.get("thumbnail") or ((r.get("videoThumbnails") or [{}])[0].get("url", ""))
            recommended.append({
                "video_id": r.get("video_id") or r.get("videoId"),
                "title": r.get("title"), "author": r.get("author"),
                "view_count_text": r.get("view_count_text") or r.get("viewCountText")
                                   or (f"{r['viewCount']:,} 回視聴" if r.get("viewCount") else ""),
                "thumbnail": th,
            })

        icon = v_data.get("authorIcon") or ""
        if not icon:
            th = v_data.get("authorThumbnails") or []
            icon = th[-1]["url"] if th else ""

        sub = v_data.get("subCountText") or "非公開"
        used = v_data.get("api_used", "unknown")
        resp = templates.TemplateResponse("watch.html", {
            "request": request, "videoid": v,
            "video_title": v_data.get("title", ""),
            "videourls": video_urls, "streamUrls": stream_urls,
            "author": v_data.get("author", ""), "author_id": v_data.get("authorId", ""),
            "author_icon": icon, "subscribers_count": sub,
            "view_count": v_data.get("viewCount", 0), "like_count": v_data.get("likeCount", 0),
            "description": v_data.get("descriptionHtml") or (v_data.get("description") or "").replace("\n", "<br>"),
            "recommended_videos": recommended, "comments": process_comments(cd),
            "youtube_url": f"https://www.youtube.com/watch?v={v}",
            "info_api_used": used, "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": used, "playlist_id": list, "playlist_title": p_data.get("title", ""),
            "playlist_videos": [{"videoId": i.get("videoId"), "title": i.get("title"), "author": i.get("author")}
                                for i in p_data.get("videos", []) if isinstance(i, dict)],
        })

        try:
            h = json.loads(request.cookies.get("history", "[]"))
            h = [x for x in h if x.get("videoId") != v]
            h.append({"videoId": v, "title": v_data.get("title", ""),
                      "author": v_data.get("author", ""), "added_at": datetime.now().isoformat()})
            resp.set_cookie("history", json.dumps(h[-50:]), max_age=2592000, httponly=True, samesite="Lax")
        except Exception as e:
            logger.debug(f"history error: {e}")
        return resp
    except Exception as e:
        return await _error_response(request, e, "watch", v)


@router.get("/api/watch-info/{video_id}")
async def api_watch_info(video_id: str, api: Optional[str] = None, nocache: bool = False):
    d = await fetch_video_info(video_id, api=api, nocache=nocache)
    return JSONResponse(d) if d else JSONResponse({"error": "動画情報の取得に失敗しました"}, status_code=502)


@router.get("/api/watch-streams/{video_id}")
async def api_watch_streams(video_id: str, api: Optional[str] = None):
    try:
        return JSONResponse(await fetch_fastest_stream_urls(video_id, api=api))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@router.get("/api/stats")
async def get_api_stats():
    async with _STATS_LOCK:
        return {"stats": dict(_STATS), "timestamp": datetime.now().isoformat()}
