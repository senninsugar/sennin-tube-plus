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
from fastapi.templating import Jinja2Templates

from app.search import (
    fetch_invidious,
    client_session,
    get_invidious_instances_from_url,
    INVIDIOUS_VIDEO_LIST_URL,
)

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")

# ─────────────────────────────────────────────
#  設定（キー・ホストは環境変数から読む）
# ─────────────────────────────────────────────
#  RAPIDAPI_KEYS   : カンマ区切りの RapidAPI キー
#  RAPIDAPI_HOST   : RapidAPI のホスト名 (/dl を持つもの)
#  ZERNIO_BASE     : 例 "https://.../?id="  (video_id を末尾に連結、&formatId=N を付与)
#  SIA_STREAM_BASE : 例 "https://.../"      (video_id を末尾に連結、JSONを返す)
#  SENNIN_BASE     : Sennin API のベースURL
# ─────────────────────────────────────────────
RAPIDAPI_KEYS: List[str] = [k.strip() for k in os.environ.get("RAPIDAPI_KEYS", "").split(",") if k.strip()]
RAPIDAPI_HOST: str = os.environ.get("RAPIDAPI_HOST", "")
ZERNIO_BASE: str = os.environ.get("ZERNIO_BASE", "")
SIA_STREAM_BASE: str = os.environ.get("SIA_STREAM_BASE", "")
SENNIN_BASE: str = os.environ.get(
    "SENNIN_BASE", "https://discerning-adventure-production-ebfc.up.railway.app"
).rstrip("/")
SIA_INFO_BASE: str = os.environ.get("SIA_INFO_BASE", "https://siatube.com").rstrip("/")

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

CACHE_CONFIG = {
    "video_info": 300.0,
    "comments": 600.0,
    "streams": 60.0,      # 署名付きURLは短命なので短め
    "zernio": 60.0,
}

ZERNIO_FORMATS = {
    2: {"quality": "360p", "type": "combined", "codec": "H.264"},
    4: {"quality": "720p", "type": "video-only", "codec": "H.264"},
    5: {"quality": "1080p", "type": "video-only", "codec": "H.264"},
}
ZERNIO_DEFAULT = 2

QUALITY_ORDER = {
    "2160p": 0, "1440p": 1, "1080p": 2, "720p": 3,
    "480p": 4, "360p": 5, "240p": 6, "144p": 7,
    "hd2160": 0, "hd1440": 1, "hd1080": 2, "hd720": 3,
    "large": 4, "medium": 5, "small": 6, "tiny": 7,
}

_API_STATS: Dict[str, Dict[str, Any]] = {
    n: {"success": 0, "failure": 0, "avg_time": 0.0}
    for n in ("invidious", "sia", "sennin", "piped",
              "rapidapi", "zernio", "sia_stream")
}
_STATS_LOCK = asyncio.Lock()

# ─────────────────────────────────────────────
#  キャッシュ / 同時リクエスト集約
# ─────────────────────────────────────────────
_CACHE: Dict[str, Tuple[Any, float]] = {}
_CACHE_MAX = 400
_INFLIGHT: Dict[str, "asyncio.Future"] = {}
_rr_index = 0


def _now() -> float:
    return time.time()


def _cache_get(key: str) -> Optional[Any]:
    e = _CACHE.get(key)
    if not e:
        return None
    if e[1] < _now():
        _CACHE.pop(key, None)
        return None
    return e[0]


def _cache_set(key: str, value: Any, ttl: float) -> None:
    if len(_CACHE) >= _CACHE_MAX:
        oldest = min(_CACHE, key=lambda k: _CACHE[k][1])
        _CACHE.pop(oldest, None)
    _CACHE[key] = (value, _now() + ttl)


async def _dedupe(key: str, factory):
    """同じキーの同時リクエストを1本にまとめる。"""
    fut = _INFLIGHT.get(key)
    if fut is not None:
        try:
            return await asyncio.shield(fut)
        except Exception:
            pass
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _INFLIGHT[key] = fut
    try:
        res = await factory()
        if not fut.done():
            fut.set_result(res)
        return res
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
            fut.exception()  # 未取得警告の抑止
        raise
    finally:
        _INFLIGHT.pop(key, None)


async def record_api_performance(api: str, success: bool, duration: float) -> None:
    async with _STATS_LOCK:
        st = _API_STATS.get(api)
        if st is None:
            return
        if success:
            st["success"] += 1
            n = st["success"]
            st["avg_time"] = st["avg_time"] * (n - 1) / n + duration / n
        else:
            st["failure"] += 1


async def _race(tasks: List["asyncio.Task"], timeout: float, valid) -> Optional[Any]:
    """最初に valid を満たした結果を返し、残りはキャンセル。"""
    pending = set(tasks)
    result = None
    deadline = _now() + timeout
    try:
        while pending and result is None:
            remain = deadline - _now()
            if remain <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remain, return_when=asyncio.FIRST_COMPLETED
            )
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


# ─────────────────────────────────────────────
#  共通ヘルパー
# ─────────────────────────────────────────────
def _parse_mime(mime: str) -> Tuple[str, str]:
    container = "webm" if "webm" in mime else (
        "m4a" if ("audio/mp4" in mime or "m4a" in mime) else "mp4"
    )
    cs = ""
    for q in ('codecs="', "codecs='"):
        if q in mime:
            cs = mime.split(q)[1].rstrip(q[-1]).split(",")[0].strip().lower()
            break
    return container, _codec_name(cs)


def _codec_name(vc: str) -> str:
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


def _format_sub_count(count: int) -> str:
    if not count:
        return ""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{round(count / 1_000)}K"
    return str(count)


def _relative_date(date_str: str) -> str:
    try:
        days = (date.today() - date.fromisoformat(date_str[:10])).days
        if days < 0:
            return date_str
        if days == 0:
            return "今日"
        if days < 30:
            return f"{days} 日前"
        if days < 365:
            return f"{days // 30} ヶ月前"
        return f"{days // 365} 年前"
    except Exception:
        return date_str


def _fmt_entry(url, itag, mime, quality, label, fps, size, bitrate, container, enc, **extra):
    d = {
        "url": url, "itag": str(itag), "type": mime, "quality": quality,
        "qualityLabel": label, "fps": fps, "size": size,
        "bitrate": str(bitrate), "container": container, "encoding": enc,
    }
    d.update(extra)
    return d


# ─────────────────────────────────────────────
#  動画情報: Piped → Invidious互換
# ─────────────────────────────────────────────
def _piped_to_invidious(piped: dict) -> dict:
    uploader_url = piped.get("uploaderUrl", "") or ""
    author_id = ""
    if "/channel/" in uploader_url:
        author_id = uploader_url.split("/channel/")[-1].strip("/")
    elif uploader_url.startswith("/@"):
        author_id = uploader_url[1:]
    elif uploader_url.startswith("/c/"):
        author_id = uploader_url[3:]

    upload_date = piped.get("uploadDate", "")
    avatar = piped.get("uploaderAvatar", "")
    thumb_url = piped.get("thumbnailUrl", "")

    related = []
    for s in piped.get("relatedStreams") or []:
        if s.get("type") != "stream":
            continue
        vurl = s.get("url", "")
        vid = vurl.split("?v=")[-1].split("&")[0] if "?v=" in vurl else ""
        if not vid:
            continue
        up = s.get("uploaderUrl", "") or ""
        aid = up.split("/channel/")[-1].strip("/") if "/channel/" in up else ""
        th = s.get("thumbnail", "")
        related.append({
            "videoId": vid,
            "video_id": vid,
            "title": s.get("title", ""),
            "author": s.get("uploaderName", ""),
            "authorId": aid,
            "lengthSeconds": s.get("duration", 0) or 0,
            "viewCount": s.get("views", 0) or 0,
            "view_count_text": s.get("uploadedDate") or "",
            "publishedText": s.get("uploadedDate") or "",
            "thumbnail": th,
            "videoThumbnails": [{"quality": "hq", "url": th}] if th else [],
        })

    sub = piped.get("uploaderSubscriberCount") or 0
    return {
        "title": piped.get("title", ""),
        "author": piped.get("uploader", "") or "",
        "authorId": author_id,
        "authorIcon": avatar,
        "viewCount": piped.get("views", 0) or 0,
        "likeCount": piped.get("likes", 0) or 0,
        "publishedText": _relative_date(upload_date) if upload_date else "",
        "description": "",
        "descriptionHtml": piped.get("description", ""),
        "lengthSeconds": piped.get("duration", 0) or 0,
        "subCount": sub,
        "subCountText": _format_sub_count(sub) or "非公開",
        "authorVerified": piped.get("uploaderVerified", False),
        "authorThumbnails": [{"url": avatar, "width": 48, "height": 48}] if avatar else [],
        "videoThumbnails": [{"quality": "maxresdefault", "url": thumb_url,
                             "width": 1280, "height": 720}] if thumb_url else [],
        "thumbnail": thumb_url,
        "recommendedVideos": related,
        "api_used": "piped",
    }


async def _piped_raw(video_id: str) -> Optional[dict]:
    """Piped /streams/{id} の生JSON（情報・ストリーム共用、キャッシュ付き）。"""
    key = f"piped_raw:{video_id}"
    hit = _cache_get(key)
    if hit is not None:
        return hit

    async def _one(instance: str) -> Optional[dict]:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(12.0), follow_redirects=True) as cl:
                r = await cl.get(f"{instance}/streams/{video_id}")
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and not data.get("error") and data.get("title"):
                    return data
        except Exception:
            pass
        return None

    tasks = [asyncio.create_task(_one(i)) for i in PIPED_INSTANCES]
    data = await _race(tasks, 12.0, lambda d: isinstance(d, dict))
    if data:
        _cache_set(key, data, CACHE_CONFIG["video_info"])
    return data


async def fetch_piped_video_info(v: str) -> Optional[Dict[str, Any]]:
    t0 = _now()
    raw = await _piped_raw(v)
    ok = raw is not None
    await record_api_performance("piped", ok, _now() - t0)
    return _piped_to_invidious(raw) if raw else None


# ─────────────────────────────────────────────
#  動画情報: Sennin / Sia / Invidious
# ─────────────────────────────────────────────
def _process_related_videos(raw_rel: List[Any]) -> List[Dict[str, Any]]:
    out = []
    for item in raw_rel:
        if not isinstance(item, dict):
            continue
        thumb = item.get("thumbnail", "")
        if not thumb and isinstance(item.get("thumbnails"), list) and item["thumbnails"]:
            thumb = item["thumbnails"][0].get("url", "")
        out.append({
            "video_id": item.get("videoId") or item.get("id"),
            "title": item.get("title"),
            "author": item.get("channelName") or item.get("author"),
            "view_count_text": item.get("viewCountText"),
            "thumbnail": thumb,
        })
    return out


def normalize_sennin_video_info(d: Dict[str, Any]) -> Dict[str, Any]:
    if not d or not isinstance(d, dict):
        return {}
    ai = d.get("author", {}) if isinstance(d.get("author"), dict) else {}
    desc_obj = d.get("description", {})
    if isinstance(desc_obj, dict):
        desc_text = desc_obj.get("text", "")
        desc_html = desc_obj.get("formatted") or desc_text.replace("\n", "<br>")
    else:
        desc_text = str(desc_obj or "")
        desc_html = desc_text.replace("\n", "<br>")
    rel = d.get("Related-videos", {})
    raw_rel = rel.get("relatedVideos", []) if isinstance(rel, dict) else []
    return {
        "title": d.get("title", ""),
        "author": ai.get("name") or "",
        "authorId": ai.get("id") or "",
        "authorIcon": ai.get("thumbnail") or "",
        "subCountText": ai.get("subscribers") or "非公開",
        "viewCount": d.get("views") or d.get("extended_stats", {}).get("views_original", 0),
        "likeCount": d.get("likes", 0),
        "description": desc_text,
        "descriptionHtml": desc_html,
        "recommendedVideos": _process_related_videos(raw_rel),
        "thumbnail": d.get("thumbnail", ""),
    }


async def fetch_sennin_video_info(v: str) -> Optional[Dict[str, Any]]:
    key = f"sennin_video:{v}"
    hit = _cache_get(key)
    if hit is not None:
        return hit
    t0 = _now()
    try:
        resp = await asyncio.wait_for(
            client_session.get(f"{SENNIN_BASE}/api/video/{v}",
                               timeout=httpx.Timeout(3.0, connect=1.0, read=2.0)),
            timeout=3.5,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data and not data.get("unavailable"):
                norm = normalize_sennin_video_info(data)
                if norm.get("title"):
                    norm["api_used"] = "sennin"
                    _cache_set(key, norm, CACHE_CONFIG["video_info"])
                    await record_api_performance("sennin", True, _now() - t0)
                    return norm
    except Exception as e:
        logger.debug(f"Sennin error for {v}: {e}")
    await record_api_performance("sennin", False, _now() - t0)
    return None


async def fetch_sia_video(v: str) -> Optional[Dict[str, Any]]:
    key = f"sia_video:{v}"
    hit = _cache_get(key)
    if hit is not None:
        return hit
    t0 = _now()
    try:
        resp = await asyncio.wait_for(
            client_session.get(f"{SIA_INFO_BASE}/api/video/{v}",
                               timeout=httpx.Timeout(2.5, connect=1.0, read=1.5)),
            timeout=3.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            ai = data.get("author", {}) if isinstance(data.get("author"), dict) else {}
            name = ai.get("name") or data.get("uploader") or ""
            if name:
                desc_obj = data.get("description", {})
                desc_text = desc_obj.get("text", "") if isinstance(desc_obj, dict) else str(desc_obj or "")
                rel = data.get("Related-videos") or data.get("relatedVideos") or {}
                raw_rel = rel.get("relatedVideos", []) if isinstance(rel, dict) else (rel if isinstance(rel, list) else [])
                result = {
                    "title": data.get("title", ""),
                    "author": name,
                    "authorId": ai.get("id", ""),
                    "authorIcon": ai.get("thumbnail", ""),
                    "subCountText": ai.get("subscribers", "非公開"),
                    "viewCount": data.get("views", 0),
                    "likeCount": data.get("likes", 0),
                    "description": desc_text,
                    "descriptionHtml": desc_text.replace("\n", "<br>"),
                    "recommendedVideos": _process_related_videos(raw_rel),
                    "thumbnail": data.get("thumbnail", ""),
                    "api_used": "sia",
                }
                _cache_set(key, result, CACHE_CONFIG["video_info"])
                await record_api_performance("sia", True, _now() - t0)
                return result
    except Exception as e:
        logger.debug(f"Sia error for {v}: {e}")
    await record_api_performance("sia", False, _now() - t0)
    return None


async def fetch_video_info_invidious_robust(
    v: str, force_instance: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    t0 = _now()
    try:
        res = await asyncio.wait_for(
            fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video"),
            timeout=4.0,
        )
        if isinstance(res, dict) and not res.get("error") and (res.get("title") or res.get("videoId")):
            res["api_used"] = "invidious"
            await record_api_performance("invidious", True, _now() - t0)
            return res
    except Exception as e:
        logger.debug(f"Invidious primary error for {v}: {e}")

    instances = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)

    async def _try(inst: str):
        try:
            resp = await asyncio.wait_for(
                client_session.get(f"{inst.rstrip('/')}/api/v1/videos/{v}",
                                   timeout=httpx.Timeout(3.0, connect=1.0, read=2.0)),
                timeout=3.5,
            )
            if resp.status_code == 200:
                d = resp.json()
                if isinstance(d, dict) and not d.get("error") and (d.get("title") or d.get("videoId")):
                    d["api_used"] = "invidious"
                    return d
        except Exception:
            pass
        return None

    if instances:
        tasks = [asyncio.create_task(_try(i)) for i in instances[:5]]
        r = await _race(tasks, 4.0, lambda d: isinstance(d, dict))
        if r:
            await record_api_performance("invidious", True, _now() - t0)
            return r
    await record_api_performance("invidious", False, _now() - t0)
    return None


async def fetch_video_info(
    v: str, force_instance: Optional[str] = None, api: Optional[str] = None,
    nocache: bool = False,
) -> Optional[Dict[str, Any]]:
    """api 指定ならそのAPIのみ。未指定なら全API並行で最速採用。"""
    ckey = f"video_info:{v}:{force_instance or ''}:{api or 'auto'}"
    if not nocache:
        hit = _cache_get(ckey)
        if hit is not None:
            return hit

    async def _do():
        if api == "invidious":
            return await fetch_video_info_invidious_robust(v, force_instance)
        if api == "sia":
            return await fetch_sia_video(v)
        if api == "sennin":
            return await fetch_sennin_video_info(v)
        if api == "piped":
            return await fetch_piped_video_info(v)
        tasks = [
            asyncio.create_task(fetch_video_info_invidious_robust(v, force_instance)),
            asyncio.create_task(fetch_piped_video_info(v)),
            asyncio.create_task(fetch_sia_video(v)),
            asyncio.create_task(fetch_sennin_video_info(v)),
        ]
        return await _race(
            tasks, 6.0,
            lambda r: isinstance(r, dict) and (r.get("title") or r.get("videoId")),
        )

    result = await (_do() if nocache else _dedupe(f"info:{ckey}", _do))
    if result and not result.get("error"):
        _cache_set(ckey, result, CACHE_CONFIG["video_info"])
    return result


# ─────────────────────────────────────────────
#  ストリーム: 各ソース → 共通フォーマット
#    {"formatStreams": [...combined...], "adaptiveFormats": [...], "_source": ...}
# ─────────────────────────────────────────────
def _norm_rapidapi(raw: dict) -> dict:
    fs, af = [], []
    for f in raw.get("formats", []):
        mime = f.get("mimeType", "")
        c, e = _parse_mime(mime)
        w, h = f.get("width", 0), f.get("height", 0)
        fs.append(_fmt_entry(f.get("url", ""), f.get("itag", ""), mime, f.get("quality", ""),
                             f.get("qualityLabel", f.get("quality", "")), f.get("fps", 30),
                             f"{w}x{h}" if w and h else "", f.get("bitrate", 0), c, e))
    for f in raw.get("adaptiveFormats", []):
        mime = f.get("mimeType", "")
        c, e = _parse_mime(mime)
        w, h = f.get("width", 0), f.get("height", 0)
        af.append(_fmt_entry(f.get("url", ""), f.get("itag", ""), mime, f.get("quality", ""),
                             f.get("qualityLabel", ""), f.get("fps", 0),
                             f"{w}x{h}" if w and h else "", f.get("bitrate", 0), c, e))
    fs.sort(key=lambda f: QUALITY_ORDER.get(f.get("quality", ""), 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "rapidapi"}


def _norm_sia_stream(raw: dict) -> dict:
    fs, af = [], []
    streams = raw.get("streams", {})
    for s in streams.get("muxed", []):
        url = s.get("streamUrl", "")
        if not url:
            continue
        note, ext = s.get("formatNote", ""), s.get("ext", "mp4")
        w, h = s.get("width", 0), s.get("height", 0)
        fs.append(_fmt_entry(url, s.get("formatId", ""), f"video/{ext}", note, note,
                             s.get("fps", 25), f"{w}x{h}" if w and h else "",
                             int(s.get("tbr", 0) or 0), ext, _codec_name(s.get("vcodec", ""))))
    for s in streams.get("videoOnly", []):
        url = s.get("streamUrl", "")
        if not url:
            continue
        note, ext = s.get("formatNote", ""), s.get("ext", "mp4")
        w, h = s.get("width", 0), s.get("height", 0)
        af.append(_fmt_entry(url, s.get("formatId", ""), f"video/{ext}", note, note,
                             s.get("fps", 30), f"{w}x{h}" if w and h else "",
                             int(s.get("tbr", 0) or 0), ext, _codec_name(s.get("vcodec", ""))))
    for s in streams.get("audioOnly", []):
        url = s.get("streamUrl", "")
        if not url:
            continue
        ext = s.get("ext", "webm")
        af.append(_fmt_entry(url, s.get("formatId", ""), f"audio/{ext}", s.get("formatNote", ""), "",
                             0, "", int(s.get("tbr", 0) or 0), ext, _codec_name(s.get("acodec", ""))))
    # ライブ動画 HLS
    m3u8 = raw.get("m3u8", {}).get("list", [])
    if m3u8 and not fs:
        seen = set()
        for s in m3u8:
            url, h, w = s.get("streamUrl", ""), s.get("height") or 0, s.get("width") or 0
            fid = s.get("formatId")
            if not url or not h or not fid or (fid, h) in seen:
                continue
            seen.add((fid, h))
            q = f"{h}p"
            fs.append(_fmt_entry(url, fid, f"video/{s.get('ext') or 'mp4'}", q, q, s.get("fps") or 30,
                                 f"{w}x{h}" if w else "", int(s.get("tbr", 0) or 0),
                                 s.get("ext") or "mp4", _codec_name(s.get("vcodec", "")), isHls=True))
    fs.sort(key=lambda f: QUALITY_ORDER.get(f.get("quality", ""), 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "sia_stream"}


def _norm_piped_stream(data: dict) -> dict:
    fs, af = [], []
    for s in data.get("videoStreams", []) or []:
        url = s.get("url")
        if not url:
            continue
        fmt = (s.get("format") or "").upper()
        q = str(s.get("quality", ""))
        w, h = s.get("width", 0), s.get("height", 0)
        ext = "mp4" if "MP4" in fmt or "MPEG" in fmt else "webm"
        entry = _fmt_entry(url, s.get("itag", ""), f"video/{ext}", q, q, s.get("fps", 30),
                           f"{w}x{h}" if w and h else "", s.get("bitrate", 0), ext,
                           _codec_name(s.get("codec", "")))
        if s.get("videoOnly", True):
            af.append(entry)
        elif fmt != "HLS":
            fs.append(entry)
    for s in data.get("audioStreams", []) or []:
        url = s.get("url")
        if not url:
            continue
        fmt = (s.get("format") or "").upper()
        ext = "m4a" if "M4A" in fmt or "MP4" in fmt else "webm"
        af.append(_fmt_entry(url, s.get("itag", ""), f"audio/{ext}", s.get("quality", ""), "", 0, "",
                             s.get("bitrate", 0), ext, _codec_name(s.get("codec", "")),
                             language=(s.get("audioTrackLocale") or "")[:2]))
    hls = data.get("hls")
    if hls and not fs:
        fs.append(_fmt_entry(hls, "hls", "application/x-mpegURL", "auto", "auto", 30, "", 0,
                             "m3u8", "", isHls=True))
    fs.sort(key=lambda f: QUALITY_ORDER.get(f.get("quality", ""), 99))
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "piped"}


def _norm_invidious_stream(v_data: dict) -> dict:
    fs = [dict(f, itag=str(f.get("itag", ""))) for f in v_data.get("formatStreams", [])]
    af = [dict(f, itag=str(f.get("itag", ""))) for f in v_data.get("adaptiveFormats", [])]
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "invidious"}


async def _stream_rapidapi(v: str) -> Optional[dict]:
    global _rr_index
    if not RAPIDAPI_KEYS or not RAPIDAPI_HOST:
        return None
    t0 = _now()
    tried = set()
    for _ in range(len(RAPIDAPI_KEYS)):
        k = RAPIDAPI_KEYS[_rr_index % len(RAPIDAPI_KEYS)]
        _rr_index = (_rr_index + 1) % len(RAPIDAPI_KEYS)
        if k in tried:
            break
        tried.add(k)
        try:
            rp = await client_session.get(
                f"https://{RAPIDAPI_HOST}/dl", params={"id": v},
                headers={"X-RapidAPI-Key": k, "X-RapidAPI-Host": RAPIDAPI_HOST},
                timeout=httpx.Timeout(18.0),
            )
            if rp.status_code == 429 or rp.status_code >= 500:
                continue
            rp.raise_for_status()
            raw = rp.json()
            if raw.get("status") not in ("OK", None) and "formats" not in raw and "adaptiveFormats" not in raw:
                continue
            res = _norm_rapidapi(raw)
            if res["formatStreams"] or res["adaptiveFormats"]:
                await record_api_performance("rapidapi", True, _now() - t0)
                return res
        except Exception as e:
            logger.debug(f"RapidAPI error for {v}: {e}")
    await record_api_performance("rapidapi", False, _now() - t0)
    return None


async def _stream_zernio(v: str) -> Optional[dict]:
    if not ZERNIO_BASE:
        return None
    t0 = _now()

    async def _one(cl: httpx.AsyncClient, fid: int) -> Optional[dict]:
        ck = f"zernio:{v}:{fid}"
        hit = _cache_get(ck)
        if hit:
            return hit
        try:
            rp = await cl.get(f"{ZERNIO_BASE}{v}&formatId={fid}", headers={"User-Agent": UA})
            loc = rp.headers.get("location", "")
            if not loc:
                return None
            meta = ZERNIO_FORMATS[fid]
            entry = _fmt_entry(loc, fid, "video/mp4", meta["quality"], meta["quality"], 30, "", 0,
                               "mp4", meta["codec"], _kind=meta["type"])
            _cache_set(ck, entry, CACHE_CONFIG["zernio"])
            return entry
        except Exception:
            return None

    async with httpx.AsyncClient(timeout=httpx.Timeout(18.0), follow_redirects=False) as cl:
        entries = await asyncio.gather(*[_one(cl, f) for f in ZERNIO_FORMATS])
    fs = [e for e in entries if e and e.pop("_kind", "") == "combined"]
    # _kind を pop した後なので再取得して video-only を分類
    af = []
    for e, fid in zip(entries, ZERNIO_FORMATS):
        if e and ZERNIO_FORMATS[fid]["type"] == "video-only":
            af.append(e)
    fs.sort(key=lambda f: QUALITY_ORDER.get(f.get("quality", ""), 99))
    ok = bool(fs or af)
    await record_api_performance("zernio", ok, _now() - t0)
    return {"formatStreams": fs, "adaptiveFormats": af, "_source": "zernio"} if ok else None


async def _stream_sia(v: str) -> Optional[dict]:
    if not SIA_STREAM_BASE:
        return None
    t0 = _now()
    try:
        rp = await client_session.get(SIA_STREAM_BASE + v, headers={"User-Agent": UA},
                                      timeout=httpx.Timeout(18.0))
        if rp.status_code == 200:
            res = _norm_sia_stream(rp.json())
            if res["formatStreams"] or res["adaptiveFormats"]:
                await record_api_performance("sia_stream", True, _now() - t0)
                return res
    except Exception as e:
        logger.debug(f"Sia stream error for {v}: {e}")
    await record_api_performance("sia_stream", False, _now() - t0)
    return None


async def _stream_piped(v: str) -> Optional[dict]:
    raw = await _piped_raw(v)
    if not raw:
        return None
    res = _norm_piped_stream(raw)
    return res if (res["formatStreams"] or res["adaptiveFormats"]) else None


async def _stream_invidious(v: str, force_instance: Optional[str]) -> Optional[dict]:
    info = await fetch_video_info_invidious_robust(v, force_instance)
    if not info:
        return None
    res = _norm_invidious_stream(info)
    return res if (res["formatStreams"] or res["adaptiveFormats"]) else None


# ─────────────────────────────────────────────
#  共通フォーマット → テンプレ用 streamUrls / videoUrls
# ─────────────────────────────────────────────
def _pick_audio(adaptive: List[dict]) -> str:
    audios = [f for f in adaptive if "audio" in (f.get("type") or "")]
    if not audios:
        return ""
    for f in audios:
        if (f.get("language") or "").startswith("ja"):
            return f.get("url", "")
    # 互換性重視で m4a(AAC) を優先し、その中で最高ビットレート
    def _score(f):
        try:
            br = int(f.get("bitrate") or 0)
        except Exception:
            br = 0
        return (1 if f.get("container") == "m4a" else 0, br)
    return max(audios, key=_score).get("url", "")


def build_stream_lists(norm: dict) -> Dict[str, List]:
    fs = norm.get("formatStreams", [])
    af = norm.get("adaptiveFormats", [])
    audio_url = _pick_audio(af)

    stream_urls: List[dict] = []
    for f in fs:
        if not f.get("url"):
            continue
        stream_urls.append({
            "url": f["url"],
            "resolution": f.get("qualityLabel") or f.get("quality"),
            "format": "hls" if f.get("isHls") else "mp4/mixed",
            "audioUrl": "",
        })
    if audio_url:
        videos = [f for f in af if "video" in (f.get("type") or "") and f.get("url")]
        videos.sort(key=lambda f: (QUALITY_ORDER.get(f.get("qualityLabel") or f.get("quality") or "", 99)))
        for f in videos:
            cont = f.get("container", "")
            stream_urls.append({
                "url": f["url"],
                "resolution": f.get("qualityLabel") or f.get("quality"),
                "format": f"{'webm' if cont == 'webm' else 'mp4'}/videoOnly",
                "audioUrl": audio_url,
            })

    video_urls = [f["url"] for f in fs if f.get("url")]
    if not video_urls:
        video_urls = [f["url"] for f in af if "video" in (f.get("type") or "") and f.get("url")]
    return {"streamUrls": stream_urls, "videoUrls": video_urls}


def extract_invidious_streams(v_data: Dict[str, Any]) -> Dict[str, List]:
    if not v_data:
        return {"streamUrls": [], "videoUrls": []}
    return build_stream_lists(_norm_invidious_stream(v_data))


_STREAM_SOURCES = ("rapidapi", "zernio", "sia", "piped", "invidious")


async def fetch_fastest_stream_urls(
    v: str, api: Optional[str] = None, force_instance: Optional[str] = None,
) -> Dict[str, Any]:
    """api 指定ならそのソースのみ、未指定なら全ソース並行で最速採用。"""
    ckey = f"streams:{v}:{api or 'auto'}:{force_instance or ''}"
    hit = _cache_get(ckey)
    if hit is not None:
        return hit

    def _launch(name: str):
        if name == "rapidapi":
            return _stream_rapidapi(v)
        if name == "zernio":
            return _stream_zernio(v)
        if name == "sia":
            return _stream_sia(v)
        if name == "piped":
            return _stream_piped(v)
        return _stream_invidious(v, force_instance)

    async def _do():
        if api in _STREAM_SOURCES:
            return await _launch(api)
        tasks = [asyncio.create_task(_launch(n)) for n in _STREAM_SOURCES]
        return await _race(
            tasks, 12.0,
            lambda r: isinstance(r, dict) and (r.get("formatStreams") or r.get("adaptiveFormats")),
        )

    norm = await _dedupe(f"stream:{ckey}", _do)
    if not norm:
        raise RuntimeError("all stream sources failed")

    lists = build_stream_lists(norm)
    result = {**lists, "stream_api_used": norm.get("_source", "unknown")}
    _cache_set(ckey, result, CACHE_CONFIG["streams"])
    return result


# ─────────────────────────────────────────────
#  コメント
# ─────────────────────────────────────────────
def normalize_sennin_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = data.get("comments", []) if isinstance(data, dict) else []
    out = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        ai = c.get("author", {}) if isinstance(c.get("author"), dict) else {}
        li = c.get("likes", {}) if isinstance(c.get("likes"), dict) else {}
        ri = c.get("replies", {}) if isinstance(c.get("replies"), dict) else {}
        name = ai.get("name") or (c.get("author") if isinstance(c.get("author"), str) else "")
        icon = ai.get("avatar") or c.get("authorIcon") or ""
        text = c.get("text") or c.get("content") or ""
        out.append({
            "commentId": c.get("commentId", ""),
            "author": name,
            "authorId": ai.get("channelId") or c.get("authorId") or "",
            "authorIcon": icon, "authorThumbnail": icon,
            "authorThumbnails": [{"url": icon}] if icon else [],
            "content": text, "contentHtml": text.replace("\n", "<br>"),
            "publishedTime": c.get("publishedTime", ""),
            "publishedText": c.get("publishedTime", ""),
            "likeCount": li.get("count") if li else c.get("likes", 0),
            "replyCount": ri.get("count") if ri else c.get("replies", 0),
            "isCreator": ai.get("creator", False),
            "isVerified": ai.get("verified", False),
        })
    return out


def _normalize_comment(comment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    item = dict(comment)
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
    item.setdefault("contentHtml", item.get("content_html") or text.replace("\n", "<br>"))
    item.setdefault("content", text)
    pub = item.get("publishedTime") or item.get("published") or item.get("publishedText", "")
    item["publishedTime"] = item["publishedText"] = pub
    lk = item.get("likes")
    if isinstance(lk, dict):
        item["likeCount"] = lk.get("count", 0)
    return item


def process_comments(comment_data: Any) -> List[Dict[str, Any]]:
    if isinstance(comment_data, Exception) or not comment_data:
        return []
    if (isinstance(comment_data, dict) and comment_data.get("success") is True
            and isinstance(comment_data.get("comments"), list)):
        return normalize_sennin_comments(comment_data)
    comments = (comment_data.get("comments", []) if isinstance(comment_data, dict)
                else (comment_data if isinstance(comment_data, list) else []))
    out = []
    for c in comments:
        if isinstance(c, dict):
            n = _normalize_comment(c)
            if n:
                out.append(n)
    return out


async def fetch_comments(
    v: str, force_instance: Optional[str] = None, api: Optional[str] = None,
) -> Any:
    ckey = f"comments:{v}:{api or 'auto'}:{force_instance or ''}"
    hit = _cache_get(ckey)
    if hit is not None:
        return hit

    async def _inv():
        try:
            res = await asyncio.wait_for(
                fetch_invidious(f"/comments/{v}", force_instance=force_instance), timeout=5.0)
            if isinstance(res, dict) and not res.get("error") and res.get("comments"):
                return res
        except Exception as e:
            logger.debug(f"Invidious comments error: {e}")
        return None

    async def _sennin():
        try:
            resp = await asyncio.wait_for(
                client_session.get(f"{SENNIN_BASE}/api/comments/{v}",
                                   timeout=httpx.Timeout(4.0, connect=1.5)),
                timeout=5.0)
            if resp.status_code == 200:
                d = resp.json()
                if isinstance(d, dict) and d.get("comments"):
                    return d
        except Exception as e:
            logger.debug(f"Sennin comments error: {e}")
        return None

    if api == "sennin":
        res = await _sennin()
    elif api == "invidious":
        res = await _inv()
    else:
        tasks = [asyncio.create_task(_inv()), asyncio.create_task(_sennin())]
        res = await _race(tasks, 6.0, lambda r: isinstance(r, dict))

    if res:
        _cache_set(ckey, res, CACHE_CONFIG["comments"])
    return res or {}


# ─────────────────────────────────────────────
#  ルーター
# ─────────────────────────────────────────────
def _error_page(request: Request, exc: Exception, where: str, v: str, instances):
    logger.error(f"Error in {where} for {v}: {exc}")


@router.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts_player(
    request: Request, v: str,
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None, alias="info_api"),
    stream_api: Optional[str] = Query(None, alias="stream_api"),
    api: Optional[str] = Query(None),
):
    r_info = info_api or api or None
    r_stream = stream_api or api or None
    try:
        video_data, stream_data, comment_data = await asyncio.gather(
            fetch_video_info(v, force_instance=force_instance, api=r_info),
            fetch_fastest_stream_urls(v, api=r_stream, force_instance=force_instance),
            fetch_comments(v, force_instance=force_instance, api=r_info),
            return_exceptions=True,
        )
        if isinstance(video_data, Exception) and isinstance(stream_data, Exception):
            raise video_data

        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_data if isinstance(stream_data, dict) else {}

        video_urls = s_data.get("videoUrls", [])
        if not video_urls and v_data:
            video_urls = extract_invidious_streams(v_data).get("videoUrls", [])

        info_used = v_data.get("api_used", "unknown")
        return templates.TemplateResponse("short.html", {
            "request": request,
            "videoid": v,
            "video_title": v_data.get("title", ""),
            "videourls": video_urls,
            "author": v_data.get("author", ""),
            "view_count": v_data.get("viewCount", 0),
            "like_count": v_data.get("likeCount", 0),
            "description": (v_data.get("descriptionHtml")
                            or (v_data.get("description") or "").replace("\n", "<br>")),
            "comments": process_comments(comment_data),
            "info_api_used": info_used,
            "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": info_used,
        })
    except httpx.TimeoutException:
        logger.error(f"Timeout in shorts_player for {v}")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception as e:
        logger.error(f"Error in shorts_player for {v}: {e}")
        fb = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fb})


@router.get("/watch", response_class=HTMLResponse)
async def watch(
    request: Request,
    v: str = Query(...),
    list: Optional[str] = Query(None),
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None, alias="info_api"),
    stream_api: Optional[str] = Query(None, alias="stream_api"),
    api: Optional[str] = Query(None),
    nocache: bool = Query(False),
):
    r_info = info_api or api or None
    r_stream = stream_api or api or None
    try:
        async def _fetch_playlist() -> Optional[Dict[str, Any]]:
            if not list:
                return None
            try:
                res = await asyncio.wait_for(
                    fetch_invidious(f"/playlists/{list}", force_instance=force_instance), timeout=4.0)
                if isinstance(res, dict) and not res.get("error"):
                    return res
            except Exception as e:
                logger.debug(f"Playlist error: {e}")
            return None

        video_data, stream_res, comment_data, playlist_data = await asyncio.gather(
            fetch_video_info(v, force_instance=force_instance, api=r_info, nocache=nocache),
            fetch_fastest_stream_urls(v, api=r_stream, force_instance=force_instance),
            fetch_comments(v, force_instance=force_instance, api=r_info),
            _fetch_playlist(),
            return_exceptions=True,
        )
        if isinstance(video_data, Exception) and isinstance(stream_res, Exception):
            raise video_data

        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_res if isinstance(stream_res, dict) else {}
        p_data = playlist_data if isinstance(playlist_data, dict) else {}

        playlist_videos = [
            {"videoId": i.get("videoId"), "title": i.get("title"), "author": i.get("author")}
            for i in p_data.get("videos", []) if isinstance(i, dict)
        ]

        stream_urls = s_data.get("streamUrls", [])
        video_urls = s_data.get("videoUrls", [])
        if not stream_urls and v_data:
            inv = extract_invidious_streams(v_data)
            stream_urls, video_urls = inv["streamUrls"], inv["videoUrls"]

        recommended = [
            {
                "video_id": rec.get("video_id") or rec.get("videoId"),
                "title": rec.get("title"),
                "author": rec.get("author"),
                "view_count_text": rec.get("view_count_text") or rec.get("viewCountText"),
                "thumbnail": rec.get("thumbnail")
                             or ((rec.get("videoThumbnails") or [{}])[0].get("url", "")),
            }
            for rec in v_data.get("recommendedVideos", []) if isinstance(rec, dict)
        ]

        author_icon = v_data.get("authorIcon") or ""
        if not author_icon:
            th = v_data.get("authorThumbnails", [])
            author_icon = th[-1]["url"] if th else ""

        info_used = v_data.get("api_used", "unknown")
        response = templates.TemplateResponse("watch.html", {
            "request": request,
            "videoid": v,
            "video_title": v_data.get("title", ""),
            "videourls": video_urls,
            "streamUrls": stream_urls,
            "author": v_data.get("author", ""),
            "author_id": v_data.get("authorId", ""),
            "author_icon": author_icon,
            "subscribers_count": v_data.get("subCountText") or "非公開",
            "view_count": v_data.get("viewCount", 0),
            "like_count": v_data.get("likeCount", 0),
            "description": (v_data.get("descriptionHtml")
                            or (v_data.get("description") or "").replace("\n", "<br>")),
            "recommended_videos": recommended,
            "comments": process_comments(comment_data),
            "youtube_url": f"https://www.youtube.com/watch?v={v}",
            "info_api_used": info_used,
            "stream_api_used": s_data.get("stream_api_used", "unknown"),
            "api_used": info_used,
            "playlist_id": list,
            "playlist_title": p_data.get("title", ""),
            "playlist_videos": playlist_videos,
        })

        try:
            history = json.loads(request.cookies.get("history", "[]"))
            history = [h for h in history if h.get("videoId") != v]
            history.append({
                "videoId": v,
                "title": v_data.get("title", ""),
                "author": v_data.get("author", ""),
                "added_at": datetime.now().isoformat(),
            })
            response.set_cookie("history", json.dumps(history[-50:]), max_age=2592000,
                                httponly=True, samesite="Lax")
        except Exception as e:
            logger.debug(f"Failed to update history: {e}")

        return response

    except httpx.TimeoutException:
        logger.error(f"Timeout in watch for {v}")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception as e:
        logger.error(f"Error in watch for {v}: {e}")
        fb = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fb})


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
        return {"stats": dict(_API_STATS), "timestamp": datetime.now().isoformat()}
