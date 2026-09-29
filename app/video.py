import asyncio
import datetime
import hashlib
import json
import logging
import os
import pathlib
import re
import time
from typing import Optional, Dict, Any, List, Union, Tuple
from urllib.parse import quote, urlparse, urljoin

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)

router = APIRouter()

APP_NAME = "sennin-tube-plus"

INVIDIOUS_LIST_URL = "https://raw.githubusercontent.com/kuru-bana/yt-data/refs/heads/main/list/injidious.json"
INNERTUBE_BASE = "https://choco-youtube-js.onrender.com"
CACHE_TTL = 5 * 60

# nocookie 埋め込みが使えるので、APIの結果は最大この秒数だけ待ってページを表示する
PAGE_WAIT_SECONDS = 4.0
NOCOOKIE_EMBED_BASE = "https://www.youtube-nocookie.com/embed/"

KEEPALIVE_TARGETS = [
    f"{INNERTUBE_BASE}/version",
]

_CLIENT_TIMEOUT = httpx.Timeout(connect=5.0, read=18.0, write=5.0, pool=5.0)
_CLIENT_LIMITS = httpx.Limits(max_keepalive_connections=20, keepalive_expiry=30.0)

http_client: httpx.AsyncClient = None


async def get_client() -> httpx.AsyncClient:
    global http_client
    if http_client is None or http_client.is_closed:
        http_client = httpx.AsyncClient(
            timeout=_CLIENT_TIMEOUT,
            limits=_CLIENT_LIMITS,
            follow_redirects=True,
        )
    return http_client


_keepalive_self_url: str = ""


async def _periodic_keepalive():
    await asyncio.sleep(60)
    while True:
        targets = list(KEEPALIVE_TARGETS)
        if _keepalive_self_url:
            targets.append(f"{_keepalive_self_url}/whats")
        for t in targets:
            try:
                client = await get_client()
                await client.get(t, timeout=10)
            except Exception:
                pass
        await asyncio.sleep(10 * 60)


LINKUP_JSON_URL = "https://raw.githubusercontent.com/kuru-bana/Choco-Tube-Plus/main/linkup.json"


async def _fetch_linkup_bases() -> "list[str]":
    try:
        client = await get_client()
        resp = await client.get(LINKUP_JSON_URL, timeout=10)
        text = resp.text
        bases = re.findall(r'https?://[^\s\],\'"]+', text)
        return [b.rstrip("/") for b in bases if b]
    except Exception:
        return []


async def _ping_keepalive(self_url: str):
    bases = await _fetch_linkup_bases()
    if not bases:
        return
    for base in bases:
        try:
            client = await get_client()
            resp = await client.get(f"{base}/url={self_url}", timeout=15)
            if resp.status_code < 500:
                return
        except Exception:
            continue


templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")


def _get_static_ver() -> str:
    h = hashlib.md5()
    static_root = pathlib.Path("templates/static")
    try:
        for f in sorted(static_root.rglob("*.js")) + sorted(static_root.rglob("*.css")):
            h.update(f.read_bytes())
        return h.hexdigest()[:8]
    except Exception:
        pass
    return str(int(time.time()))


_STATIC_VER = _get_static_ver()
templates.env.globals["static_ver"] = _STATIC_VER

category_cache: dict = {}

_proxy_resp_cache: dict = {}
_proxy_resp_ttl: dict = {
    "video": 180,
    "trending": 300,
    "trending_music": 300,
    "trending_gaming": 300,
    "trending_news": 300,
    "trending_movies": 300,
    "search": 60,
    "search_suggestions": 30,
    "channel": 120,
    "channel_videos": 120,
    "channel_shorts": 120,
    "channel_streams": 120,
    "channel_latest": 120,
    "popular": 300,
    "hashtag": 90,
    "comments": 120,
    "playlist": 180,
}
_PROXY_RESP_DEFAULT_TTL = 60
_PROXY_RESP_MAX_SIZE = 150

_proxy_inflight: dict = {}


_INVIDIOUS_LIST_KEY = "__invidious_list__"


async def _fetch_invidious_list() -> list:
    now = time.time()
    cached = category_cache.get(_INVIDIOUS_LIST_KEY)
    if cached and now - cached["time"] < CACHE_TTL:
        return cached["instances"]
    try:
        client = await get_client()
        resp = await client.get(INVIDIOUS_LIST_URL, timeout=10)
        resp.raise_for_status()
        instances = resp.json()
        if not isinstance(instances, list):
            instances = []
    except Exception:
        if cached:
            return cached["instances"]
        raise
    if not instances:
        if cached:
            return cached["instances"]
        return []
    category_cache[_INVIDIOUS_LIST_KEY] = {"instances": instances, "time": now}
    return instances


async def get_instances(category: str) -> list:
    return await _fetch_invidious_list()


async def get_video_back_instances() -> list:
    return await _fetch_invidious_list()


_RETRYABLE = (httpx.RemoteProtocolError, httpx.LocalProtocolError, httpx.ConnectError)


async def _try_instance(base: str, invidious_path: str) -> dict:
    client = await get_client()
    for attempt in range(2):
        try:
            resp = await client.get(base + invidious_path)
            resp.raise_for_status()
            return {"data": resp.json(), "used_instance": base}
        except _RETRYABLE:
            if attempt == 0:
                continue
            raise


def _has_valid_videos(data) -> bool:
    items = data if isinstance(data, list) else data.get("videos", [])
    return any(
        not item.get("errorMessage") and (item.get("videoId") or item.get("title"))
        for item in items
    )


def _has_valid_stream(data) -> bool:
    if not isinstance(data, dict):
        return False
    streams = data.get("formatStreams") or data.get("adaptiveFormats") or []
    return any(isinstance(s, dict) and s.get("url") for s in streams)


_innertube_cont_cache: dict = {}
_INNERTUBE_CONT_TTL = 600


def _innertube_cont_set(channel_id: str, tab: str, inv_cont: str, innertube_key: str):
    key = (channel_id, tab, inv_cont)
    _innertube_cont_cache[key] = {"key": innertube_key, "time": time.time()}
    now = time.time()
    stale = [k for k, v in _innertube_cont_cache.items() if now - v["time"] > _INNERTUBE_CONT_TTL]
    for k in stale:
        del _innertube_cont_cache[k]


def _innertube_cont_get(channel_id: str, tab: str, inv_cont: str) -> "str | None":
    key = (channel_id, tab, inv_cont)
    entry = _innertube_cont_cache.get(key)
    if not entry:
        return None
    if time.time() - entry["time"] > _INNERTUBE_CONT_TTL:
        del _innertube_cont_cache[key]
        return None
    return entry["key"]


def _extract_innertube_videos(data: dict) -> tuple:
    result = []
    raw_cont_token = data.get("_rawContToken") or None

    contents = data.get("current_tab", {}).get("content", {}).get("contents", [])
    for item in contents:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "ContinuationItem":
            if not raw_cont_token:
                raw_cont_token = item.get("endpoint", {}).get("payload", {}).get("token")
            continue
        lv = item.get("content", {})
        if not isinstance(lv, dict) or lv.get("content_type") != "VIDEO":
            continue
        video_id = lv.get("content_id")
        if not video_id:
            continue
        title_obj = lv.get("metadata", {}).get("title", {})
        title = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj)
        result.append({"videoId": video_id, "title": title})

    if not result:
        for item in (data.get("videos") or data.get("items") or []):
            if not isinstance(item, dict):
                continue
            video_id = item.get("videoId") or item.get("id")
            if not video_id:
                continue
            title_obj = item.get("title", {})
            title = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj)
            result.append({"videoId": video_id, "title": title})

    return result, raw_cont_token


async def fetch_innertube_videos(channel_id: str, tab: str) -> tuple:
    tab_map = {"videos": "videos", "shorts": "shorts", "streams": "live", "latest": "videos"}
    innertube_tab = tab_map.get(tab, "videos")
    try:
        client = await get_client()
        resp = await client.get(
            f"{INNERTUBE_BASE}/channel/{channel_id}/{innertube_tab}",
            timeout=httpx.Timeout(30.0),
        )
        resp.raise_for_status()
        return _extract_innertube_videos(resp.json())
    except Exception:
        return [], None


async def fetch_innertube_continuation(raw_token: str) -> tuple:
    try:
        client = await get_client()
        resp = await client.get(
            f"{INNERTUBE_BASE}/channel/continue-raw",
            params={"token": raw_token},
            timeout=httpx.Timeout(30.0),
        )
        resp.raise_for_status()
        return _extract_innertube_videos(resp.json())
    except Exception:
        return [], None


def _innertube_to_invidious(innertube_items: list, channel_id: str = "") -> list:
    return [
        {
            "videoId": item["videoId"],
            "title": item["title"],
            "author": "",
            "authorId": channel_id,
            "lengthSeconds": 0,
            "viewCount": 0,
            "publishedText": "",
            "authorThumbnails": [],
        }
        for item in innertube_items
    ]


def _apply_enrichment(data, innertube_items: list, channel_id: str):
    if not innertube_items:
        return data

    is_list = isinstance(data, list)
    items = data if is_list else data.get("videos", [])

    valid_items = [v for v in items if not v.get("errorMessage")]
    items_needing_id = [v for v in valid_items if not v.get("videoId") and v.get("title")]

    if not items_needing_id and valid_items:
        return data

    if not valid_items:
        fallback = _innertube_to_invidious(innertube_items, channel_id)
        return fallback if is_list else {"videos": fallback, "continuation": None}

    title_map: dict = {}
    for iv in innertube_items:
        t = iv.get("title", "").strip()
        vid = iv.get("videoId")
        if t and vid:
            title_map[t] = vid
            for length in (80, 60, 40, 20):
                if len(t) >= length:
                    title_map[t[:length]] = vid

    for item in items_needing_id:
        title = item.get("title", "").strip()
        vid = title_map.get(title)
        if not vid:
            for length in (80, 60, 40, 20):
                vid = title_map.get(title[:length])
                if vid:
                    break
        if not vid:
            for k, v in title_map.items():
                min_len = min(len(k), len(title), 25)
                if min_len >= 15 and k[:min_len] == title[:min_len]:
                    vid = v
                    break
        if vid:
            item["videoId"] = vid

    return data


def _proxy_cache_get(key: str) -> "dict | None":
    entry = _proxy_resp_cache.get(key)
    if not entry:
        return None
    if time.time() - entry["time"] > entry["ttl"]:
        _proxy_resp_cache.pop(key, None)
        return None
    return entry["data"]


def _is_valid_proxy_data(data) -> bool:
    if data is None:
        return False
    if isinstance(data, dict):
        if data.get("error"):
            return False
        if not data:
            return False
    if isinstance(data, list) and len(data) == 0:
        return False
    return True


def _proxy_cache_set(key: str, result: dict, ttl: int) -> None:
    data = result.get("data") if isinstance(result, dict) else result
    if not _is_valid_proxy_data(data):
        return
    if len(_proxy_resp_cache) >= _PROXY_RESP_MAX_SIZE:
        now = time.time()
        stale = [k for k, v in _proxy_resp_cache.items() if now - v["time"] > v["ttl"]]
        for k in stale:
            _proxy_resp_cache.pop(k, None)
        if len(_proxy_resp_cache) >= _PROXY_RESP_MAX_SIZE:
            oldest = min(_proxy_resp_cache, key=lambda k: _proxy_resp_cache[k]["time"])
            _proxy_resp_cache.pop(oldest, None)
    _proxy_resp_cache[key] = {"data": result, "time": time.time(), "ttl": ttl}


async def proxy_parallel(
    category: str,
    invidious_path: str,
    exclude_list: list = None,
    prefer_valid_videos: bool = False,
    prefer_valid_stream: bool = False,
    override_instances: list = None,
    no_cache: bool = False,
) -> dict:
    # prefer_* が違う呼び出し同士でキャッシュ/待ち合わせが混ざらないようキーに含める
    cache_key = (
        f"{category}:{invidious_path}:"
        f"{int(prefer_valid_videos)}{int(prefer_valid_stream)}"
    )
    ttl = _proxy_resp_ttl.get(category, _PROXY_RESP_DEFAULT_TTL)
    use_cache = not no_cache and not exclude_list and override_instances is None

    if use_cache:
        cached = _proxy_cache_get(cache_key)
        if cached is not None:
            return cached

        if cache_key in _proxy_inflight:
            waiting = _proxy_inflight[cache_key]
            try:
                return await asyncio.shield(waiting)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    if use_cache:
        _proxy_inflight[cache_key] = fut

    try:
        instances = (
            override_instances
            if override_instances is not None
            else await get_instances(category)
        )
        if exclude_list:
            instances = [
                b for b in instances
                if not any(ex in b or b in ex for ex in exclude_list)
            ]
        if not instances:
            raise Exception(f'No working instances for category "{category}" after exclusions')

        task_to_base = {
            asyncio.create_task(_try_instance(base, invidious_path)): base
            for base in instances
        }
        tasks = list(task_to_base)
        errors = []
        pending = set(tasks)
        winner = None
        fallback = None
        try:
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    exc = task.exception()
                    if exc is None:
                        result = task.result()
                        if prefer_valid_videos:
                            if _has_valid_videos(result["data"]):
                                if winner is None:
                                    winner = result
                            else:
                                if fallback is None:
                                    fallback = result
                        elif prefer_valid_stream:
                            if _has_valid_stream(result["data"]):
                                if winner is None:
                                    winner = result
                            else:
                                if fallback is None:
                                    fallback = result
                        elif winner is None:
                            winner = result
                    else:
                        msg = str(exc) or type(exc).__name__
                        errors.append(f"{task_to_base.get(task, '?')}:{msg}")
                if winner is not None:
                    break
        finally:
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        best = winner or fallback
        if best is None:
            category_cache.pop(category, None)
            raise Exception("All instances failed: " + ", ".join(errors))

        if use_cache:
            _proxy_cache_set(cache_key, best, ttl)

        if not fut.done():
            fut.set_result(best)
        return best

    except Exception as exc:
        if not fut.done():
            fut.set_exception(exc)
            try:
                fut.exception()
            except BaseException:
                pass
        raise
    finally:
        # キャンセル(CancelledError)でも待機側が永遠に固まらないよう必ず解決する
        if not fut.done():
            fut.set_exception(RuntimeError("request cancelled"))
            try:
                fut.exception()
            except BaseException:
                pass
        if _proxy_inflight.get(cache_key) is fut:
            _proxy_inflight.pop(cache_key, None)


def map_path(app_path: str) -> tuple:
    m = re.match(r"^/api/trending/(music|gaming|news|movies)([?].*)?$", app_path, re.IGNORECASE)
    if m:
        type_name = m.group(1).lower()
        qs_part = m.group(2) or ""
        type_map = {"music": "Music", "gaming": "Gaming", "news": "News", "movies": "Movies"}
        if qs_part:
            invidious_path = f"/api/v1/trending{qs_part}&type={type_map[type_name]}"
        else:
            invidious_path = f"/api/v1/trending?type={type_map[type_name]}"
        return f"trending_{type_name}", invidious_path

    m = re.match(r"^/api/stream/([^?]+)(.*)", app_path)
    if m:
        return "video", f"/api/v1/videos/{m.group(1)}{m.group(2)}"

    if app_path.startswith("/api/search/suggestions"):
        return (
            "search_suggestions",
            "/api/v1/search/suggestions" + app_path[len("/api/search/suggestions"):],
        )

    m = re.match(
        r"^/api/channels/([^/?]+)/(videos|shorts|streams|latest|playlists|comments|search)(.*)",
        app_path,
    )
    if m:
        sub = m.group(2)
        return f"channel_{sub}", f"/api/v1/channels/{m.group(1)}/{sub}{m.group(3)}"

    prefix_map = [
        ("/api/trending", "trending"),
        ("/api/search", "search"),
        ("/api/channels", "channel"),
        ("/api/videos", "video"),
        ("/api/playlists", "playlist"),
        ("/api/mixes", "mix"),
        ("/api/hashtag", "hashtag"),
        ("/api/comments", "comments"),
        ("/api/transcripts", "transcripts"),
        ("/api/captions", "captions"),
        ("/api/annotations", "annotations"),
        ("/api/clip", "clip"),
        ("/api/resolveurl", "resolveurl"),
        ("/api/popular", "popular"),
        ("/api/stats", "stats"),
    ]
    for prefix, category in prefix_map:
        if app_path.startswith(prefix):
            return category, "/api/v1" + app_path[4:]

    return "video", "/api/v1" + app_path[4:]


CHANNEL_VIDEO_CATEGORIES = {"channel_videos", "channel_shorts", "channel_streams", "channel_latest"}
_CH_TAB_RE = re.compile(r"^/api/channels/([^/?]+)/(videos|shorts|streams|latest)")


INFO_API_CONFIG = {
    "invidious": {
        "priority": 1,
        "timeout": 4.0,
        "description": "Invidious 集約API",
        "features": ["video info", "comments", "recommendations", "streams"],
        "max_retries": 2,
    },
    "piped": {
        "priority": 2,
        "timeout": 12.0,
        "description": "Piped API",
        "features": ["video info", "recommendations", "streams"],
        "max_retries": 2,
    },
    "sia": {
        "priority": 3,
        "timeout": 5.0,
        "description": "Sia Tube API",
        "features": ["video info", "recommendations", "streams"],
        "max_retries": 2,
    },
    "sennin": {
        "priority": 4,
        "timeout": 4.0,
        "description": "Sennin API",
        "features": ["video info", "extended stats"],
        "max_retries": 1,
    },
}


CACHE_CONFIG = {
    "video_info": 300.0,
    "comments": 600.0,
    "streams": 600.0,
    "recommended": 1800.0,
}


_API_STATS: Dict[str, Dict[str, Any]] = {
    "invidious": {"success": 0, "failure": 0, "avg_time": 0.0},
    "piped": {"success": 0, "failure": 0, "avg_time": 0.0},
    "sia": {"success": 0, "failure": 0, "avg_time": 0.0},
    "sennin": {"success": 0, "failure": 0, "avg_time": 0.0},
}

_STATS_LOCK = asyncio.Lock()


_simple_cache: Dict[str, Tuple[Any, float]] = {}
_cache_lock = asyncio.Lock()


async def _cache_get(key: str) -> Optional[Any]:
    async with _cache_lock:
        entry = _simple_cache.get(key)
        if entry is None:
            return None
        value, expire_at = entry
        if time.monotonic() > expire_at:
            _simple_cache.pop(key, None)
            return None
        return value


async def _cache_set(key: str, value: Any, ttl: float) -> None:
    async with _cache_lock:
        _simple_cache[key] = (value, time.monotonic() + ttl)


async def _cache_delete(key: str) -> None:
    async with _cache_lock:
        _simple_cache.pop(key, None)


async def record_api_performance(api: str, success: bool, duration: float) -> None:
    async with _STATS_LOCK:
        if api not in _API_STATS:
            return
        stats = _API_STATS[api]
        if success:
            old_total = stats["success"] + stats["failure"]
            stats["success"] += 1
            new_total = old_total + 1
            if new_total > 0:
                stats["avg_time"] = ((stats["avg_time"] * old_total) + duration) / new_total
        else:
            stats["failure"] += 1


DEFAULT_UA = (
    "Mozilla/5.0 "
    "(X11; Linux x86_64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/144.0.0.0 "
    "Safari/537.36"
)


_PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]


async def _fetch_piped_data(video_id: str) -> Optional[Dict[str, Any]]:
    """Piped の全インスタンスへ並列に問い合わせ、最初に成功したものを返す。"""

    async def one(instance: str) -> Optional[Dict[str, Any]]:
        try:
            client = await get_client()
            response = await client.get(
                f"{instance}/streams/{video_id}",
                timeout=httpx.Timeout(12.0, connect=4.0, read=8.0),
                headers={"User-Agent": DEFAULT_UA},
            )
            if response.status_code != 200:
                return None
            data = response.json()
            if not isinstance(data, dict):
                return None
            if data.get("error"):
                return None
            if not data.get("title"):
                return None
            data["_piped_instance"] = instance
            return data
        except Exception as exc:
            logger.debug("Piped instance failed %s: %s", instance, exc)
            return None

    tasks = [asyncio.create_task(one(i)) for i in _PIPED_INSTANCES]
    try:
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                return result
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return None


def _piped_date_to_relative(date_str: str) -> str:
    try:
        dt = datetime.date.fromisoformat(date_str[:10])
        today = datetime.date.today()
        days = (today - dt).days
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


def _format_sub_count(count: Any) -> str:
    try:
        count = int(count or 0)
    except Exception:
        return str(count or "")
    if count <= 0:
        return ""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{round(count / 1_000)}K"
    return str(count)


def _piped_to_video_info(piped: Dict[str, Any], video_id: str) -> Dict[str, Any]:
    uploader_url = piped.get("uploaderUrl", "") or ""
    author_id = ""

    if "/channel/" in uploader_url:
        author_id = uploader_url.split("/channel/")[-1].strip("/")
    elif uploader_url.startswith("/@"):
        author_id = uploader_url[1:]
    elif uploader_url.startswith("/c/"):
        author_id = uploader_url[3:]

    avatar = piped.get("uploaderAvatar", "") or ""

    author_thumbnails = []
    if avatar:
        author_thumbnails = [{"url": avatar, "width": 48, "height": 48}]

    thumbnail = piped.get("thumbnailUrl", "") or ""

    recommended = []

    for item in (piped.get("relatedStreams") or []):
        if not isinstance(item, dict):
            continue
        if item.get("type") not in (None, "stream"):
            continue

        raw_url = item.get("url", "") or ""
        related_id = ""

        if "?v=" in raw_url:
            related_id = raw_url.split("?v=", 1)[1].split("&", 1)[0]
        elif "/watch?v=" in raw_url:
            related_id = raw_url.split("/watch?v=", 1)[1].split("&", 1)[0]

        if not related_id:
            related_id = item.get("videoId") or item.get("id") or ""
        if not related_id:
            continue

        related_thumb = item.get("thumbnail", "") or ""

        recommended.append(
            {
                "videoId": related_id,
                "video_id": related_id,
                "title": item.get("title", ""),
                "author": item.get("uploaderName", ""),
                "authorId": "",
                "lengthSeconds": item.get("duration", 0) or 0,
                "viewCount": item.get("views", 0) or 0,
                "view_count_text": str(item.get("views", "") or ""),
                "publishedText": item.get("uploadedDate", ""),
                "thumbnail": related_thumb,
                "videoThumbnails": (
                    [{"quality": "hq", "url": related_thumb}] if related_thumb else []
                ),
            }
        )

    sub_count = piped.get("uploaderSubscriberCount") or 0

    return {
        "videoId": video_id,
        "title": piped.get("title", ""),
        "author": piped.get("uploader", "") or "",
        "authorId": author_id,
        "authorIcon": avatar,
        "authorThumbnails": author_thumbnails,
        "subCount": sub_count,
        "subCountText": _format_sub_count(sub_count),
        "viewCount": piped.get("views", 0) or 0,
        "likeCount": piped.get("likes", 0) or 0,
        "publishedText": _piped_date_to_relative(piped.get("uploadDate", "") or ""),
        "description": piped.get("description", "") or "",
        "descriptionHtml": str(piped.get("description", "") or "").replace("\n", "<br>"),
        "lengthSeconds": piped.get("duration", 0) or 0,
        "recommendedVideos": recommended,
        "thumbnail": thumbnail,
        "_source": "piped",
    }


_SIA_BASE_URL = os.environ.get("SIA_API_BASE", "https://siatube.com")


async def fetch_sia_video(video_id: str) -> Optional[Dict[str, Any]]:
    cache_key = f"sia_video:{video_id}"

    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    start = time.monotonic()

    try:
        client = await get_client()

        response = await client.get(
            f"{_SIA_BASE_URL}/api/video/{video_id}",
            timeout=httpx.Timeout(5.0, connect=2.0, read=3.0),
            headers={"User-Agent": DEFAULT_UA},
        )

        if response.status_code != 200:
            raise RuntimeError(f"Sia HTTP {response.status_code}")

        data = response.json()

        if not isinstance(data, dict):
            raise RuntimeError("invalid Sia response")

        author_info = data.get("author", {})
        if not isinstance(author_info, dict):
            author_info = {}

        author_name = author_info.get("name") or data.get("uploader") or ""
        if not author_name:
            raise RuntimeError("Sia returned no author")

        author_id = author_info.get("id") or ""
        author_icon = author_info.get("thumbnail") or ""
        sub_count = author_info.get("subscribers") or "非公開"

        description = data.get("description", "")
        if isinstance(description, dict):
            description_text = description.get("text", "")
        else:
            description_text = str(description or "")

        related = data.get("Related-videos") or data.get("relatedVideos") or {}
        if isinstance(related, dict):
            raw_related = related.get("relatedVideos", [])
        elif isinstance(related, list):
            raw_related = related
        else:
            raw_related = []

        result = {
            "videoId": video_id,
            "title": data.get("title", ""),
            "author": author_name,
            "authorId": author_id,
            "authorIcon": author_icon,
            "authorThumbnails": (
                [{"url": author_icon, "width": 48, "height": 48}] if author_icon else []
            ),
            "subCountText": str(sub_count),
            "subCount": (
                author_info.get("subscribers", 0)
                if isinstance(author_info.get("subscribers"), int)
                else 0
            ),
            "viewCount": data.get("views", 0) or 0,
            "likeCount": data.get("likes", 0) or 0,
            "description": description_text,
            "descriptionHtml": description_text.replace("\n", "<br>"),
            "recommendedVideos": _process_related_videos(raw_related),
            "thumbnail": data.get("thumbnail", ""),
            "lengthSeconds": data.get("duration", 0) or 0,
            "_source": "sia",
        }

        await _cache_set(cache_key, result, CACHE_CONFIG["video_info"])
        await record_api_performance("sia", True, time.monotonic() - start)
        return result

    except Exception as exc:
        logger.debug("Sia video error %s: %s", video_id, exc)
        await record_api_performance("sia", False, time.monotonic() - start)
        return None


_SENNIN_BASE_URL = os.environ.get(
    "SENNIN_API_BASE",
    "https://ytapi-production-90b2.up.railway.app",
)


def normalize_sennin_video_info(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    author_info = data.get("author", {})
    if not isinstance(author_info, dict):
        author_info = {}

    author_name = author_info.get("name") or ""
    author_id = author_info.get("id") or ""
    author_icon = author_info.get("thumbnail") or ""
    sub_count = author_info.get("subscribers") or "非公開"

    description = data.get("description", "")

    if isinstance(description, dict):
        description_text = description.get("text", "")
        description_html = description.get("formatted") or description_text.replace("\n", "<br>")
    else:
        description_text = str(description or "")
        description_html = description_text.replace("\n", "<br>")

    related = data.get("Related-videos", {})
    if isinstance(related, dict):
        raw_related = related.get("relatedVideos", [])
    else:
        raw_related = []

    return {
        "videoId": data.get("videoId", ""),
        "title": data.get("title", ""),
        "author": author_name,
        "authorId": author_id,
        "authorIcon": author_icon,
        "authorThumbnails": (
            [{"url": author_icon, "width": 48, "height": 48}] if author_icon else []
        ),
        "subCountText": str(sub_count),
        "subCount": sub_count if isinstance(sub_count, int) else 0,
        "viewCount": (
            data.get("views")
            or (data.get("extended_stats", {}) or {}).get("views_original", 0)
        ),
        "likeCount": data.get("likes", 0),
        "description": description_text,
        "descriptionHtml": description_html,
        "recommendedVideos": _process_related_videos(raw_related),
        "thumbnail": data.get("thumbnail", ""),
        "lengthSeconds": data.get("duration", 0) or 0,
        "_source": "sennin",
    }


async def fetch_sennin_video_info(video_id: str) -> Optional[Dict[str, Any]]:
    cache_key = f"sennin_video:{video_id}"

    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    start = time.monotonic()

    try:
        client = await get_client()

        response = await client.get(
            f"{_SENNIN_BASE_URL}/api/video/{video_id}",
            timeout=httpx.Timeout(4.0, connect=1.5, read=2.5),
            headers={"User-Agent": DEFAULT_UA},
        )

        if response.status_code != 200:
            raise RuntimeError(f"Sennin HTTP {response.status_code}")

        data = response.json()

        if not isinstance(data, dict):
            raise RuntimeError("invalid Sennin response")

        if data.get("unavailable"):
            raise RuntimeError("video unavailable")

        result = normalize_sennin_video_info(data)

        if not result.get("title"):
            raise RuntimeError("Sennin returned no title")

        result["api_used"] = "sennin"

        await _cache_set(cache_key, result, CACHE_CONFIG["video_info"])
        await record_api_performance("sennin", True, time.monotonic() - start)
        return result

    except Exception as exc:
        logger.debug("Sennin video error %s: %s", video_id, exc)
        await record_api_performance("sennin", False, time.monotonic() - start)
        return None


def _process_related_videos(raw_rel: List[Any]) -> List[Dict[str, Any]]:
    recommended = []

    for item in raw_rel or []:
        if not isinstance(item, dict):
            continue

        video_id = item.get("videoId") or item.get("id") or ""
        if not video_id:
            continue

        thumbnail = item.get("thumbnail", "") or ""

        if (
            not thumbnail
            and isinstance(item.get("thumbnails"), list)
            and item["thumbnails"]
        ):
            first = item["thumbnails"][0]
            if isinstance(first, dict):
                thumbnail = first.get("url", "")

        author = (
            item.get("channelName")
            or item.get("uploaderName")
            or item.get("author")
            or ""
        )

        view_text = item.get("viewCountText") or item.get("view_count_text") or ""

        recommended.append(
            {
                "videoId": video_id,
                "video_id": video_id,
                "title": item.get("title", ""),
                "author": author,
                "authorId": item.get("authorId") or "",
                "viewCount": item.get("viewCount", 0) or 0,
                "view_count_text": view_text,
                "lengthSeconds": item.get("lengthSeconds", 0) or 0,
                "publishedText": item.get("publishedText", "") or "",
                "thumbnail": thumbnail,
                "videoThumbnails": (
                    [{"url": thumbnail, "width": 320, "height": 180}] if thumbnail else []
                ),
            }
        )

    return recommended


async def fetch_video_info_invidious_robust(
    video_id: str,
    force_instance: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    start = time.monotonic()

    try:
        result = await proxy_parallel(
            "video",
            f"/api/v1/videos/{video_id}",
            override_instances=([force_instance] if force_instance else None),
            prefer_valid_stream=False,
        )

        data = result.get("data") if isinstance(result, dict) else None

        if (
            isinstance(data, dict)
            and not data.get("error")
            and (data.get("title") or data.get("videoId"))
        ):
            data = dict(data)
            data["api_used"] = "invidious"
            await record_api_performance("invidious", True, time.monotonic() - start)
            return data

    except Exception as exc:
        logger.debug("Invidious video error %s: %s", video_id, exc)

    await record_api_performance("invidious", False, time.monotonic() - start)
    return None


async def fetch_video_info(
    video_id: str,
    force_instance: Optional[str] = None,
    api: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    cache_key = f"video_info:{video_id}:{force_instance or ''}:{api or 'auto'}"

    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    if api == "invidious":
        result = await fetch_video_info_invidious_robust(video_id, force_instance)

    elif api == "piped":
        data = await _fetch_piped_data(video_id)
        result = _piped_to_video_info(data, video_id) if data else None
        if result:
            result["api_used"] = "piped"

    elif api == "sia":
        result = await fetch_sia_video(video_id)
        if result:
            result["api_used"] = "sia"

    elif api == "sennin":
        result = await fetch_sennin_video_info(video_id)

    else:
        result = await _fetch_video_info_fastest(video_id, force_instance)

    if result:
        await _cache_set(cache_key, result, CACHE_CONFIG["video_info"])

    return result


async def _fetch_video_info_fastest(
    video_id: str,
    force_instance: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    tasks = [
        asyncio.create_task(fetch_video_info_invidious_robust(video_id, force_instance)),
        asyncio.create_task(_fetch_piped_info_wrapper(video_id)),
        asyncio.create_task(fetch_sia_video(video_id)),
        asyncio.create_task(fetch_sennin_video_info(video_id)),
    ]

    try:
        for task in asyncio.as_completed(tasks, timeout=15.0):
            try:
                result = await task
                if isinstance(result, dict) and result.get("title"):
                    return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("Video info task error: %s", exc)

    except asyncio.TimeoutError:
        logger.debug("Video info fastest timeout: %s", video_id)

    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    return None


async def _fetch_piped_info_wrapper(video_id: str) -> Optional[Dict[str, Any]]:
    data = await _fetch_piped_data(video_id)
    if not data:
        return None

    result = _piped_to_video_info(data, video_id)
    result["api_used"] = "piped"
    await record_api_performance("piped", True, 0.0)
    return result


def _codec_name(mime: str) -> Tuple[str, str]:
    mime = mime or ""
    container = "mp4"

    if "webm" in mime:
        container = "webm"
    elif "audio/mp4" in mime:
        container = "m4a"

    codecs = ""
    match = re.search(r'codecs=["\']([^"\']+)', mime)
    if match:
        codecs = match.group(1).split(",")[0].strip().lower()

    if codecs.startswith("avc1") or codecs == "h264":
        encoding = "H.264"
    elif codecs.startswith("vp09") or codecs == "vp9":
        encoding = "VP9"
    elif codecs.startswith("av01") or codecs == "av1":
        encoding = "AV1"
    elif codecs.startswith("mp4a"):
        encoding = "AAC"
    elif codecs == "opus":
        encoding = "Opus"
    else:
        encoding = codecs

    return container, encoding


def _normalize_invidious_streams(data: Dict[str, Any]) -> Dict[str, List]:
    if not isinstance(data, dict):
        return {
            "streamUrls": [],
            "videoUrls": [],
            "formatStreams": [],
            "adaptiveFormats": [],
        }

    format_streams = []
    adaptive_formats = []

    for fmt in (data.get("formatStreams") or []):
        if not isinstance(fmt, dict):
            continue
        url = fmt.get("url", "")
        if not url:
            continue

        mime = fmt.get("type", fmt.get("mimeType", ""))
        container, encoding = _codec_name(mime)

        format_streams.append(
            {
                "url": url,
                "itag": str(fmt.get("itag", "")),
                "type": mime,
                "quality": fmt.get("quality", ""),
                "qualityLabel": fmt.get("qualityLabel", fmt.get("quality", "")),
                "fps": fmt.get("fps", 30),
                "size": (
                    f"{fmt.get('width')}x{fmt.get('height')}"
                    if fmt.get("width") and fmt.get("height")
                    else ""
                ),
                "bitrate": str(fmt.get("bitrate", 0)),
                "container": container,
                "encoding": encoding,
            }
        )

    for fmt in (data.get("adaptiveFormats") or []):
        if not isinstance(fmt, dict):
            continue
        url = fmt.get("url", "")
        if not url:
            continue

        mime = fmt.get("type", fmt.get("mimeType", ""))
        container, encoding = _codec_name(mime)

        adaptive_formats.append(
            {
                "url": url,
                "itag": str(fmt.get("itag", "")),
                "type": mime,
                "quality": fmt.get("quality", ""),
                "qualityLabel": fmt.get("qualityLabel", ""),
                "fps": fmt.get("fps", 0),
                "size": (
                    f"{fmt.get('width')}x{fmt.get('height')}"
                    if fmt.get("width") and fmt.get("height")
                    else ""
                ),
                "bitrate": str(fmt.get("bitrate", 0)),
                "container": container,
                "encoding": encoding,
            }
        )

    video_urls = [item["url"] for item in format_streams if item.get("url")]

    if not video_urls:
        video_urls = [
            item["url"]
            for item in adaptive_formats
            if "video" in str(item.get("type", "")) and item.get("url")
        ]

    stream_urls = []

    for fmt in format_streams:
        stream_urls.append(
            {
                "url": fmt.get("url"),
                "resolution": fmt.get("qualityLabel"),
                "format": "mp4/mixed",
                "audioUrl": "",
            }
        )

    audio_url = None

    for fmt in adaptive_formats:
        mime = str(fmt.get("type", ""))
        if "audio" in mime and fmt.get("url"):
            if fmt.get("language") == "ja":
                audio_url = fmt.get("url")
                break
            if audio_url is None:
                audio_url = fmt.get("url")

    for fmt in adaptive_formats:
        mime = str(fmt.get("type", ""))
        if "video" in mime and fmt.get("url"):
            stream_urls.append(
                {
                    "url": fmt.get("url"),
                    "resolution": fmt.get("qualityLabel"),
                    "format": f"{fmt.get('container', 'mp4')}/videoOnly",
                    "audioUrl": audio_url or "",
                }
            )

    return {
        "streamUrls": stream_urls,
        "videoUrls": video_urls,
        "formatStreams": format_streams,
        "adaptiveFormats": adaptive_formats,
    }


def _normalize_codec(codec: str) -> str:
    codec = str(codec or "").lower()

    if codec.startswith("avc1") or codec == "h264":
        return "H.264"
    if codec.startswith("vp09") or codec == "vp9":
        return "VP9"
    if codec.startswith("av01") or codec == "av1":
        return "AV1"
    if codec.startswith("mp4a"):
        return "AAC"
    if codec == "opus":
        return "Opus"

    return codec


def _piped_stream_result(data: Dict[str, Any]) -> Dict[str, Any]:
    format_streams = []
    adaptive_formats = []

    combined_url = None
    hls_url = data.get("hls") or None

    video_streams = data.get("videoStreams") or []

    for stream in video_streams:
        if not isinstance(stream, dict):
            continue

        url = stream.get("url", "")
        if not url:
            continue

        fmt = str(stream.get("format", "mp4")).lower()
        video_only = stream.get("videoOnly", True)
        quality = stream.get("quality", "")
        width = stream.get("width", 0)
        height = stream.get("height", 0)

        codec = stream.get("videoCodec") or stream.get("vcodec") or ""
        encoding = _normalize_codec(codec)

        item = {
            "url": url,
            "itag": str(stream.get("formatId", "")),
            "type": f"video/{fmt}",
            "quality": quality,
            "qualityLabel": quality,
            "fps": stream.get("fps", 30) or 30,
            "size": (f"{width}x{height}" if width and height else ""),
            "bitrate": str(int(stream.get("bitrate", stream.get("tbr", 0)) or 0)),
            "container": fmt,
            "encoding": encoding,
        }

        if not video_only:
            if combined_url is None:
                combined_url = url
            format_streams.append(item)
        else:
            adaptive_formats.append(item)

    for stream in (data.get("audioStreams") or []):
        if not isinstance(stream, dict):
            continue

        url = stream.get("url", "")
        if not url:
            continue

        fmt = str(stream.get("format", "webm")).lower()
        codec = stream.get("audioCodec") or stream.get("acodec") or ""

        adaptive_formats.append(
            {
                "url": url,
                "itag": str(stream.get("formatId", "")),
                "type": f"audio/{fmt}",
                "quality": stream.get("quality", ""),
                "qualityLabel": "",
                "fps": 0,
                "size": "",
                "bitrate": str(int(stream.get("bitrate", stream.get("tbr", 0)) or 0)),
                "container": fmt,
                "encoding": _normalize_codec(codec),
            }
        )

    if not format_streams and hls_url:
        format_streams.append(
            {
                "url": hls_url,
                "itag": "hls",
                "type": "application/vnd.apple.mpegurl",
                "quality": "HLS",
                "qualityLabel": "HLS",
                "fps": 0,
                "size": "",
                "bitrate": "0",
                "container": "m3u8",
                "encoding": "",
                "isHls": True,
            }
        )

    fallback_url = None

    for stream in video_streams:
        if not isinstance(stream, dict):
            continue
        if "360" in str(stream.get("quality", "")) and stream.get("videoOnly", True):
            fallback_url = stream.get("url")
            if fallback_url:
                break

    if not format_streams:
        if combined_url:
            pass
        elif hls_url:
            pass
        elif fallback_url:
            format_streams.append(
                {
                    "url": fallback_url,
                    "itag": "fallback",
                    "type": "video/mp4",
                    "quality": "360p",
                    "qualityLabel": "360p",
                    "fps": 30,
                    "size": "",
                    "bitrate": "0",
                    "container": "mp4",
                    "encoding": "H.264",
                }
            )

    video_urls = [item.get("url") for item in format_streams if item.get("url")]

    if not video_urls:
        video_urls = [
            item.get("url")
            for item in adaptive_formats
            if item.get("url") and "video" in str(item.get("type", ""))
        ]

    stream_urls = []

    for item in format_streams:
        stream_urls.append(
            {
                "url": item.get("url"),
                "resolution": item.get("qualityLabel"),
                "format": f"{item.get('container', 'mp4')}/mixed",
                "audioUrl": "",
            }
        )

    return {
        "streamUrls": stream_urls,
        "videoUrls": video_urls,
        "formatStreams": format_streams,
        "adaptiveFormats": adaptive_formats,
        "combined_url": combined_url,
        "hls_url": hls_url,
        "fallback_url": fallback_url,
    }


async def _fetch_invidious_streams(
    video_id: str,
    force_instance: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    start = time.monotonic()

    try:
        result = await proxy_parallel(
            "video",
            f"/api/v1/videos/{video_id}",
            override_instances=([force_instance] if force_instance else None),
            prefer_valid_stream=True,
        )

        data = result.get("data") if isinstance(result, dict) else None

        if not isinstance(data, dict):
            raise RuntimeError("invalid Invidious stream data")

        normalized = _normalize_invidious_streams(data)

        if not normalized["streamUrls"] and not normalized["videoUrls"]:
            raise RuntimeError("empty Invidious streams")

        normalized["stream_api_used"] = "invidious"

        await record_api_performance("invidious", True, time.monotonic() - start)
        return normalized

    except Exception as exc:
        logger.debug("Invidious stream error %s: %s", video_id, exc)
        await record_api_performance("invidious", False, time.monotonic() - start)
        return None


async def _fetch_piped_streams(video_id: str) -> Optional[Dict[str, Any]]:
    start = time.monotonic()

    data = await _fetch_piped_data(video_id)

    if not data:
        await record_api_performance("piped", False, time.monotonic() - start)
        return None

    result = _piped_stream_result(data)
    result["instance"] = data.get("_piped_instance", "")

    if (
        not result["streamUrls"]
        and not result["videoUrls"]
        and not result.get("hls_url")
    ):
        await record_api_performance("piped", False, time.monotonic() - start)
        return None

    result["stream_api_used"] = "piped"

    await record_api_performance("piped", True, time.monotonic() - start)
    return result


async def fetch_fastest_stream_urls(
    video_id: str,
    api: Optional[str] = None,
    force_instance: Optional[str] = None,
) -> Dict[str, Any]:
    cache_key = f"streams:{video_id}:{force_instance or ''}:{api or 'auto'}"

    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    if api == "invidious":
        result = await _fetch_invidious_streams(video_id, force_instance)
    elif api == "piped":
        result = await _fetch_piped_streams(video_id)
    elif api in ("sia", "sennin"):
        result = await _fetch_api_streams(video_id, api)
    else:
        result = await _fetch_streams_fastest(video_id, force_instance)

    if result:
        await _cache_set(cache_key, result, CACHE_CONFIG["streams"])
        return result

    return {
        "streamUrls": [],
        "videoUrls": [],
        "formatStreams": [],
        "adaptiveFormats": [],
        "stream_api_used": "unknown",
    }


async def _fetch_api_streams(video_id: str, api: str) -> Optional[Dict[str, Any]]:
    if api == "sia":
        data = await _fetch_sia_data(video_id)
        if data:
            result = _normalize_generic_api_streams(data)
            if result:
                result["stream_api_used"] = "sia"
                return result

    if api == "sennin":
        data = await _fetch_sennin_data(video_id)
        if data:
            result = _normalize_generic_api_streams(data)
            if result:
                result["stream_api_used"] = "sennin"
                return result

    return None


async def _fetch_sia_data(video_id: str) -> Optional[Dict[str, Any]]:
    try:
        client = await get_client()
        response = await client.get(
            f"{_SIA_BASE_URL}/api/video/{video_id}",
            timeout=httpx.Timeout(6.0),
            headers={"User-Agent": DEFAULT_UA},
        )
        if response.status_code != 200:
            return None
        data = response.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def _fetch_sennin_data(video_id: str) -> Optional[Dict[str, Any]]:
    try:
        client = await get_client()
        response = await client.get(
            f"{_SENNIN_BASE_URL}/api/video/{video_id}",
            timeout=httpx.Timeout(6.0),
            headers={"User-Agent": DEFAULT_UA},
        )
        if response.status_code != 200:
            return None
        data = response.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _normalize_generic_api_streams(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    candidates = []

    for key in ("formatStreams", "adaptiveFormats", "streams", "videoStreams"):
        value = data.get(key)
        if isinstance(value, list):
            candidates.extend(value)

    if not candidates:
        return None

    format_streams = []
    adaptive_formats = []

    for item in candidates:
        if not isinstance(item, dict):
            continue

        url = item.get("url") or item.get("streamUrl") or ""
        if not url:
            continue

        quality = (
            item.get("qualityLabel")
            or item.get("quality")
            or item.get("formatNote")
            or ""
        )

        ext = item.get("ext") or "mp4"
        codec = item.get("vcodec") or item.get("videoCodec") or ""

        normalized = {
            "url": url,
            "itag": str(item.get("itag", item.get("formatId", ""))),
            "type": f"video/{ext}",
            "quality": quality,
            "qualityLabel": quality,
            "fps": item.get("fps", 30) or 30,
            "size": (
                f"{item.get('width')}x{item.get('height')}"
                if item.get("width") and item.get("height")
                else ""
            ),
            "bitrate": str(item.get("bitrate", item.get("tbr", 0)) or 0),
            "container": ext,
            "encoding": _normalize_codec(codec),
        }

        if item.get("videoOnly") or item.get("type") == "video-only":
            adaptive_formats.append(normalized)
        else:
            format_streams.append(normalized)

    if not format_streams and not adaptive_formats:
        return None

    video_urls = [
        item["url"] for item in (format_streams + adaptive_formats) if item.get("url")
    ]

    stream_urls = [
        {
            "url": item["url"],
            "resolution": item.get("qualityLabel"),
            "format": f"{item.get('container', 'mp4')}/mixed",
            "audioUrl": "",
        }
        for item in format_streams
    ]

    return {
        "streamUrls": stream_urls,
        "videoUrls": video_urls,
        "formatStreams": format_streams,
        "adaptiveFormats": adaptive_formats,
    }


async def _fetch_streams_fastest(
    video_id: str,
    force_instance: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    tasks = [
        asyncio.create_task(_fetch_invidious_streams(video_id, force_instance)),
        asyncio.create_task(_fetch_piped_streams(video_id)),
    ]

    try:
        for task in asyncio.as_completed(tasks, timeout=15.0):
            try:
                result = await task

                if not result:
                    continue

                if (
                    result.get("streamUrls")
                    or result.get("videoUrls")
                    or result.get("formatStreams")
                    or result.get("adaptiveFormats")
                ):
                    return result

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("Stream task failed: %s", exc)

    except asyncio.TimeoutError:
        logger.debug("Stream fastest timeout: %s", video_id)

    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    return None


def _normalize_comment(comment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(comment, dict):
        return None

    item = dict(comment)

    author_obj = item.get("author")
    author_icon = ""

    if isinstance(author_obj, dict):
        item["author"] = author_obj.get("name", "")
        author_icon = (
            author_obj.get("avatar")
            or author_obj.get("authorIcon")
            or item.get("avatar", "")
        )
        item["authorId"] = author_obj.get("channelId", "")
    else:
        author_thumbs = item.get("authorThumbnails", [])
        if isinstance(author_thumbs, list) and author_thumbs:
            last = author_thumbs[-1]
            if isinstance(last, dict):
                author_icon = last.get("url", "")

    item["authorIcon"] = (
        author_icon or item.get("authorIcon", "") or item.get("avatar", "")
    )
    item["authorThumbnail"] = item["authorIcon"]
    item["avatar"] = item["authorIcon"]

    if not isinstance(item.get("authorThumbnails"), list):
        item["authorThumbnails"] = (
            [{"url": item["authorIcon"]}] if item["authorIcon"] else []
        )

    text = item.get("text") or item.get("content") or ""

    item["content"] = item.get("content") or text
    item["text"] = item.get("text") or text
    item["contentHtml"] = item.get("contentHtml") or text.replace("\n", "<br>")

    published = (
        item.get("publishedTime")
        or item.get("published")
        or item.get("publishedText", "")
    )

    item["publishedTime"] = published
    item["publishedText"] = published

    likes = item.get("likes")

    if isinstance(likes, dict):
        item["likeCount"] = likes.get("count", 0)
    else:
        item["likeCount"] = item.get("likeCount", likes or 0)

    return item


def normalize_sennin_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = data.get("comments", [])
    if not isinstance(comments, list):
        return []

    result = []

    for comment in comments:
        if not isinstance(comment, dict):
            continue

        author = comment.get("author", {})
        if not isinstance(author, dict):
            author = {}

        likes = comment.get("likes", {})
        if not isinstance(likes, dict):
            likes = {}

        replies = comment.get("replies", {})
        if not isinstance(replies, dict):
            replies = {}

        author_name = author.get("name") or (
            comment.get("author") if isinstance(comment.get("author"), str) else ""
        )

        author_icon = author.get("avatar") or comment.get("authorIcon", "")

        text = comment.get("text") or comment.get("content") or ""

        result.append(
            {
                "commentId": comment.get("commentId", ""),
                "author": author_name,
                "authorId": author.get("channelId") or comment.get("authorId", ""),
                "authorIcon": author_icon,
                "authorThumbnail": author_icon,
                "authorThumbnails": ([{"url": author_icon}] if author_icon else []),
                "content": text,
                "contentHtml": text.replace("\n", "<br>"),
                "publishedTime": comment.get("publishedTime", ""),
                "publishedText": comment.get("publishedTime", ""),
                "likeCount": likes.get("count", 0),
                "replyCount": replies.get("count", 0),
                "isCreator": author.get("creator", False),
                "isVerified": author.get("verified", False),
            }
        )

    return result


def process_comments(comment_data: Any) -> List[Dict[str, Any]]:
    if isinstance(comment_data, Exception):
        return []

    if not comment_data:
        return []

    if isinstance(comment_data, dict):
        if comment_data.get("success") is True and isinstance(
            comment_data.get("comments"), list
        ):
            return normalize_sennin_comments(comment_data)

        comments = comment_data.get("comments", [])

    elif isinstance(comment_data, list):
        comments = comment_data

    else:
        comments = []

    result = []

    for comment in comments:
        normalized = _normalize_comment(comment)
        if normalized:
            result.append(normalized)

    return result


async def fetch_comments(
    video_id: str,
    force_instance: Optional[str] = None,
    api: Optional[str] = None,
) -> List[Dict[str, Any]]:
    cache_key = f"comments:{video_id}:{force_instance or ''}:{api or 'auto'}"

    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    if api == "sennin":
        try:
            client = await get_client()

            response = await client.get(
                f"{_SENNIN_BASE_URL}/api/video/{video_id}",
                timeout=5.0,
                headers={"User-Agent": DEFAULT_UA},
            )

            if response.status_code == 200:
                data = response.json()
                comments = normalize_sennin_comments(data)

                if comments:
                    await _cache_set(cache_key, comments, CACHE_CONFIG["comments"])
                    return comments

        except Exception as exc:
            logger.debug("Sennin comments failed: %s", exc)

        return []

    try:
        result = await proxy_parallel(
            "comments",
            f"/api/v1/comments/{video_id}",
            override_instances=([force_instance] if force_instance else None),
        )

        data = result.get("data") if isinstance(result, dict) else result

        comments = process_comments(data)

        if comments:
            await _cache_set(cache_key, comments, CACHE_CONFIG["comments"])
            return comments

    except Exception as exc:
        logger.debug("Invidious comments failed: %s", exc)

    return []


async def _fetch_playlist(
    playlist_id: Optional[str],
    force_instance: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not playlist_id:
        return None

    try:
        result = await proxy_parallel(
            "playlist",
            f"/api/v1/playlists/{playlist_id}",
            override_instances=([force_instance] if force_instance else None),
        )

        data = result.get("data") if isinstance(result, dict) else None

        if isinstance(data, dict):
            return data

    except Exception as exc:
        logger.debug("Playlist error: %s", exc)

    return None


def _is_piped_proxy_url_allowed(url: str) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()

    allowed_exact = {
        "pipedapi.wireway.ch",
        "api.piped.private.coffee",
        "pipedapi.winscloud.net",
        "player.odycdn.com",
        "googlevideo.com",
    }

    if hostname in allowed_exact:
        return True

    allowed_suffixes = (
        ".piped.private.coffee",
        ".googlevideo.com",
        ".youtube.com",
        ".odycdn.com",
        ".ggpht.com",
    )

    return hostname.endswith(allowed_suffixes)


def _rewrite_hls_manifest(body: str, base_url: str) -> str:
    lines = body.splitlines()
    output = []

    for line in lines:
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            if stripped.startswith("#EXT-X-KEY") or stripped.startswith("#EXT-X-MEDIA"):

                def replace_uri(match):
                    original = match.group(1)
                    absolute = urljoin(base_url, original)
                    return 'URI="/proxy/piped-stream?url=' + quote(absolute) + '"'

                line = re.sub(r'URI="([^"]+)"', replace_uri, line)

            output.append(line)
            continue

        absolute = urljoin(base_url, stripped)
        output.append("/proxy/piped-stream?url=" + quote(absolute))

    return "\n".join(output) + "\n"


@router.get("/proxy/piped-stream")
async def proxy_piped_stream(url: str, request: Request):
    if not _is_piped_proxy_url_allowed(url):
        return JSONResponse({"error": "不正なURL"}, status_code=400)

    request_headers = {}

    range_header = request.headers.get("range")
    if range_header:
        request_headers["Range"] = range_header

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=5.0),
        follow_redirects=True,
    )

    is_manifest = ".m3u8" in url or "/manifest/hls_" in url

    if is_manifest:
        try:
            response = await client.get(url, headers=request_headers)

            content_type = response.headers.get(
                "content-type", "application/vnd.apple.mpegurl"
            )

            rewritten = _rewrite_hls_manifest(response.text, str(response.url))

            return PlainTextResponse(
                rewritten,
                status_code=response.status_code,
                media_type=content_type,
                headers={"cache-control": "no-cache"},
            )

        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=502)

        finally:
            await client.aclose()

    try:
        request_obj = client.build_request("GET", url, headers=request_headers)
        response = await client.send(request_obj, stream=True)

    except Exception as exc:
        await client.aclose()
        return JSONResponse({"error": str(exc)}, status_code=502)

    forwarded = {}

    for header in (
        "content-type",
        "content-length",
        "content-range",
        "accept-ranges",
        "cache-control",
    ):
        if header in response.headers:
            forwarded[header] = response.headers[header]

    forwarded.setdefault("accept-ranges", "bytes")

    async def generate():
        try:
            async for chunk in response.aiter_bytes(65536):
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()

    return StreamingResponse(
        generate(),
        status_code=response.status_code,
        headers=forwarded,
    )


@router.get("/api/pipedstream/{video_id}")
async def api_piped_stream(
    video_id: str,
    want_proxy: bool = True,
    request: Request = None,
):
    result = await _fetch_piped_streams(video_id)

    if not result:
        return JSONResponse(
            {"error": "Piped APIからストリームURLを取得できませんでした"},
            status_code=502,
        )

    combined = result.get("combined_url")
    hls = result.get("hls_url")
    fallback = result.get("fallback_url")

    stream_url = combined or hls or fallback

    if not stream_url:
        return JSONResponse(
            {"error": "ストリームURLが見つかりませんでした"},
            status_code=502,
        )

    stream_type = "combined" if combined else ("hls" if hls else "video_only")

    instance = result.get("instance") or ""

    if want_proxy:
        return JSONResponse(
            {
                "mode": "proxy",
                "proxy_url": "/proxy/piped-stream?url=" + quote(stream_url),
                "instance": instance,
                "remaining": -1,
                "stream_type": stream_type,
            }
        )

    return JSONResponse(
        {
            "mode": "direct",
            "url": stream_url,
            "instance": instance,
            "stream_type": stream_type,
        }
    )


def _nocookie_url(video_id: str) -> str:
    return NOCOOKIE_EMBED_BASE + quote(video_id, safe="")


async def _wait_for_results(
    tasks: List["asyncio.Task"],
    timeout: float = PAGE_WAIT_SECONDS,
) -> List[Any]:
    """
    最大 timeout 秒だけ待ち、終わったタスクの結果を返す(未完了・失敗は None)。
    未完了のタスクはキャンセルせずバックグラウンドで続行させ、キャッシュを温める。
    nocookie の埋め込みは動画IDだけで表示できるので、APIの完了は待ちきらない。
    """
    _, pending = await asyncio.wait(tasks, timeout=timeout)

    for task in pending:
        task.add_done_callback(
            lambda t: t.cancelled() or t.exception()  # 例外を回収して警告を防ぐ
        )

    results: List[Any] = []
    for task in tasks:
        if task.done() and not task.cancelled() and task.exception() is None:
            results.append(task.result())
        else:
            results.append(None)
    return results


@router.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts_player(
    request: Request,
    v: str,
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None, alias="info_api"),
    stream_api: Optional[str] = Query(None, alias="stream_api"),
    api: Optional[str] = Query(None),
):
    resolved_info_api = info_api or api or None
    resolved_stream_api = stream_api or api or None

    try:
        video_task = asyncio.create_task(
            fetch_video_info(v, force_instance=force_instance, api=resolved_info_api)
        )
        stream_task = asyncio.create_task(
            fetch_fastest_stream_urls(
                v, api=resolved_stream_api, force_instance=force_instance
            )
        )
        comment_task = asyncio.create_task(
            fetch_comments(v, force_instance=force_instance, api=resolved_info_api)
        )

        video_data, stream_data, comment_data = await _wait_for_results(
            [video_task, stream_task, comment_task]
        )

        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_data if isinstance(stream_data, dict) else {}

        video_urls = s_data.get("videoUrls", [])

        if not video_urls and v_data:
            fallback = _normalize_invidious_streams(v_data)
            video_urls = fallback.get("videoUrls", [])

        comments = process_comments(comment_data)

        info_api_used = v_data.get("api_used") or v_data.get("_source") or "unknown"
        stream_api_used = s_data.get("stream_api_used") or "unknown"

        return templates.TemplateResponse(
            "short.html",
            {
                "request": request,
                "videoid": v,
                "nocookie_url": _nocookie_url(v),
                "video_title": v_data.get("title", ""),
                "videourls": video_urls,
                "author": v_data.get("author", ""),
                "view_count": v_data.get("viewCount", 0),
                "like_count": v_data.get("likeCount", 0),
                "description": (
                    v_data.get("descriptionHtml")
                    or (v_data.get("description", "") or "").replace("\n", "<br>")
                ),
                "comments": comments,
                "info_api_used": info_api_used,
                "stream_api_used": stream_api_used,
                "api_used": info_api_used,
            },
        )

    except httpx.TimeoutException:
        logger.error("Timeout in shorts_player: %s", v)
        return templates.TemplateResponse("apitimeout.html", {"request": request})

    except Exception as exc:
        logger.error("Error in shorts_player %s: %s", v, exc)

        try:
            instances = await get_video_back_instances()
        except Exception:
            instances = []

        return templates.TemplateResponse(
            "apiallerror.html",
            {"request": request, "instances": instances},
        )


@router.get("/watch", response_class=HTMLResponse)
async def watch(
    request: Request,
    v: str = Query(...),
    playlist_id: Optional[str] = Query(None, alias="list"),
    force_instance: Optional[str] = Query(None),
    info_api: Optional[str] = Query(None, alias="info_api"),
    stream_api: Optional[str] = Query(None, alias="stream_api"),
    api: Optional[str] = Query(None),
):
    resolved_info_api = info_api or api or None
    resolved_stream_api = stream_api or api or None

    try:
        video_task = asyncio.create_task(
            fetch_video_info(v, force_instance=force_instance, api=resolved_info_api)
        )
        stream_task = asyncio.create_task(
            fetch_fastest_stream_urls(
                v, api=resolved_stream_api, force_instance=force_instance
            )
        )
        comment_task = asyncio.create_task(
            fetch_comments(v, force_instance=force_instance, api=resolved_info_api)
        )
        playlist_task = asyncio.create_task(
            _fetch_playlist(playlist_id, force_instance)
        )

        # nocookie 埋め込みで再生できるので、最大 PAGE_WAIT_SECONDS 待ったら
        # 取れている情報だけでページを表示する
        video_data, stream_data, comment_data, playlist_data = await _wait_for_results(
            [video_task, stream_task, comment_task, playlist_task]
        )

        v_data = video_data if isinstance(video_data, dict) else {}
        s_data = stream_data if isinstance(stream_data, dict) else {}
        p_data = playlist_data if isinstance(playlist_data, dict) else {}

        playlist_videos = []

        for item in (p_data.get("videos", []) or []):
            if not isinstance(item, dict):
                continue
            playlist_videos.append(
                {
                    "videoId": item.get("videoId"),
                    "title": item.get("title"),
                    "author": item.get("author"),
                }
            )

        stream_urls = s_data.get("streamUrls", [])
        video_urls = s_data.get("videoUrls", [])

        if not stream_urls and v_data:
            fallback = _normalize_invidious_streams(v_data)
            stream_urls = fallback.get("streamUrls", [])
            video_urls = fallback.get("videoUrls", [])

        recommended = []

        for item in (v_data.get("recommendedVideos", []) or []):
            if not isinstance(item, dict):
                continue
            recommended.append(
                {
                    "video_id": item.get("video_id") or item.get("videoId"),
                    "title": item.get("title"),
                    "author": item.get("author"),
                    "view_count_text": (
                        item.get("view_count_text") or item.get("viewCountText")
                    ),
                    "thumbnail": item.get("thumbnail", ""),
                }
            )

        author_icon = v_data.get("authorIcon") or ""

        if not author_icon:
            thumbnails = v_data.get("authorThumbnails", [])
            if isinstance(thumbnails, list) and thumbnails:
                last = thumbnails[-1]
                if isinstance(last, dict):
                    author_icon = last.get("url", "")

        formatted_comments = process_comments(comment_data)

        info_api_used = v_data.get("api_used") or v_data.get("_source") or "unknown"
        stream_api_used = s_data.get("stream_api_used") or "unknown"

        return templates.TemplateResponse(
            "watch.html",
            {
                "request": request,
                "videoid": v,
                "nocookie_url": _nocookie_url(v),
                "video_title": v_data.get("title") or s_data.get("title", ""),
                "videourls": video_urls,
                "streamUrls": stream_urls,
                "author": v_data.get("author") or s_data.get("author", ""),
                "author_id": v_data.get("authorId") or s_data.get("authorId", ""),
                "author_icon": author_icon,
                "subscribers_count": v_data.get("subCountText", "非公開"),
                "view_count": v_data.get("viewCount", s_data.get("viewCount", 0)),
                "like_count": v_data.get("likeCount", 0),
                "description": (
                    v_data.get("descriptionHtml")
                    or (v_data.get("description", "") or "").replace("\n", "<br>")
                ),
                "published": v_data.get("publishedText", ""),
                "comments": formatted_comments,
                "recommended_videos": recommended,
                "playlist_videos": playlist_videos,
                "playlist_id": playlist_id,
                "info_api_used": info_api_used,
                "stream_api_used": stream_api_used,
                "api_used": info_api_used,
            },
        )

    except httpx.TimeoutException:
        logger.error("Timeout in watch: %s", v)
        return templates.TemplateResponse("apitimeout.html", {"request": request})

    except Exception as exc:
        logger.error("Error in watch %s: %s", v, exc)

        try:
            instances = await get_video_back_instances()
        except Exception:
            instances = []

        return templates.TemplateResponse(
            "apiallerror.html",
            {"request": request, "instances": instances},
        )
