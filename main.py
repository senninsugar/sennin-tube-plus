from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import httpx
import asyncio
import json
import random
import time
from datetime import datetime
import base64

app = FastAPI()

templates = Jinja2Templates(directory="templates")
templates.env.add_extension('jinja2.ext.do')

FALLBACK_INVIDIOUS_INSTANCES = [
    "https://invidious.ritoge.com",
    "https://yt.omada.cafe",
    "https://invidious.darkness.services",
    "https://invidious.f5.si",
    "https://invidious.ducks.party",
    "https://y.com.sb",
    "https://super8.absturztau.be",
    "https://inv.zoomerville.com",
    "https://invidious.nerdvpn.de",
    "https://inv.thepixora.com"
]

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net"
]

SENNIN_API_BASE = "https://discerning-adventure-production-ebfc.up.railway.app"

RAPID_API_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"

_ENCRYPTED_KEYS = [
    "ZTYxNTE4MzAzNG1zaDJkZmRhMzFhNDdhNmYxMnAxZmE2Y2Nqc241OWExYTVlMDY0MTU=",
    "NjllMjk5OWE3OW1zaGNiNjU3MTg0YmE2NzMxY3AxNmY2ODRqc24zMjA1NGEwNzBiYTU="
]

def _get_rapid_api_keys():
    return [base64.b64decode(k.encode('utf-8')).decode('utf-8') for k in _ENCRYPTED_KEYS]

INVIDIOUS_VIDEO_LIST_URL = "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/video.json"
INVIDIOUS_SEARCH_LIST_URL = "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/search.json"

limits = httpx.Limits(max_connections=500, max_keepalive_connections=200)
client_session = httpx.AsyncClient(timeout=4.5, limits=limits, follow_redirects=True)
no_redirect_client = httpx.AsyncClient(timeout=3.5, limits=limits, follow_redirects=False)

_CACHE = {}
_INFLIGHT = {}
_CACHE_LOCK = asyncio.Lock()

def get_cache(key: str):
    now = time.time()
    hit = _CACHE.get(key)
    if hit and hit["exp"] > now:
        return hit["val"]
    return None

def set_cache(key: str, val: any, ttl: float = 180.0):
    now = time.time()
    if len(_CACHE) >= 1000:
        oldest = min(_CACHE, key=lambda k: _CACHE[k]["exp"])
        _CACHE.pop(oldest, None)
    _CACHE[key] = {"val": val, "exp": now + ttl}

async def fetch_with_inflight(key: str, fetch_func, ttl: float = 180.0):
    cached = get_cache(key)
    if cached is not None:
        return cached

    loop = asyncio.get_event_loop()
    async with _CACHE_LOCK:
        cached = get_cache(key)
        if cached is not None:
            return cached
        if key in _INFLIGHT:
            fut = _INFLIGHT[key]
            return await asyncio.shield(fut)
        
        fut = loop.create_future()
        _INFLIGHT[key] = fut

    try:
        res = await fetch_func()
        if res is not None:
            set_cache(key, res, ttl=ttl)
        if not fut.done():
            fut.set_result(res)
        return res
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
            try:
                fut.exception()
            except Exception:
                pass
        raise e
    finally:
        async with _CACHE_LOCK:
            _INFLIGHT.pop(key, None)


async def get_invidious_instances_from_url(list_url: str) -> list:
    cache_key = f"inv_instances_list:{list_url}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    try:
        resp = await client_session.get(list_url, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            instances = []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "instance" in item:
                        inst = item["instance"].strip()
                        if inst:
                            instances.append(inst)
                    elif isinstance(item, str):
                        instances.append(item.strip())
            if instances:
                set_cache(cache_key, instances, ttl=600.0)
                return instances
    except Exception:
        pass

    return FALLBACK_INVIDIOUS_INSTANCES


async def get_fastest_invidious_instance(list_url: str = INVIDIOUS_VIDEO_LIST_URL) -> str:
    cache_key = f"fastest_inv_instance:{list_url}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    base_instances = await get_invidious_instances_from_url(list_url)
    target_instances = base_instances[:10]

    async def ping_instance(instance):
        start = time.time()
        try:
            url = f"{instance.rstrip('/')}/api/v1/stats"
            resp = await client_session.get(url, timeout=2.0)
            if resp.status_code == 200:
                elapsed = time.time() - start
                return instance, elapsed
        except Exception:
            pass
        return instance, float('inf')

    tasks = [ping_instance(inst) for inst in target_instances]
    results = await asyncio.gather(*tasks)
    
    valid_results = [r for r in results if r[1] < float('inf')]
    if valid_results:
        fastest_instance = min(valid_results, key=lambda x: x[1])[0]
        set_cache(cache_key, fastest_instance, ttl=300.0)
        return fastest_instance

    return base_instances[0] if base_instances else FALLBACK_INVIDIOUS_INSTANCES[0]


async def fetch_invidious(endpoint: str, params: dict = None, force_instance: str = None, list_type: str = "video"):
    param_str = json.dumps(params, sort_keys=True) if params else ""
    cache_key = f"inv:{endpoint}:{param_str}:{force_instance or ''}:{list_type}"

    async def _do_fetch():
        list_url = INVIDIOUS_SEARCH_LIST_URL if list_type == "search" else INVIDIOUS_VIDEO_LIST_URL
        base_instances = await get_invidious_instances_from_url(list_url)

        if force_instance:
            instances = [force_instance] + [i for i in base_instances if i != force_instance]
            last_error = None
            for instance in instances:
                try:
                    url = f"{instance.rstrip('/')}/api/v1{endpoint}"
                    response = await client_session.get(url, params=params, timeout=3.0)
                    response.raise_for_status()
                    return response.json()
                except Exception as e:
                    last_error = e
                    continue
            raise last_error if last_error else Exception("All Invidious instances failed")
        else:
            fastest = await get_fastest_invidious_instance(list_url)
            instances = [fastest] + [i for i in base_instances if i != fastest]
            target_instances = instances[:5]
            
            async def task(instance):
                url = f"{instance.rstrip('/')}/api/v1{endpoint}"
                resp = await client_session.get(url, params=params, timeout=2.8)
                resp.raise_for_status()
                return resp.json()

            tasks = [asyncio.create_task(task(inst)) for inst in target_instances]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            res = None
            for t in done:
                try:
                    res = t.result()
                    if res is not None:
                        break
                except Exception:
                    continue
            
            for t in pending:
                t.cancel()
            
            if res is not None:
                return res

            remaining = [i for i in instances if i not in target_instances]
            last_err = None
            for inst in remaining:
                try:
                    url = f"{inst.rstrip('/')}/api/v1{endpoint}"
                    response = await client_session.get(url, params=params, timeout=2.5)
                    response.raise_for_status()
                    return response.json()
                except Exception as e:
                    last_err = e
                    continue
            raise last_err if last_err else Exception("All Invidious instances failed")

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=180.0)


# ===== Sennin API (Node.js Express API) =====
async def fetch_sennin_comments(v: str, sort: str = "top"):
    """Sennin APIからコメント取得"""
    try:
        url = f"{SENNIN_API_BASE}/api/comments"
        params = {"videoId": v, "sort": sort}
        resp = await client_session.get(url, params=params, timeout=4.0)
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get("success") is True:
                return data
    except Exception:
        pass
    raise Exception("Sennin comments failed")


async def fetch_sennin_video_info(v: str):
    """Sennin APIからビデオ情報取得"""
    try:
        url = f"{SENNIN_API_BASE}/api/video/{v}"
        resp = await client_session.get(url, timeout=4.0)
        if resp.status_code == 200:
            data = resp.json()
            if data and not data.get("unavailable"):
                return normalize_sennin_video_info(data)
    except Exception:
        pass
    raise Exception("Sennin video info failed")


def normalize_sennin_video_info(sennin_data: dict) -> dict:
    """
    Sennin API /api/video/:id のレスポンスを標準フォーマットに正規化
    """
    if not sennin_data or not isinstance(sennin_data, dict):
        return {}

    author_info = sennin_data.get("author", {}) if isinstance(sennin_data.get("author"), dict) else {}
    author_name = author_info.get("name") or ""
    author_id = author_info.get("id") or ""
    author_icon = author_info.get("thumbnail") or ""
    sub_count = author_info.get("subscribers") or "非公開"

    desc_obj = sennin_data.get("description", {})
    if isinstance(desc_obj, dict):
        desc_html = desc_obj.get("formatted") or (desc_obj.get("text", "").replace("\n", "<br>"))
        desc_text = desc_obj.get("text", "")
    else:
        desc_text = str(desc_obj or "")
        desc_html = desc_text.replace("\n", "<br>")

    rel_data = sennin_data.get("Related-videos", {})
    raw_rel = rel_data.get("relatedVideos", []) if isinstance(rel_data, dict) else []

    recommended = []
    for item in raw_rel:
        if not isinstance(item, dict):
            continue
        
        thumb_url = item.get("thumbnail") or ""
        if not thumb_url and isinstance(item.get("thumbnails"), list) and len(item["thumbnails"]) > 0:
            thumb_url = item["thumbnails"][0].get("url", "")

        recommended.append({
            "video_id": item.get("videoId") or item.get("id"),
            "title": item.get("title"),
            "author": item.get("channelName") or item.get("author"),
            "view_count_text": item.get("viewCountText"),
            "thumbnail": thumb_url
        })

    return {
        "title": sennin_data.get("title", ""),
        "author": author_name,
        "authorId": author_id,
        "authorIcon": author_icon,
        "subCountText": sub_count,
        "viewCount": sennin_data.get("views") or sennin_data.get("extended_stats", {}).get("views_original", 0),
        "likeCount": sennin_data.get("likes", 0),
        "description": desc_text,
        "descriptionHtml": desc_html,
        "recommendedVideos": recommended,
        "thumbnail": sennin_data.get("thumbnail", "")
    }


def normalize_sennin_comments(sennin_data: dict) -> list:
    """
    Sennin API /api/comments レスポンス形式を標準フォーマットに正規化
    """
    if not sennin_data or not isinstance(sennin_data, dict):
        return []
    
    comments = sennin_data.get("comments", [])
    if not isinstance(comments, list):
        return []
    
    processed = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        
        author_info = c.get("author", {}) if isinstance(c.get("author"), dict) else {}
        likes_info = c.get("likes", {}) if isinstance(c.get("likes"), dict) else {}
        replies_info = c.get("replies", {}) if isinstance(c.get("replies"), dict) else {}

        author_name = author_info.get("name") or (c.get("author") if isinstance(c.get("author"), str) else "")
        author_icon = author_info.get("avatar") or c.get("authorIcon") or ""
        author_id = author_info.get("channelId") or c.get("authorId") or ""

        text_content = c.get("text") or c.get("content") or ""

        processed.append({
            "commentId": c.get("commentId", ""),
            "author": author_name,
            "authorId": author_id,
            "authorIcon": author_icon,
            "authorThumbnail": author_icon,
            "authorThumbnails": [{"url": author_icon}] if author_icon else [],
            "content": text_content,
            "contentHtml": text_content.replace("\n", "<br>"),
            "publishedTime": c.get("publishedTime", ""),
            "publishedText": c.get("publishedTime", ""),
            "likeCount": likes_info.get("count") if isinstance(likes_info, dict) else c.get("likes", 0),
            "replyCount": replies_info.get("count") if isinstance(replies_info, dict) else c.get("replies", 0),
            "isCreator": author_info.get("creator", False),
            "isVerified": author_info.get("verified", False),
        })
    
    return processed


async def fetch_sia_video(v: str):
    try:
        url = f"https://siatube.com/api/video/{v}"
        resp = await client_session.get(url, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()

            author_info = data.get("author", {}) if isinstance(data.get("author"), dict) else {}
            author_name = author_info.get("name") or data.get("uploader") or ""
            author_id = author_info.get("id", "")
            author_icon = author_info.get("thumbnail", "")
            sub_count = author_info.get("subscribers", "非公開")

            if not author_name:
                raise Exception("Sia author_name is empty")

            desc_obj = data.get("description", {})
            if isinstance(desc_obj, dict):
                desc_text = desc_obj.get("text", "")
            else:
                desc_text = str(desc_obj or "")
            desc_html = desc_text.replace("\n", "<br>")

            rel_data = data.get("Related-videos", {}) or data.get("relatedVideos", {})
            raw_rel = rel_data.get("relatedVideos", []) if isinstance(rel_data, dict) else (rel_data if isinstance(rel_data, list) else [])
            
            recommended = []
            for item in raw_rel:
                if not isinstance(item, dict):
                    continue
                thumbs = item.get("thumbnails", [])
                thumb_url = thumbs[0].get("url", "") if isinstance(thumbs, list) and thumbs else ""
                
                recommended.append({
                    "video_id": item.get("videoId") or item.get("id"),
                    "title": item.get("title"),
                    "author": item.get("channelName") or item.get("author"),
                    "view_count_text": item.get("viewCountText"),
                    "thumbnail": thumb_url
                })

            return {
                "title": data.get("title", ""),
                "author": author_name,
                "authorId": author_id,
                "authorIcon": author_icon,
                "subCountText": sub_count,
                "viewCount": data.get("views", 0),
                "likeCount": data.get("likes", 0),
                "descriptionHtml": desc_html,
                "recommendedVideos": recommended,
                "thumbnail": data.get("thumbnail", "")
            }
    except Exception:
        pass
    raise Exception("Sia video info failed")

async def fetch_video_info(v: str, force_instance: str = None, api: str = None):
    cache_key = f"video_info:{v}:{force_instance or ''}:{api or ''}"

    async def _do_fetch():
        if api == "invidious":
            return await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")
        elif api == "sia":
            try:
                return await fetch_sia_video(v)
            except Exception:
                return await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")
        elif api == "piped":
            piped_res = await fetch_piped_stream(v)
            return piped_res
        elif api == "sennin":
            try:
                return await fetch_sennin_video_info(v)
            except Exception:
                return await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")

        if not force_instance:
            # デフォルト: Sennin -> Sia -> Invidiousの順で試行
            try:
                return await fetch_sennin_video_info(v)
            except Exception:
                pass
            try:
                return await fetch_sia_video(v)
            except Exception:
                pass
        return await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=180.0)

async def fetch_sia_stream(v: str):
    try:
        url = f"https://siatube.com/api/stream/{v}"
        resp = await client_session.get(url, timeout=2.5)
        if resp.status_code == 200:
            data = resp.json()
            stream_urls = []
            video_urls = []

            muxed = data.get("muxed", []) or data.get("formats", []) or []
            if isinstance(muxed, list):
                for item in muxed:
                    u = item.get("url")
                    if u:
                        video_urls.append(u)
                        stream_urls.append({
                            "url": u,
                            "resolution": item.get("quality", item.get("qualityLabel", "Auto")),
                            "format": "mp4/mixed",
                            "audioUrl": ""
                        })

            hls_url = data.get("hls") or data.get("m3u8") or data.get("manifestUrl")
            if hls_url:
                if hls_url not in video_urls:
                    video_urls.append(hls_url)
                stream_urls.append({
                    "url": hls_url,
                    "resolution": "HLS/Live",
                    "format": "application/x-mpegURL",
                    "audioUrl": ""
                })

            audio_only = data.get("audioOnly", []) or []
            audio_url = audio_only[0].get("url") if isinstance(audio_only, list) and len(audio_only) > 0 else None

            video_only = data.get("videoOnly", []) or []
            if isinstance(video_only, list):
                for item in video_only:
                    u = item.get("url")
                    if u:
                        stream_urls.append({
                            "url": u,
                            "resolution": item.get("quality", item.get("qualityLabel", "1080p")),
                            "format": "webm/videoOnly",
                            "audioUrl": audio_url or ""
                        })

            if not video_urls and stream_urls:
                video_urls = [s["url"] for s in stream_urls if s.get("url")]

            if video_urls:
                return {
                    "streamUrls": stream_urls,
                    "videoUrls": video_urls
                }
    except Exception:
        pass
    raise Exception("Sia failed")

async def fetch_piped_stream(v: str):
    instances = list(PIPED_INSTANCES)
    random.shuffle(instances)
    for instance in instances:
        try:
            url = f"{instance.rstrip('/')}/streams/{v}"
            resp = await client_session.get(url, timeout=2.5)
            if resp.status_code == 200:
                data = resp.json()
                stream_urls = []
                video_urls = []
                audio_url = None

                for item in data.get("audioStreams", []):
                    if item.get("mimeType", "").startswith("audio"):
                        audio_url = item.get("url")
                        break

                for item in data.get("videoStreams", []):
                    url_str = item.get("url")
                    quality = item.get("quality", "")
                    if item.get("videoOnly", False):
                        stream_urls.append({
                            "url": url_str,
                            "resolution": quality,
                            "format": "webm/videoOnly",
                            "audioUrl": audio_url
                        })
                    else:
                        stream_urls.append({
                            "url": url_str,
                            "resolution": quality,
                            "format": "mp4/mixed",
                            "audioUrl": ""
                        })
                        video_urls.append(url_str)

                if not video_urls:
                    video_urls = [s["url"] for s in stream_urls if s.get("url")]

                return {
                    "streamUrls": stream_urls,
                    "videoUrls": video_urls,
                    "title": data.get("title"),
                    "author": data.get("uploader"),
                    "authorId": data.get("uploaderUrl", "").replace("/channel/", ""),
                    "descriptionHtml": data.get("description", "").replace("\n", "<br>"),
                    "viewCount": data.get("views", 0),
                    "likeCount": data.get("likes", 0)
                }
        except Exception:
            continue
    raise Exception("Piped failed")

async def fetch_zernio_stream(v: str):
    try:
        target_url = f"https://www.youtube.com/watch?v={v}"
        url = f"https://getlate.dev/api/tools/youtube-live-downloader?url={target_url}"
        resp = await no_redirect_client.get(url, timeout=3.0)
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location") or resp.headers.get("Location")
            if location:
                return {
                    "streamUrls": [{
                        "url": location,
                        "resolution": "Live/Auto",
                        "format": "mp4/mixed",
                        "audioUrl": ""
                    }],
                    "videoUrls": [location]
                }
    except Exception:
        pass
    raise Exception("Zernio failed")

async def fetch_rapidapi_stream(v: str):
    keys = _get_rapid_api_keys()
    random.shuffle(keys)
    
    for key in keys:
        try:
            url = f"https://{RAPID_API_HOST}/dl?id={v}"
            headers = {
                "X-RapidAPI-Key": key,
                "X-RapidAPI-Host": RAPID_API_HOST
            }
            resp = await client_session.get(url, headers=headers, timeout=2.5)
            if resp.status_code == 200:
                data = resp.json()
                formats = data.get("formats", [])
                stream_urls = []
                video_urls = []
                for f in formats:
                    u = f.get("url")
                    if u:
                        video_urls.append(u)
                        stream_urls.append({
                            "url": u,
                            "resolution": f.get("qualityLabel", "720p"),
                            "format": "mp4/mixed",
                            "audioUrl": ""
                        })
                if video_urls:
                    return {
                        "streamUrls": stream_urls,
                        "videoUrls": video_urls,
                        "title": data.get("title")
                    }
        except Exception:
            continue
    raise Exception("RapidAPI failed")

async def fetch_fastest_stream_urls(v: str, api: str = None, force_instance: str = None):
    cache_key = f"fastest_stream:{v}:{api or ''}:{force_instance or ''}"

    async def _do_fetch():
        if api == "sia":
            try:
                return await fetch_sia_stream(v)
            except Exception:
                v_data = await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")
                return extract_invidious_streams(v_data)
        elif api == "piped":
            return await fetch_piped_stream(v)
        elif api == "rapidapi":
            return await fetch_rapidapi_stream(v)
        elif api == "zernio":
            return await fetch_zernio_stream(v)
        elif api == "invidious":
            v_data = await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")
            return extract_invidious_streams(v_data)
        elif api == "sennin":
            # Sennin形式では動画URL情報がないため、Sia→Invidiousで取得
            try:
                return await fetch_sia_stream(v)
            except Exception:
                v_data = await fetch_invidious(f"/videos/{v}", force_instance=force_instance, list_type="video")
                return extract_invidious_streams(v_data)

        tasks = [
            asyncio.create_task(fetch_sia_stream(v)),
            asyncio.create_task(fetch_piped_stream(v)),
            asyncio.create_task(fetch_rapidapi_stream(v)),
            asyncio.create_task(fetch_zernio_stream(v))
        ]

        for completed in asyncio.as_completed(tasks):
            try:
                res = await completed
                if res and res.get("videoUrls"):
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    return res
            except Exception:
                continue

        return None

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=120.0)

async def fetch_sia_comments(v: str):
    try:
        url = f"https://siatube.com/api/comments?videoId={v}"
        resp = await client_session.get(url, timeout=3.5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and "comments" in data:
                return data
    except Exception:
        pass
    raise Exception("Sia comments failed")

async def fetch_comments(v: str, force_instance: str = None, api: str = None):
    cache_key = f"comments:{v}:{force_instance or ''}:{api or ''}"

    async def _do_fetch():
        if api == "sia":
            try:
                return await fetch_sia_comments(v)
            except Exception:
                return await fetch_invidious(f"/comments/{v}", force_instance=force_instance, list_type="video")
        elif api == "invidious":
            return await fetch_invidious(f"/comments/{v}", force_instance=force_instance, list_type="video")
        elif api == "sennin":
            try:
                return await fetch_sennin_comments(v)
            except Exception:
                return await fetch_invidious(f"/comments/{v}", force_instance=force_instance, list_type="video")

        # デフォルト: Sennin -> Sia -> Invidiousの順
        sennin_task = asyncio.create_task(fetch_sennin_comments(v))
        
        done, pending = await asyncio.wait([sennin_task], timeout=2.5)
        
        if sennin_task in done:
            try:
                res = sennin_task.result()
                if res is not None:
                    return res
            except Exception:
                pass

        sia_task = asyncio.create_task(fetch_sia_comments(v))
        done_sia, pending_sia = await asyncio.wait([sia_task], timeout=2.5)
        
        if sia_task in done_sia:
            try:
                res = sia_task.result()
                if res is not None:
                    return res
            except Exception:
                pass

        invidious_task = asyncio.create_task(fetch_invidious(f"/comments/{v}", force_instance=force_instance, list_type="video"))
        
        remaining_tasks = [t for t in [sennin_task, sia_task, invidious_task] if not t.done()]
        
        while remaining_tasks:
            done_batch, pending_batch = await asyncio.wait(remaining_tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done_batch:
                try:
                    res = t.result()
                    if res is not None:
                        for p in pending_batch:
                            p.cancel()
                        return res
                except Exception:
                    continue
            remaining_tasks = list(pending_batch)

        return None

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=180.0)

async def fetch_sia_channel(ucid: str):
    try:
        url = f"https://siatube.com/api/channel/{ucid}"
        resp = await client_session.get(url, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and ("author" in data or "title" in data or "videos" in data or "name" in data):
                return data
    except Exception:
        pass
    raise Exception("Sia channel failed")

def extract_invidious_streams(v_data: dict):
    if not v_data:
        return {"streamUrls": [], "videoUrls": []}

    adaptive = v_data.get("adaptiveFormats", [])
    audio_url = None
    for f in adaptive:
        if "audio" in f.get("type", ""):
            if f.get("language") == "ja":
                audio_url = f.get("url")
                break
    if not audio_url:
        for f in adaptive:
            if "audio" in f.get("type", ""):
                audio_url = f.get("url")
                break

    format_streams = v_data.get("formatStreams", [])
    stream_urls = [{
        "url": fmt.get("url"),
        "resolution": fmt.get("qualityLabel"),
        "format": "mp4/mixed",
        "audioUrl": ""
    } for fmt in format_streams]
    
    stream_urls.extend({
        "url": fmt.get("url"),
        "resolution": fmt.get("qualityLabel"),
        "format": "webm/videoOnly",
        "audioUrl": audio_url
    } for fmt in adaptive if "video" in fmt.get("type", "") and "webm" in fmt.get("container", ""))

    video_urls = [fmt.get("url") for fmt in format_streams] or \
                 [fmt.get("url") for fmt in adaptive if "video" in fmt.get("type", "")]

    return {
        "streamUrls": stream_urls,
        "videoUrls": video_urls
    }

def process_comments(comment_data):
    if isinstance(comment_data, Exception) or not comment_data:
        return []
    
    # Sennin形式の場合
    if isinstance(comment_data, dict) and comment_data.get("success") is True and isinstance(comment_data.get("comments"), list):
        return normalize_sennin_comments(comment_data)
    
    # Invidious/Sia形式の場合
    comments = comment_data.get("comments", []) if isinstance(comment_data, dict) else (comment_data if isinstance(comment_data, list) else [])
    processed = []
    
    for c in comments:
        if not isinstance(c, dict):
            continue
        item = dict(c)

        author_obj = item.get("author")
        if isinstance(author_obj, dict):
            item["author"] = author_obj.get("name", "")
            item["authorIcon"] = author_obj.get("avatar") or author_obj.get("authorIcon") or item.get("avatar", "")
            item["authorThumbnail"] = item["authorIcon"]
            item["authorId"] = author_obj.get("channelId", "")
        else:
            author_thumbs = item.get("authorThumbnails", [])
            if author_thumbs and isinstance(author_thumbs, list):
                item["authorIcon"] = author_thumbs[-1].get("url", "")
                item["authorThumbnail"] = item["authorIcon"]
            else:
                item["authorIcon"] = item.get("authorIcon") or item.get("avatar", "")
                item["authorThumbnail"] = item["authorIcon"]

        if "avatar" in item and item["avatar"]:
            item["authorIcon"] = item["avatar"]
            item["authorThumbnail"] = item["avatar"]
        elif isinstance(author_obj, dict) and author_obj.get("avatar"):
            item["authorIcon"] = author_obj.get("avatar")
            item["avatar"] = author_obj.get("avatar")
            item["authorThumbnail"] = author_obj.get("avatar")
        elif "authorIcon" in item and item["authorIcon"]:
            item["avatar"] = item["authorIcon"]
            item["authorThumbnail"] = item["authorIcon"]

        if "authorIcon" in item and item["authorIcon"]:
            item["authorIconUrl"] = item["authorIcon"]
            item["avatar"] = item["authorIcon"]
            item["authorThumbnail"] = item["authorIcon"]

        if not item.get("authorThumbnails") or not isinstance(item.get("authorThumbnails"), list):
            icon_url = item.get("authorIcon") or item.get("authorThumbnail") or item.get("avatar") or ""
            if icon_url:
                item["authorThumbnails"] = [{"url": icon_url}]
            else:
                item["authorThumbnails"] = []

        if "text" in item and "contentHtml" not in item and "content" not in item:
            text_str = item.get("text", "")
            item["content"] = text_str
            item["contentHtml"] = text_str.replace("\n", "<br>")
        elif "contentHtml" in item and "content" not in item:
            item["content"] = item.get("contentHtml", "")
        elif "content" in item and "contentHtml" not in item:
            item["contentHtml"] = item.get("content", "").replace("\n", "<br>")

        if not item.get("contentHtml"):
            text_str = item.get("text") or item.get("content") or ""
            item["contentHtml"] = text_str.replace("\n", "<br>")

        if "publishedTime" in item and "publishedText" not in item:
            item["publishedText"] = item.get("publishedTime", "")
        elif "publishedText" not in item:
            item["publishedText"] = item.get("published", "") or item.get("publishedTime", "")

        likes_obj = item.get("likes")
        if isinstance(likes_obj, dict):
            item["likeCount"] = likes_obj.get("count", 0)

        processed.append(item)
        
    return processed

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        search_history_json = request.cookies.get("search_history", "[]")
        search_history = json.loads(search_history_json)
    except:
        search_history = []

    # 過去5回の検索キーワードからおすすめ動画を取得
    recent_keywords = search_history[-5:] if search_history else ["ボカロ", "VTuber", "ゲーム実況", "音楽", "ニュース"]
    
    async def fetch_keyword_results(kw):
        try:
            res = await fetch_invidious("/search", {"q": kw, "page": 1, "type": "video"}, list_type="search")
            if isinstance(res, list):
                return [item for item in res if item.get("type") == "video" and item.get("videoId")]
        except:
            pass
        return []

    tasks = [fetch_keyword_results(kw) for kw in recent_keywords]
    results_list = await asyncio.gather(*tasks)

    recommended_videos = []
    seen_ids = set()
    for res in results_list:
        for item in res:
            vid = item.get("videoId")
            if vid and vid not in seen_ids:
                seen_ids.add(vid)
                recommended_videos.append(item)

    random.shuffle(recommended_videos)
    recommended_videos = recommended_videos[:24]

    return templates.TemplateResponse("home.html", {
        "request": request,
        "recommended_videos": recommended_videos
    })

@app.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = Query(...), page: int = 1, type: str = "video", force_instance: str = Query(None)):
    try:
        search_type = type if type != "short" else "video"
        query_q = q if type != "short" else f"{q} shorts"
        params = {"q": query_q, "page": page, "type": search_type}

        if force_instance:
            data = await fetch_invidious("/search", params, force_instance=force_instance, list_type="search")
        else:
            search_instances = await get_invidious_instances_from_url(INVIDIOUS_SEARCH_LIST_URL)
            instances = list(search_instances)
            random.shuffle(instances)
            target_instances = instances[:4]
            
            async def fetch_task(instance):
                url = f"{instance.rstrip('/')}/api/v1/search"
                resp = await client_session.get(url, params=params, timeout=2.8)
                resp.raise_for_status()
                return resp.json()

            tasks = [asyncio.create_task(fetch_task(inst)) for inst in target_instances]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            data = None
            for task in done:
                try:
                    data = task.result()
                    if data is not None:
                        break
                except:
                    continue
            
            for task in pending:
                task.cancel()
            
            if data is None:
                data = await fetch_invidious("/search", params, list_type="search")

        results = [{
            "type": item.get("type"),
            "videoId": item.get("videoId"),
            "playlistId": item.get("playlistId"),
            "authorId": item.get("authorId"),
            "title": item.get("title"),
            "lengthSeconds": item.get("lengthSeconds"),
            "author": item.get("author"),
            "authorThumbnails": item.get("authorThumbnails"),
            "videoThumbnails": item.get("videoThumbnails"),
            "viewCountText": item.get("viewCountText"),
            "viewCount": item.get("viewCount"),
            "publishedText": item.get("publishedText"),
            "subCountText": item.get("subCountText"),
            "videoCount": item.get("videoCount")
        } for item in data]

        response = templates.TemplateResponse("search.html", {
            "request": request, 
            "query": q, 
            "results": results,
            "type": type,
            "page": page
        })

        # 検索キーワードをCookie(search_history)に直近5回分保存
        try:
            search_history_json = request.cookies.get("search_history", "[]")
            search_history = json.loads(search_history_json)
            if q in search_history:
                search_history.remove(q)
            search_history.append(q)
            if len(search_history) > 5:
                search_history = search_history[-5:]
            response.set_cookie(key="search_history", value=json.dumps(search_history), max_age=2592000, httponly=True)
        except:
            pass
            
        return response
    except httpx.TimeoutException:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception:
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_SEARCH_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fallback_instances})

@app.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts_player(request: Request, v: str, force_instance: str = Query(None), api: str = Query(None)):
    try:
        video_info_task = fetch_video_info(v, force_instance=force_instance, api=api)
        stream_task = fetch_fastest_stream_urls(v, api=api, force_instance=force_instance)
        comment_task = fetch_comments(v, force_instance=force_instance, api=api)

        video_data, stream_data, comment_data = await asyncio.gather(
            video_info_task, stream_task, comment_task, return_exceptions=True
        )

        if isinstance(video_data, Exception) and (not stream_data or isinstance(stream_data, Exception)):
            raise video_data if isinstance(video_data, Exception) else Exception("Failed to load video")

        v_data = video_data if not isinstance(video_data, Exception) else {}
        
        if stream_data and not isinstance(stream_data, Exception) and stream_data.get("videoUrls"):
            video_urls = stream_data.get("videoUrls", [])
        else:
            invidious_streams = extract_invidious_streams(v_data)
            video_urls = invidious_streams.get("videoUrls", [])

        v_title = v_data.get("title", "")
        v_author = v_data.get("author", "")
        v_views = v_data.get("viewCount", 0)
        v_likes = v_data.get("likeCount", 0)
        v_desc = v_data.get("descriptionHtml") or v_data.get("description", "").replace("\n", "<br>")

        formatted_comments = process_comments(comment_data)

        return templates.TemplateResponse("short.html", {
            "request": request,
            "videoid": v,
            "video_title": v_title,
            "videourls": video_urls,
            "author": v_author,
            "view_count": v_views,
            "like_count": v_likes,
            "description": v_desc,
            "comments": formatted_comments
        })
    except httpx.TimeoutException:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception:
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fallback_instances})

@app.get("/watch", response_class=HTMLResponse)
async def watch(request: Request, v: str = Query(...), force_instance: str = Query(None), api: str = Query(None)):
    try:
        info_task = fetch_video_info(v, force_instance=force_instance, api=api)
        stream_task = fetch_fastest_stream_urls(v, api=api, force_instance=force_instance)
        comment_task = fetch_comments(v, force_instance=force_instance, api=api)

        video_data, stream_res, comment_data = await asyncio.gather(
            info_task, stream_task, comment_task, return_exceptions=True
        )

        if isinstance(video_data, Exception) and (not stream_res or isinstance(stream_res, Exception)):
            raise video_data if isinstance(video_data, Exception) else Exception("Failed to load video")

        v_data = video_data if not isinstance(video_data, Exception) else {}
        s_data = stream_res if (stream_res and not isinstance(stream_res, Exception)) else {}

        stream_urls = s_data.get("streamUrls", [])
        video_urls = s_data.get("videoUrls", [])

        if not stream_urls and v_data:
            invidious_streams = extract_invidious_streams(v_data)
            stream_urls = invidious_streams.get("streamUrls", [])
            video_urls = invidious_streams.get("videoUrls", [])

        recommended = []
        raw_recs = v_data.get("recommendedVideos", [])
        for rec in raw_recs:
            if not isinstance(rec, dict):
                continue
            recommended.append({
                "video_id": rec.get("video_id") or rec.get("videoId"),
                "title": rec.get("title"),
                "author": rec.get("author"),
                "view_count_text": rec.get("view_count_text") or rec.get("viewCountText"),
                "thumbnail": rec.get("thumbnail", "")
            })

        author_icon = v_data.get("authorIcon")
        if not author_icon:
            author_thumbs = v_data.get("authorThumbnails", [])
            author_icon = author_thumbs[-1]["url"] if author_thumbs else ""

        youtube_url = f"https://www.youtube.com/watch?v={v}"
        v_title = v_data.get("title") or s_data.get("title") or ""
        v_author = v_data.get("author") or s_data.get("author") or ""
        v_sub_count = v_data.get("subCountText") or v_data.get("subCountText", "非公開")
        v_desc = v_data.get("descriptionHtml") or s_data.get("descriptionHtml") or v_data.get("description", "").replace("\n", "<br>")

        formatted_comments = process_comments(comment_data)

        response = templates.TemplateResponse("watch.html", {
            "request": request,
            "videoid": v,
            "video_title": v_title,
            "videourls": video_urls,
            "streamUrls": stream_urls,
            "author": v_author,
            "author_id": v_data.get("authorId") or s_data.get("authorId"),
            "author_icon": author_icon,
            "subscribers_count": v_sub_count,
            "view_count": v_data.get("viewCount", s_data.get("viewCount", 0)),
            "like_count": v_data.get("likeCount", s_data.get("likeCount", 0)),
            "description": v_desc,
            "recommended_videos": recommended,
            "comments": formatted_comments,
            "youtube_url": youtube_url
        })

        try:
            history_json = request.cookies.get("history", "[]")
            history = json.loads(history_json)
            history = [item for item in history if item.get("videoId") != v]
            history.append({
                "videoId": v,
                "title": v_title,
                "author": v_author,
                "added_at": datetime.now().strftime("%Y-%m-%d %H:%M")
            })
            if len(history) > 50: history = history[-50:]
            response.set_cookie(key="history", value=json.dumps(history), max_age=2592000, httponly=True)
        except:
            pass

        return response

    except httpx.TimeoutException:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception:
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fallback_instances})

@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    try:
        history_list = json.loads(request.cookies.get("history", "[]"))
    except:
        history_list = []
    history_list.reverse()
    return templates.TemplateResponse("history.html", {"request": request, "history": history_list})

@app.get("/history/clear")
async def clear_history():
    response = RedirectResponse(url="/history")
    response.delete_cookie("history")
    return response

@app.get("/playlist", response_class=HTMLResponse)
async def playlist(request: Request, list: str = Query(...), force_instance: str = Query(None)):
    try:
        data = await fetch_invidious(f"/playlists/{list}", force_instance=force_instance, list_type="video")
        return templates.TemplateResponse("playlist.html", {
            "request": request,
            "title": data.get("title"),
            "playlistId": list,
            "author": data.get("author"),
            "authorId": data.get("authorId"),
            "videos": data.get("videos", []),
            "description": data.get("descriptionHtml", "")
        })
    except httpx.TimeoutException:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception:
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fallback_instances})

@app.get("/channel/{ucid}", response_class=HTMLResponse)
async def channel(request: Request, ucid: str, sort_by: str = "newest", tab: str = "videos", force_instance: str = Query(None), api: str = Query(None)):
    try:
        cache_key = f"channel_data_all:{ucid}:{sort_by}:{force_instance or ''}:{api or ''}"

        async def _do_fetch_channel():
            if api == "sia" or (not api and not force_instance):
                try:
                    sia_res = await fetch_sia_channel(ucid)
                    if sia_res and isinstance(sia_res, dict):
                        return {
                            "channel": sia_res,
                            "videos": sia_res.get("videos", []),
                            "shorts": sia_res.get("shorts", []),
                            "playlists": sia_res.get("playlists", []),
                            "community": sia_res.get("community", [])
                        }
                except Exception:
                    if api == "sia":
                        pass

            tasks = [
                fetch_invidious(f"/channels/{ucid}", force_instance=force_instance, list_type="search"),
                fetch_invidious(f"/channels/{ucid}/videos", {"sort_by": sort_by}, force_instance=force_instance, list_type="search"),
                fetch_invidious(f"/channels/{ucid}/shorts", force_instance=force_instance, list_type="search"),
                fetch_invidious(f"/channels/{ucid}/playlists", force_instance=force_instance, list_type="search"),
                fetch_invidious(f"/channels/{ucid}/community", force_instance=force_instance, list_type="search")
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return {
                "channel": results[0] if not isinstance(results[0], Exception) else {},
                "videos": results[1] if not isinstance(results[1], Exception) else {},
                "shorts": results[2] if not isinstance(results[2], Exception) else {},
                "playlists": results[3] if not isinstance(results[3], Exception) else {},
                "community": results[4] if not isinstance(results[4], Exception) else {}
            }

        fetched_res = await fetch_with_inflight(cache_key, _do_fetch_channel, ttl=180.0)

        channel_data = fetched_res.get("channel", {})
        videos_data = fetched_res.get("videos", {})
        shorts_data = fetched_res.get("shorts", {})
        playlists_data = fetched_res.get("playlists", {})
        community_data = fetched_res.get("community", {})

        if isinstance(videos_data, list):
            final_videos = videos_data
        elif isinstance(videos_data, dict):
            final_videos = videos_data.get("videos", [])
        else:
            final_videos = []

        if isinstance(shorts_data, list):
            final_shorts = shorts_data
        elif isinstance(shorts_data, dict):
            final_shorts = shorts_data.get("videos", [])
        else:
            final_shorts = []

        playlists = []
        raw_playlists = playlists_data.get("playlists", []) if isinstance(playlists_data, dict) else (playlists_data if isinstance(playlists_data, list) else [])
        for pl in raw_playlists:
            if not isinstance(pl, dict):
                continue
            thumb = pl.get("playlistThumbnail", "") or pl.get("thumbnail", "")
            if thumb and not thumb.startswith("http"):
                thumb = f"https://img.youtube.com/vi/{thumb}/mqdefault.jpg"
            playlists.append({
                "id": pl.get("playlistId", "") or pl.get("id", ""),
                "title": pl.get("title", ""),
                "video_count": pl.get("videoCount", 0),
                "thumbnail": thumb,
            })

        author_name = channel_data.get("author") or channel_data.get("name") or ""
        author_icon = ""
        if channel_data.get("authorThumbnails"):
            author_icon = channel_data.get("authorThumbnails")[-1]["url"]
        elif channel_data.get("authorIcon"):
            author_icon = channel_data.get("authorIcon")
        elif channel_data.get("avatar"):
            author_icon = channel_data.get("avatar")

        comments_list = community_data.get("comments", []) if isinstance(community_data, dict) else (community_data if isinstance(community_data, list) else [])
        community = []
        for post in comments_list:
            if not isinstance(post, dict):
                continue
            community.append({
                "id": post.get("commentId", "") or post.get("id", ""),
                "content": (post.get("contentHtml") or post.get("text") or post.get("content") or "").replace("\n", "<br>"),
                "published_text": post.get("publishedText") or post.get("publishedTime") or "",
                "likes": post.get("likeCount") or (post.get("likes", {}).get("count") if isinstance(post.get("likes"), dict) else 0),
                "author": author_name,
                "author_icon": author_icon,
            })

        return templates.TemplateResponse("channel.html", {
            "request": request,
            "ucid": ucid,
            "author": author_name,
            "author_icon": author_icon,
            "sub_count": channel_data.get("subCountText", "非公開"),
            "description": channel_data.get("descriptionHtml") or channel_data.get("description", ""),
            "videos": final_videos,
            "shorts": final_shorts,
            "playlists": playlists,
            "community": community,
            "sort_by": sort_by,
            "tab": tab
        })
    except httpx.TimeoutException:
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception:
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_SEARCH_LIST_URL)
        return templates.TemplateResponse("apiallerror.html", {"request": request, "instances": fallback_instances})

@app.get("/suggest")
async def suggest(keyword: str):
    cache_key = f"suggest:{keyword}"

    async def _do_fetch():
        search_instances = await get_invidious_instances_from_url(INVIDIOUS_SEARCH_LIST_URL)
        instances = list(search_instances)
        random.shuffle(instances)
        for instance in instances:
            try:
                resp = await client_session.get(f"{instance.rstrip('/')}/api/v1/search/suggestions", params={"q": keyword}, timeout=1.2)
                if resp.status_code == 200:
                    return resp.json().get("suggestions", [])
            except: continue
        return []

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=600.0)

@app.get("/proxy/thumb")
async def proxy_thumb(v: str):
    cache_key = f"thumb:{v}"

    async def _do_fetch():
        thumb_url = f"https://i.ytimg.com/vi/{v}/mqdefault.jpg"
        try:
            resp = await client_session.get(thumb_url, timeout=3.0)
            if resp.status_code == 200:
                return resp.content
        except:
            pass
        return None

    content = await fetch_with_inflight(cache_key, _do_fetch, ttl=1800.0)
    if content:
        return Response(content=content, media_type="image/jpeg")
    return Response(status_code=404)

@app.get("/thumbnail")
async def thumbnail(v: str):
    return await proxy_thumb(v)

@app.get("/games", response_class=HTMLResponse)
async def read_games(request: Request):
    return templates.TemplateResponse("games.html", {"request": request})

@app.get("/block.html", response_class=HTMLResponse)
async def read_block(request: Request):
    return templates.TemplateResponse("block.html", {"request": request})

@app.get("/tumu.html", response_class=HTMLResponse)
async def read_tumu(request: Request):
    return templates.TemplateResponse("tumu.html", {"request": request})

@app.get("/2048.html", response_class=HTMLResponse)
async def read_2048(request: Request):
    return templates.TemplateResponse("2048.html", {"request": request})

@app.get("/status", response_class=HTMLResponse)
async def read_status(request: Request):
    async def check_instance(instance):
        start_time = asyncio.get_event_loop().time()
        try:
            resp = await client_session.get(f"{instance.rstrip('/')}/api/v1/stats", timeout=3.0)
            latency = (asyncio.get_event_loop().time() - start_time) * 1000
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "instance": instance,
                    "status": "Online",
                    "latency": f"{int(latency)}ms",
                    "version": data.get("software", {}).get("version", "unknown"),
                    "users": data.get("usage", {}).get("users", {}).get("total", 0)
                }
            return {"instance": instance, "status": f"Error {resp.status_code}", "latency": "-", "version": "-", "users": "-"}
        except:
            return {"instance": instance, "status": "Offline", "latency": "-", "version": "-", "users": "-"}

    video_instances = await get_invidious_instances_from_url(INVIDIOUS_VIDEO_LIST_URL)
    status_results = await asyncio.gather(*(check_instance(inst) for inst in video_instances))
    return templates.TemplateResponse("status.html", {"request": request, "instances": status_results})

@app.get("/subscriptions", response_class=HTMLResponse)
async def subscriptions_page(request: Request):
    return templates.TemplateResponse("subscriptions.html", {"request": request})

@app.get("/bbs", response_class=HTMLResponse)
async def bbs_page(request: Request):
    return templates.TemplateResponse("bbs.html", {"request": request})

@app.get("/ytdl", response_class=HTMLResponse)
async def ytdl_page(request: Request):
    return templates.TemplateResponse("bbs.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
