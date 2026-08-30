import asyncio
import base64
import json
import time
import httpx
import logging
from typing import Optional, Any, Dict, List
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# ロギング設定
logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")

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
    "https://inv.thepixora.com",
]

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]

SENNIN_API_BASE = "https://discerning-adventure-production-ebfc.up.railway.app"
RAPID_API_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"

_ENCRYPTED_KEYS = [
    "ZTYxNTE4MzAzNG1zaDJkZmRhMzFhNDdhNmYxMnAxZmE2Y2Nqc241OWExYTVlMDY0MTU=",
    "NjllMjk5OWE3OW1zaGNiNjU3MTg0YmE2NzMxY3AxNmY2ODRqc24zMjA1NGEwNzBiYTU=",
]

INVIDIOUS_VIDEO_LIST_URL = "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/video.json"
INVIDIOUS_SEARCH_LIST_URL = "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/search.json"

# 改善: より強固なHTTPクライアント設定
limits = httpx.Limits(max_connections=500, max_keepalive_connections=200)
client_session = httpx.AsyncClient(
    timeout=httpx.Timeout(4.5, connect=2.0, read=3.5),
    limits=limits,
    follow_redirects=True,
    http2=False,  # HTTP/2の問題を回避
)
no_redirect_client = httpx.AsyncClient(
    timeout=httpx.Timeout(3.5, connect=1.5, read=2.5),
    limits=limits,
    follow_redirects=False,
    http2=False,
)

_CACHE: Dict[str, Dict[str, Any]] = {}
_INFLIGHT: Dict[str, asyncio.Future] = {}
_CACHE_LOCK = asyncio.Lock()
_INSTANCE_HEALTH: Dict[str, Dict[str, Any]] = {}  # インスタンスのヘルスチェック
_HEALTH_LOCK = asyncio.Lock()

# キャッシュサイズ制限
MAX_CACHE_SIZE = 2000
CACHE_CLEANUP_INTERVAL = 3600


def _get_rapid_api_keys() -> List[str]:
    """復号化されたRapid APIキーを取得"""
    return [
        base64.b64decode(k.encode("utf-8")).decode("utf-8")
        for k in _ENCRYPTED_KEYS
    ]


def get_cache(key: str) -> Optional[Any]:
    """キャッシュから値を取得"""
    now = time.time()
    hit = _CACHE.get(key)
    if hit and hit["exp"] > now:
        return hit["val"]
    elif hit:
        _CACHE.pop(key, None)  # 期限切れキャッシュを削除
    return None


def set_cache(key: str, val: Any, ttl: float = 180.0) -> None:
    """キャッシュに値を設定"""
    now = time.time()
    # キャッシュサイズチェック
    if len(_CACHE) >= MAX_CACHE_SIZE:
        # 最も古いキャッシュを複数削除
        oldest_keys = sorted(
            _CACHE.items(),
            key=lambda x: x[1]["exp"]
        )[:int(MAX_CACHE_SIZE * 0.1)]
        for k, _ in oldest_keys:
            _CACHE.pop(k, None)
    
    _CACHE[key] = {"val": val, "exp": now + ttl}


async def fetch_with_inflight(
    key: str,
    fetch_func,
    ttl: float = 180.0,
    retry_count: int = 3,
) -> Optional[Any]:
    """インフライト重複排除付きフェッチ"""
    cached = get_cache(key)
    if cached is not None:
        return cached

    async with _CACHE_LOCK:
        cached = get_cache(key)
        if cached is not None:
            return cached
        
        if key in _INFLIGHT:
            fut = _INFLIGHT[key]
            try:
                return await asyncio.wait_for(asyncio.shield(fut), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning(f"Inflight request timeout for key: {key}")
                _INFLIGHT.pop(key, None)

        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        _INFLIGHT[key] = fut

    try:
        res = None
        last_error = None
        
        for attempt in range(retry_count):
            try:
                res = await asyncio.wait_for(fetch_func(), timeout=30.0)
                if res is not None:
                    set_cache(key, res, ttl=ttl)
                    if not fut.done():
                        fut.set_result(res)
                    return res
            except asyncio.TimeoutError as e:
                last_error = e
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
            except Exception as e:
                last_error = e
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
        
        if not fut.done():
            if last_error:
                fut.set_exception(last_error)
            else:
                fut.set_result(None)
        return res if res is not None else None
        
    except Exception as e:
        logger.error(f"Fetch error for key {key}: {e}")
        if not fut.done():
            fut.set_exception(e)
        raise
    finally:
        async with _CACHE_LOCK:
            _INFLIGHT.pop(key, None)


async def get_invidious_instances_from_url(list_url: str) -> List[str]:
    """URLからInvidious インスタンスリストを取得"""
    cache_key = f"inv_instances_list:{list_url}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    retry_count = 3
    last_error = None
    
    for attempt in range(retry_count):
        try:
            resp = await asyncio.wait_for(
                client_session.get(list_url, timeout=3.0),
                timeout=5.0
            )
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
                            inst = item.strip()
                            if inst:
                                instances.append(inst)
                
                if instances:
                    set_cache(cache_key, instances, ttl=600.0)
                    return instances
        except Exception as e:
            last_error = e
            logger.debug(f"Attempt {attempt + 1} to fetch instances failed: {e}")
            if attempt < retry_count - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
            continue

    logger.warning(f"Failed to fetch instances from {list_url}, using fallback")
    return FALLBACK_INVIDIOUS_INSTANCES


async def record_instance_health(instance: str, success: bool, response_time: float) -> None:
    """インスタンスのヘルスを記録"""
    async with _HEALTH_LOCK:
        if instance not in _INSTANCE_HEALTH:
            _INSTANCE_HEALTH[instance] = {
                "success_count": 0,
                "failure_count": 0,
                "last_response_time": 0,
                "last_checked": 0,
            }
        
        health = _INSTANCE_HEALTH[instance]
        if success:
            health["success_count"] += 1
            health["last_response_time"] = response_time
        else:
            health["failure_count"] += 1
        health["last_checked"] = time.time()


async def get_fastest_invidious_instance(
    list_url: str = INVIDIOUS_VIDEO_LIST_URL,
) -> str:
    """最速のInvidious インスタンスを取得"""
    cache_key = f"fastest_inv_instance:{list_url}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    base_instances = await get_invidious_instances_from_url(list_url)
    if not base_instances:
        return FALLBACK_INVIDIOUS_INSTANCES[0]
    
    target_instances = base_instances[:15]

    async def ping_instance(instance: str) -> tuple:
        start = time.time()
        try:
            url = f"{instance.rstrip('/')}/api/v1/stats"
            resp = await asyncio.wait_for(
                client_session.get(url, timeout=2.0),
                timeout=3.0
            )
            if resp.status_code == 200:
                elapsed = time.time() - start
                await record_instance_health(instance, True, elapsed)
                return instance, elapsed
        except Exception as e:
            logger.debug(f"Ping failed for {instance}: {e}")
            await record_instance_health(instance, False, 0)
        return instance, float("inf")

    tasks = [ping_instance(inst) for inst in target_instances]
    results = await asyncio.gather(*tasks)

    valid_results = [r for r in results if r[1] < float("inf")]
    if valid_results:
        fastest_instance = min(valid_results, key=lambda x: x[1])[0]
        set_cache(cache_key, fastest_instance, ttl=300.0)
        return fastest_instance

    return base_instances[0] if base_instances else FALLBACK_INVIDIOUS_INSTANCES[0]


def _is_valid_invidious_response(res: Any) -> bool:
    """Invidious レスポンスの妥当性を検証"""
    if not res:
        return False
    if isinstance(res, dict):
        # エラーキーをチェック
        if any(key in res for key in ["error", "message", "status"]):
            return False
        # 最小限の必須フィールドがあるかチェック
        return len(res) > 0
    if isinstance(res, list):
        return len(res) > 0
    return False


async def fetch_invidious(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    force_instance: Optional[str] = None,
    list_type: str = "video",
    max_retries: int = 3,
) -> Optional[Any]:
    """Invidious APIからデータを取得（高可用性版）"""
    param_str = json.dumps(params, sort_keys=True) if params else ""
    cache_key = f"inv:{endpoint}:{param_str}:{force_instance or ''}:{list_type}"

    async def _do_fetch() -> Optional[Any]:
        list_url = (
            INVIDIOUS_SEARCH_LIST_URL
            if list_type == "search"
            else INVIDIOUS_VIDEO_LIST_URL
        )
        base_instances = await get_invidious_instances_from_url(list_url)

        if not base_instances:
            raise Exception("No Invidious instances available")

        if force_instance:
            instances = [force_instance] + [
                i for i in base_instances if i != force_instance
            ]
            last_error = None
            
            for instance in instances[:5]:  # 最大5インスタンスまで
                for retry in range(max_retries):
                    try:
                        url = f"{instance.rstrip('/')}/api/v1{endpoint}"
                        response = await asyncio.wait_for(
                            client_session.get(url, params=params, timeout=4.0),
                            timeout=6.0
                        )
                        response.raise_for_status()
                        res_data = response.json()
                        
                        if _is_valid_invidious_response(res_data):
                            await record_instance_health(instance, True, 0)
                            return res_data
                        else:
                            logger.warning(f"Invalid response from {instance}: {res_data}")
                            await record_instance_health(instance, False, 0)
                    except Exception as e:
                        logger.debug(f"Retry {retry + 1} failed for {instance}: {e}")
                        last_error = e
                        await record_instance_health(instance, False, 0)
                        if retry < max_retries - 1:
                            await asyncio.sleep(0.3 * (retry + 1))
                        continue
            
            raise last_error if last_error else Exception("All instances failed")
        
        else:
            # 最速インスタンスから並列フェッチ
            fastest = await get_fastest_invidious_instance(list_url)
            instances = [fastest] + [i for i in base_instances if i != fastest]
            target_instances = instances[:10]  # 最大10個を並列で試す

            async def task(instance: str) -> Optional[Any]:
                for retry in range(2):
                    try:
                        url = f"{instance.rstrip('/')}/api/v1{endpoint}"
                        resp = await asyncio.wait_for(
                            client_session.get(url, params=params, timeout=3.5),
                            timeout=5.0
                        )
                        resp.raise_for_status()
                        res_data = resp.json()
                        
                        if _is_valid_invidious_response(res_data):
                            await record_instance_health(instance, True, 0)
                            return res_data
                        else:
                            logger.debug(f"Invalid response from {instance}")
                    except Exception as e:
                        logger.debug(f"Task failed for {instance} (retry {retry + 1}): {e}")
                        await record_instance_health(instance, False, 0)
                        if retry < 1:
                            await asyncio.sleep(0.2)
                
                raise Exception(f"Failed to fetch from {instance}")

            # 最初のタスクを並列実行
            tasks = {asyncio.create_task(task(inst)): inst for inst in target_instances}
            
            while tasks:
                done, pending = await asyncio.wait(
                    tasks.keys(),
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=8.0
                )
                
                for completed_task in done:
                    try:
                        res = await completed_task
                        if _is_valid_invidious_response(res):
                            # 他のタスクをキャンセル
                            for t in pending:
                                t.cancel()
                                try:
                                    await t
                                except asyncio.CancelledError:
                                    pass
                            return res
                    except Exception as e:
                        logger.debug(f"Task failed: {e}")
                
                # タスク完了時に残りのタスクを削除
                for t in done:
                    tasks.pop(t, None)
                
                if not tasks and not done:
                    break
            
            # 残りのインスタンスを順序で試す
            remaining = [i for i in instances if i not in target_instances]
            for inst in remaining[:5]:
                for retry in range(2):
                    try:
                        url = f"{inst.rstrip('/')}/api/v1{endpoint}"
                        response = await asyncio.wait_for(
                            client_session.get(
                                url, params=params, timeout=3.5
                            ),
                            timeout=5.0
                        )
                        response.raise_for_status()
                        res_data = response.json()
                        
                        if _is_valid_invidious_response(res_data):
                            await record_instance_health(inst, True, 0)
                            return res_data
                    except Exception as e:
                        logger.debug(f"Remaining instance {inst} failed (retry {retry + 1}): {e}")
                        await record_instance_health(inst, False, 0)
                        if retry < 1:
                            await asyncio.sleep(0.2)
            
            raise Exception("All Invidious instances failed")

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=180.0, retry_count=2)


@router.get("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    q: str = Query(...),
    page: int = 1,
    type: str = Query("video"),
    force_instance: str = Query(None),
):
    """検索エンドポイント（安定化版）"""
    try:
        search_type = type if type != "short" else "video"
        query_q = q if type != "short" else f"{q} shorts"
        params = {"q": query_q, "page": page, "type": search_type}

        try:
            data = await asyncio.wait_for(
                fetch_invidious(
                    "/search",
                    params,
                    force_instance=force_instance,
                    list_type="search",
                ),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.error("Search request timeout")
            return templates.TemplateResponse("apitimeout.html", {"request": request})

        results_raw = data if isinstance(data, list) else []

        # 結果タイプ別にフィルタリング
        if type == "short":
            results = [
                {
                    "type": item.get("type"),
                    "videoId": item.get("videoId"),
                    "title": item.get("title"),
                    "lengthSeconds": item.get("lengthSeconds"),
                    "author": item.get("author"),
                    "authorThumbnails": item.get("authorThumbnails"),
                    "videoThumbnails": item.get("videoThumbnails"),
                    "viewCountText": item.get("viewCountText"),
                    "viewCount": item.get("viewCount"),
                    "publishedText": item.get("publishedText"),
                }
                for item in results_raw
                if item.get("type") == "video" and item.get("videoId")
            ]
        elif type == "channel":
            results = [
                {
                    "type": item.get("type"),
                    "authorId": item.get("authorId"),
                    "author": item.get("author"),
                    "authorThumbnails": item.get("authorThumbnails"),
                    "subCountText": item.get("subCountText"),
                    "videoCount": item.get("videoCount"),
                }
                for item in results_raw
                if item.get("type") == "channel"
            ]
        elif type == "playlist":
            results = [
                {
                    "type": item.get("type"),
                    "playlistId": item.get("playlistId"),
                    "title": item.get("title"),
                    "author": item.get("author"),
                    "authorThumbnails": item.get("authorThumbnails"),
                    "videoThumbnails": item.get("videoThumbnails"),
                    "videoCount": item.get("videoCount"),
                }
                for item in results_raw
                if item.get("type") == "playlist"
            ]
        else:
            results = [
                {
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
                    "videoCount": item.get("videoCount"),
                }
                for item in results_raw
            ]

        response = templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "query": q,
                "results": results,
                "type": type,
                "page": page,
            },
        )

        try:
            search_history_json = request.cookies.get("search_history", "[]")
            search_history = json.loads(search_history_json)
            if q in search_history:
                search_history.remove(q)
            search_history.insert(0, q)
            if len(search_history) > 5:
                search_history = search_history[:5]
            response.set_cookie(
                key="search_history",
                value=json.dumps(search_history),
                max_age=2592000,
                httponly=True,
            )
        except Exception as e:
            logger.debug(f"Failed to update search history: {e}")

        return response
        
    except httpx.TimeoutException:
        logger.error("HTTP timeout during search")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception as e:
        logger.error(f"Search error: {e}")
        fallback_instances = await get_invidious_instances_from_url(
            INVIDIOUS_SEARCH_LIST_URL
        )
        return templates.TemplateResponse(
            "apiallerror.html",
            {"request": request, "instances": fallback_instances},
        )


@router.get("/playlist", response_class=HTMLResponse)
async def playlist(
    request: Request,
    list: str = Query(...),
    force_instance: str = Query(None),
):
    """プレイリストエンドポイント（安定化版）"""
    try:
        try:
            data = await asyncio.wait_for(
                fetch_invidious(
                    f"/playlists/{list}",
                    force_instance=force_instance,
                    list_type="video"
                ),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.error(f"Playlist request timeout for {list}")
            return templates.TemplateResponse("apitimeout.html", {"request": request})

        return templates.TemplateResponse(
            "playlist.html",
            {
                "request": request,
                "title": data.get("title", "Unknown"),
                "playlistId": list,
                "author": data.get("author", "Unknown"),
                "authorId": data.get("authorId", ""),
                "videos": data.get("videos", []),
                "description": data.get("descriptionHtml", ""),
            },
        )
    except httpx.TimeoutException:
        logger.error("HTTP timeout during playlist fetch")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    except Exception as e:
        logger.error(f"Playlist error: {e}")
        fallback_instances = await get_invidious_instances_from_url(
            INVIDIOUS_VIDEO_LIST_URL
        )
        return templates.TemplateResponse(
            "apiallerror.html",
            {"request": request, "instances": fallback_instances},
        )
