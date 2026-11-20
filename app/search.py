import asyncio
import base64
import json
import random
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
    """キャッシュ + 同一キーの同時取得を1本にまとめる(インフライト重複排除).

    - ロックは状態の確認・登録だけに使い、通信中は保持しない(以前は待機中もロックを握り、全リクエストが直列化していた)
    - 例外・タイムアウトのときだけリトライし、None(=該当なし)は即座に返す
    """
    cached = get_cache(key)
    if cached is not None:
        return cached

    async with _CACHE_LOCK:
        cached = get_cache(key)
        if cached is not None:
            return cached
        fut = _INFLIGHT.get(key)
        is_owner = fut is None
        if is_owner:
            fut = asyncio.get_running_loop().create_future()
            _INFLIGHT[key] = fut

    if not is_owner:
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=30.0)
        except Exception:
            return None

    result: Optional[Any] = None
    try:
        for attempt in range(max(1, retry_count)):
            try:
                result = await asyncio.wait_for(fetch_func(), timeout=30.0)
                break
            except Exception as e:  # タイムアウト含む
                logger.debug(f"fetch attempt {attempt + 1} failed for {key}: {e}")
                result = None
                if attempt < retry_count - 1:
                    await asyncio.sleep(0.3 * (attempt + 1))
        if result is not None:
            set_cache(key, result, ttl=ttl)
        return result
    finally:
        if not fut.done():
            fut.set_result(result)
        async with _CACHE_LOCK:
            if _INFLIGHT.get(key) is fut:
                _INFLIGHT.pop(key, None)


async def race_first(
    factories: List[Any],
    accept,
    timeout: float = 8.0,
) -> Optional[Any]:
    """複数の取得処理を並列に走らせ、`accept` を満たす最初の結果を返す.

    以前の実装は「最初に終わったもの」が空・失敗だと残りを待たずに諦めていたため、
    ここでは「条件を満たす最初の結果」を返す。勝者が決まったら残りはキャンセルする。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    tasks = [asyncio.ensure_future(f()) for f in factories]
    try:
        pending = set(tasks)
        while pending:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for t in done:
                if t.cancelled() or t.exception() is not None:
                    continue
                res = t.result()
                if accept(res):
                    return res
        return None
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def get_invidious_instances_from_url(list_url: str) -> List[str]:
    """URLからInvidious インスタンスリストを取得.

    失敗時もフォールバック一覧を短時間キャッシュし、毎リクエストでリトライ待ちが発生しないようにする。
    """
    cache_key = f"inv_instances_list:{list_url}"
    cached = get_cache(cache_key)
    if cached:
        return cached

    for attempt in range(2):
        try:
            resp = await asyncio.wait_for(
                client_session.get(list_url, timeout=3.0),
                timeout=4.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                instances = []
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "instance" in item:
                            inst = str(item["instance"]).strip()
                            if inst:
                                instances.append(inst)
                        elif isinstance(item, str) and item.strip():
                            instances.append(item.strip())
                if instances:
                    set_cache(cache_key, instances, ttl=600.0)
                    return instances
        except Exception as e:
            logger.debug(f"Attempt {attempt + 1} to fetch instances failed: {e}")
            if attempt == 0:
                await asyncio.sleep(0.3)

    logger.warning(f"Failed to fetch instances from {list_url}, using fallback")
    set_cache(cache_key, list(FALLBACK_INVIDIOUS_INSTANCES), ttl=120.0)
    return list(FALLBACK_INVIDIOUS_INSTANCES)


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


def _rank_instances(instances: List[str]) -> List[str]:
    """ネットワークを使わずに並びを決める(成功実績が多いものを先頭、同順位内はランダム)."""
    shuffled = list(instances)
    random.shuffle(shuffled)

    def score(inst: str) -> int:
        h = _INSTANCE_HEALTH.get(inst)
        if not h or (h["success_count"] + h["failure_count"]) == 0:
            return 1
        return 0 if h["success_count"] >= h["failure_count"] else 2

    return sorted(shuffled, key=score)


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


async def _get_json_from_instance(
    instance: str,
    endpoint: str,
    params: Optional[Dict[str, Any]],
    timeout: float = 3.5,
    retries: int = 2,
) -> Optional[Any]:
    """1インスタンスからJSONを取得。取得失敗・不正データは None。"""
    url = f"{instance.rstrip('/')}/api/v1{endpoint}"
    for attempt in range(max(1, retries)):
        try:
            resp = await asyncio.wait_for(
                client_session.get(url, params=params, timeout=timeout),
                timeout=timeout + 1.0,
            )
            resp.raise_for_status()
            data = resp.json()
            if _is_valid_invidious_response(data):
                await record_instance_health(instance, True, 0)
                return data
            # 応答は届いたが中身が無効 → 同じインスタンスに再試行しても無意味
            await record_instance_health(instance, False, 0)
            return None
        except Exception as e:
            logger.debug(f"Invidious {instance} attempt {attempt + 1} failed: {e}")
            await record_instance_health(instance, False, 0)
            if attempt < retries - 1:
                await asyncio.sleep(0.2)
    return None


async def fetch_invidious(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    force_instance: Optional[str] = None,
    list_type: str = "video",
    max_retries: int = 2,
) -> Optional[Any]:
    """Invidious APIからデータを取得.

    通常時は上位インスタンスへ並列リクエストし、最初に有効な応答を採用する。
    以前は最速インスタンスを毎回15台 ping してから取得していたため、キャッシュミス時に数秒遅れていた。
    """
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
            ordered = [force_instance] + [i for i in base_instances if i != force_instance]
            for inst in ordered[:4]:
                data = await _get_json_from_instance(inst, endpoint, params, 4.0, max_retries)
                if data is not None:
                    return data
            raise Exception("All instances failed (forced)")

        ordered = _rank_instances(base_instances)
        factories = [
            (lambda inst=inst: _get_json_from_instance(inst, endpoint, params, 3.5, max_retries))
            for inst in ordered[:8]
        ]
        result = await race_first(factories, accept=_is_valid_invidious_response, timeout=8.0)
        if result is not None:
            return result

        # 並列で当たらなかった残りを順番に試す
        for inst in ordered[8:12]:
            data = await _get_json_from_instance(inst, endpoint, params, 3.5, 1)
            if data is not None:
                return data
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
