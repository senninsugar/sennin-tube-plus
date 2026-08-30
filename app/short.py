import asyncio
import json
import re
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter()

SEARXNG_INSTANCES = [
    "https://search.unredacted.org/",
    "https://searx.perennialte.ch/",
]


def extract_view_number(raw_text: str) -> int:
    """ビュー数を数字に変換"""
    if not raw_text:
        return 0
    text = str(raw_text).lower().replace(",", "").strip()
    try:
        if "k" in text:
            num = float(re.sub(r"[^\d.]", "", text.split("k")[0]))
            return int(num * 1_000)
        if "m" in text:
            num = float(re.sub(r"[^\d.]", "", text.split("m")[0]))
            return int(num * 1_000_000)
        if "b" in text:
            num = float(re.sub(r"[^\d.]", "", text.split("b")[0]))
            return int(num * 1_000_000_000)
        
        if "万" in text:
            num = float(re.sub(r"[^\d.]", "", text.split("万")[0]))
            return int(num * 10_000)
        if "億" in text:
            num = float(re.sub(r"[^\d.]", "", text.split("億")[0]))
            return int(num * 100_000_000)

        digits = "".join(c for c in text if c.isdigit())
        return int(digits) if digits else 0
    except (ValueError, TypeError):
        return 0


def extract_video_id_from_url(url: str) -> Optional[str]:
    """YouTubeのURLからvideoIdを抽出"""
    if not url:
        return None
    
    # youtube.com/watch?v=xxxxx
    match = re.search(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', url)
    if match:
        return match.group(1)
    
    # youtu.be/xxxxx
    match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
    if match:
        return match.group(1)
    
    # youtube.com/shorts/xxxxx
    match = re.search(r'youtube\.com/shorts/([a-zA-Z0-9_-]{11})', url)
    if match:
        return match.group(1)
    
    return None


def format_short_payload(
    video_id: str,
    title: str,
    author: str = "",
    author_id: str = "",
    length_seconds: int = 30,
    view_count: int = 0,
    view_count_text: str = "",
    thumbnail_url: Optional[str] = None,
    published_text: str = "",
) -> Dict[str, Any]:
    """Shorts形式のペイロードを生成"""
    if not thumbnail_url:
        thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"

    return {
        "videoId": video_id,
        "title": title,
        "lengthSeconds": length_seconds,
        "isShort": True,
        "author": author,
        "authorId": author_id,
        "authorThumbnails": [],
        "viewCount": view_count,
        "viewCountText": view_count_text or (f"{view_count:,}" if view_count else ""),
        "videoThumbnails": [
            {"url": thumbnail_url, "quality": "high"}
        ],
        "publishedText": published_text,
    }


def process_searxng_result(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """SearXNGの検索結果をShorts形式に変換"""
    if not isinstance(result, dict):
        return None

    url = result.get("url", "")
    video_id = extract_video_id_from_url(url)
    
    if not video_id:
        return None

    title = result.get("title", "")
    if not title:
        return None

    # 説明文からビュー数や投稿者を抽出
    content = result.get("content", "") or ""
    author = ""
    view_count_text = ""
    
    # 説明文から投稿者を抽出 (例: "by Channel Name")
    author_match = re.search(r'by\s+([^\-\n•]+)', content)
    if author_match:
        author = author_match.group(1).strip()
    
    # 説明文からビュー数を抽出
    view_match = re.search(r'(\d+(?:[,\.]\d+)*)\s*(?:views?|回)', content, re.IGNORECASE)
    if view_match:
        view_count_text = view_match.group(1)

    view_count = extract_view_number(view_count_text)

    # サムネイル取得
    thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
    
    # SearXNG metadata からサムネイルを取得
    metadata = result.get("metadata", {})
    if isinstance(metadata, dict):
        img = metadata.get("img", {})
        if isinstance(img, dict) and img.get("url"):
            thumbnail_url = img["url"]

    return format_short_payload(
        video_id=video_id,
        title=title,
        author=author,
        length_seconds=30,
        view_count=view_count,
        view_count_text=view_count_text,
        thumbnail_url=thumbnail_url,
    )


async def search_shorts_from_instance(
    instance: str,
    search_query: str,
    page: int = 1
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """単一のSearXNGインスタンスからShorts検索（デバッグ情報付き）"""
    debug_info = {
        "instance": instance,
        "query": search_query,
        "page": page,
        "status": "pending",
        "error": None,
        "total_results": 0,
        "youtube_results": 0,
    }
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{instance}search",
                params={
                    "q": search_query,
                    "format": "json",
                    "categories": "video",
                    "pageno": page,
                },
                timeout=httpx.Timeout(15.0),
            )
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            debug_info["total_results"] = len(results)
            debug_info["status"] = "success"
            
            shorts = []
            
            for result in results:
                short = process_searxng_result(result)
                if short:
                    shorts.append(short)
                    debug_info["youtube_results"] += 1
            
            return shorts, debug_info
    except Exception as e:
        debug_info["status"] = "error"
        debug_info["error"] = str(e)
        print(f"Error searching {instance}: {str(e)}")
        return [], debug_info


@router.get("/v1/shorts/query")
async def fetch_shorts_by_query(q: str = Query(...)):
    """SearXNG APIを使用してShortsを検索（単一ページ）"""
    try:
        search_query = f"{q} shorts"
        
        # 複数インスタンスから並行検索
        tasks = [
            search_shorts_from_instance(instance, search_query, 1)
            for instance in SEARXNG_INSTANCES
        ]
        results_list = await asyncio.gather(*tasks)
        
        # 結果を統合
        all_shorts = []
        seen = set()
        debug_data = []
        
        for shorts, debug_info in results_list:
            debug_data.append(debug_info)
            for short in shorts:
                video_id = short.get("videoId")
                if video_id not in seen:
                    seen.add(video_id)
                    all_shorts.append(short)
        
        return JSONResponse({
            "items": all_shorts,
            "debug": debug_data
        })
    except Exception as e:
        return JSONResponse({"error": str(e), "items": [], "debug": []}, status_code=502)


@router.get("/v1/shorts/query/next")
async def fetch_shorts_next_page(q: str = Query(...), page: int = Query(2)):
    """次ページのShorts検索結果を取得"""
    try:
        search_query = f"{q} shorts"
        
        # 複数インスタンスから並行検索
        tasks = [
            search_shorts_from_instance(instance, search_query, page)
            for instance in SEARXNG_INSTANCES
        ]
        results_list = await asyncio.gather(*tasks)
        
        # 結果を統合
        all_shorts = []
        seen = set()
        debug_data = []
        
        for shorts, debug_info in results_list:
            debug_data.append(debug_info)
            for short in shorts:
                video_id = short.get("videoId")
                if video_id not in seen:
                    seen.add(video_id)
                    all_shorts.append(short)
        
        return JSONResponse({
            "items": all_shorts,
            "debug": debug_data
        })
    except Exception as e:
        return JSONResponse({"error": str(e), "items": [], "debug": []}, status_code=502)


@router.get("/v1/shorts/stream")
async def stream_shorts_feed(q: str = Query(...)):
    """Shortsストリーミング検索エンドポイント - SearXNG APIのみ使用"""
    
    async def generate_event_stream():
        """イベントストリーム生成"""
        seen: set[str] = set()
        query_variants = [f"{q} shorts", f"{q} ショート", q]
        
        # 複数インスタンス、複数クエリ、複数ページを並行実行
        tasks = [
            search_shorts_from_instance(instance, search_query, page)
            for instance in SEARXNG_INSTANCES
            for search_query in query_variants
            for page in range(1, 5)
        ]
        
        for coro in asyncio.as_completed(tasks):
            batch, debug_info = await coro
            new_items = []
            
            for short in batch:
                if short and short.get("videoId") not in seen:
                    seen.add(short["videoId"])
                    new_items.append(short)

            # バッチごとにイベント送信
            if new_items:
                yield f"data: {json.dumps({'items': new_items}, ensure_ascii=False)}\n\n"

        # 完了イベント送信
        yield 'data: {"done":true}\n\n'

    return StreamingResponse(
        generate_event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/v1/shorts/debug")
async def debug_searxng(q: str = Query("猫")):
    """SearXNGインスタンスのデバッグエンドポイント"""
    debug_results = []
    
    for instance in SEARXNG_INSTANCES:
        try:
            async with httpx.AsyncClient() as client:
                # 生のAPIレスポンスを取得
                resp = await client.get(
                    f"{instance}search",
                    params={
                        "q": f"{q} shorts",
                        "format": "json",
                        "categories": "video",
                        "pageno": 1,
                    },
                    timeout=httpx.Timeout(15.0),
                )
                resp.raise_for_status()
                data = resp.json()
                
                results = data.get("results", [])
                
                debug_results.append({
                    "instance": instance,
                    "status": "success",
                    "total_results": len(results),
                    "sample_results": results[:3],  # 最初の3件を表示
                })
        except Exception as e:
            debug_results.append({
                "instance": instance,
                "status": "error",
                "error": str(e),
            })
    
    return JSONResponse({
        "query": q,
        "instances": debug_results
    })
