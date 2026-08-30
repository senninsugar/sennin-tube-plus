import asyncio
import httpx
import logging
from typing import Optional, Dict, List, Any
from functools import wraps
from datetime import datetime, timedelta
from dataclasses import dataclass
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.search import (
    fetch_invidious,
    fetch_with_inflight,
    client_session,
    get_invidious_instances_from_url,
    INVIDIOUS_SEARCH_LIST_URL,
    INVIDIOUS_VIDEO_LIST_URL,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")


@dataclass
class CacheEntry:
    value: Any
    expires_at: datetime
    
    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at


class SimpleMemoryCache:
    def __init__(self):
        self._cache: Dict[str, CacheEntry] = {}
    
    def get(self, key: str) -> Optional[Any]:
        entry = self._cache.get(key)
        if entry and not entry.is_expired():
            return entry.value
        if entry:
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        self._cache[key] = CacheEntry(
            value=value,
            expires_at=datetime.now() + timedelta(seconds=ttl_seconds)
        )
    
    def clear(self) -> None:
        self._cache.clear()
    
    def cleanup_expired(self) -> None:
        expired_keys = [
            k for k, v in self._cache.items() 
            if v.is_expired()
        ]
        for k in expired_keys:
            del self._cache[k]


_cache = SimpleMemoryCache()


class NumberFormatter:
    
    _format_cache = {}
    
    @staticmethod
    def format_subscriber_count(count_val: Any) -> str:
        cache_key = f"fmt_{str(count_val)[:50]}"
        if cache_key in NumberFormatter._format_cache:
            return NumberFormatter._format_cache[cache_key]
        
        result = NumberFormatter._do_format(count_val)
        NumberFormatter._format_cache[cache_key] = result
        
        if len(NumberFormatter._format_cache) > 1000:
            NumberFormatter._format_cache.clear()
        
        return result
    
    @staticmethod
    def _do_format(val: Any) -> str:
        if val is None or val == "":
            return "非公開"
        
        if isinstance(val, str):
            return val.strip() or "非公開"
        
        if isinstance(val, (int, float)):
            if val <= 0:
                return "非公開"
            if val >= 10_000_000:
                return f"{val / 10_000_000:.1f}千万人".replace(".0", "")
            elif val >= 10_000:
                return f"{val / 10_000:.1f}万人".replace(".0", "")
            return f"{val:,}人"
        
        return "非公開"


class APIFetcher:
    
    TIMEOUTS = {
        "sia": 2.0,
        "invidious": 3.0,
    }
    
    MAX_RETRIES = 2
    RETRY_BACKOFF = 0.5
    
    @staticmethod
    async def fetch_sia_channel(ucid: str, retries: int = 0) -> Optional[Dict]:
        cache_key = f"sia_ch_{ucid}"
        
        cached = _cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return cached
        
        try:
            url = f"https://siatube.com/api/channel/{ucid}"
            resp = await client_session.get(
                url,
                timeout=APIFetcher.TIMEOUTS["sia"]
            )
            
            if resp.status_code == 200:
                data = resp.json()
                if APIFetcher._is_valid_channel_data(data):
                    _cache.set(cache_key, data, ttl_seconds=600)
                    return data
        
        except asyncio.TimeoutError:
            logger.warning(f"Sia timeout for {ucid}")
            if retries < APIFetcher.MAX_RETRIES:
                await asyncio.sleep(APIFetcher.RETRY_BACKOFF * (retries + 1))
                return await APIFetcher.fetch_sia_channel(ucid, retries + 1)
        
        except Exception as e:
            logger.error(f"Sia fetch error for {ucid}: {e}")
        
        return None
    
    @staticmethod
    def _is_valid_channel_data(data: Any) -> bool:
        return isinstance(data, dict) and any(
            key in data for key in ["author", "title", "videos", "name"]
        )


class InvidiousFetcher:
    
    @staticmethod
    async def fetch_channel_data(
        ucid: str,
        sort_by: str = "newest",
        force_instance: Optional[str] = None
    ) -> Dict[str, Any]:
        
        tasks = [
            asyncio.wait_for(
                fetch_invidious(f"/channels/{ucid}", force_instance=force_instance),
                timeout=APIFetcher.TIMEOUTS["invidious"]
            ),
            asyncio.wait_for(
                fetch_invidious(
                    f"/channels/{ucid}/videos",
                    {"sort_by": sort_by},
                    force_instance=force_instance
                ),
                timeout=APIFetcher.TIMEOUTS["invidious"]
            ),
            asyncio.wait_for(
                fetch_invidious(f"/channels/{ucid}/shorts", force_instance=force_instance),
                timeout=APIFetcher.TIMEOUTS["invidious"]
            ),
            asyncio.wait_for(
                fetch_invidious(f"/channels/{ucid}/playlists", force_instance=force_instance),
                timeout=APIFetcher.TIMEOUTS["invidious"]
            ),
            asyncio.wait_for(
                fetch_invidious(f"/channels/{ucid}/community", force_instance=force_instance),
                timeout=APIFetcher.TIMEOUTS["invidious"]
            ),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            "channel": results[0] if not isinstance(results[0], Exception) else {},
            "videos": results[1] if not isinstance(results[1], Exception) else {},
            "shorts": results[2] if not isinstance(results[2], Exception) else {},
            "playlists": results[3] if not isinstance(results[3], Exception) else {},
            "community": results[4] if not isinstance(results[4], Exception) else {},
        }


class DataParser:
    
    @staticmethod
    def extract_list_data(data: Any, key: str = "videos") -> List[Any]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get(key, [])
        return []
    
    @staticmethod
    def extract_channel_name(channel_data: Dict) -> str:
        for key in ("author", "name", "title"):
            if value := channel_data.get(key):
                return value
        return ""
    
    @staticmethod
    def extract_author_icon(channel_data: Dict) -> str:
        if thumbnails := channel_data.get("authorThumbnails"):
            if isinstance(thumbnails, list) and thumbnails:
                return thumbnails[-1].get("url", "")
        
        for key in ("authorIcon", "avatar", "authorAvatar"):
            if value := channel_data.get(key):
                return value
        return ""

    @staticmethod
    def extract_banner_url(channel_data: Dict) -> str:
        if banners := channel_data.get("authorBanners"):
            if isinstance(banners, list) and banners:
                return banners[-1].get("url", "")
        
        for key in ("bannerUrl", "mobileBannerUrl", "banner", "authorBanner"):
            if value := channel_data.get(key):
                return value
        return ""
    
    @staticmethod
    def parse_playlists(playlists_data: Any) -> List[Dict]:
        raw = DataParser.extract_list_data(playlists_data, "playlists")
        result = []
        
        for pl in raw:
            if not isinstance(pl, dict):
                continue
            
            thumb = pl.get("playlistThumbnail") or pl.get("thumbnail", "")
            if thumb and not thumb.startswith("http"):
                thumb = f"https://img.youtube.com/vi/{thumb}/mqdefault.jpg"
            
            result.append({
                "id": pl.get("playlistId") or pl.get("id", ""),
                "title": pl.get("title", ""),
                "video_count": pl.get("videoCount", 0),
                "thumbnail": thumb,
            })
        
        return result
    
    @staticmethod
    def parse_community(
        community_data: Any,
        author_name: str,
        author_icon: str
    ) -> List[Dict]:
        raw = DataParser.extract_list_data(community_data, "comments")
        result = []
        
        for post in raw:
            if not isinstance(post, dict):
                continue
            
            content = (
                post.get("contentHtml")
                or post.get("text")
                or post.get("content", "")
            ).replace("\n", "<br>")
            
            result.append({
                "id": post.get("commentId") or post.get("id", ""),
                "content": content,
                "published_text": post.get("publishedText") or post.get("publishedTime", ""),
                "likes": post.get("likeCount") or (
                    post.get("likes", {}).get("count")
                    if isinstance(post.get("likes"), dict)
                    else 0
                ),
                "author": author_name,
                "author_icon": author_icon,
            })
        
        return result


@router.get("/channel/{ucid}", response_class=HTMLResponse)
async def channel(
    request: Request,
    ucid: str,
    sort_by: str = "newest",
    tab: str = "videos",
    force_instance: str = Query(None),
    api: str = Query(None),
):
    try:
        cache_key = f"ch_full:{ucid}:{sort_by}:{tab}:{force_instance or 'auto'}:{api or 'auto'}"
        
        cached = _cache.get(cache_key)
        if cached:
            logger.info(f"Full cache hit: {ucid}")
            return templates.TemplateResponse("channel.html", cached)
        
        fetched_res = {}
        sia_data = None
        
        if api == "sia" or not api:
            sia_data = await APIFetcher.fetch_sia_channel(ucid)
            if sia_data and api == "sia":
                fetched_res = {
                    "channel": sia_data,
                    "videos": sia_data.get("videos", []),
                    "shorts": sia_data.get("shorts", []),
                    "playlists": sia_data.get("playlists", []),
                    "community": sia_data.get("community", []),
                }
        
        if not fetched_res:
            fetched_res = await InvidiousFetcher.fetch_channel_data(
                ucid,
                sort_by=sort_by,
                force_instance=force_instance
            )
            
            if sia_data and not fetched_res.get("channel"):
                fetched_res["channel"] = sia_data
        
        channel_data = fetched_res.get("channel", {})
        
        author_name = DataParser.extract_channel_name(channel_data)
        author_icon = DataParser.extract_author_icon(channel_data)
        banner_url = DataParser.extract_banner_url(channel_data)
        
        raw_sub_count = (
            channel_data.get("subCountText")
            or channel_data.get("subCount")
            or channel_data.get("subscribers")
            or channel_data.get("subscriberCount")
            or channel_data.get("subscribersCount")
        )
        sub_count = NumberFormatter.format_subscriber_count(raw_sub_count)
        
        final_videos = DataParser.extract_list_data(fetched_res.get("videos", {}), "videos")
        final_shorts = DataParser.extract_list_data(fetched_res.get("shorts", {}), "videos")
        
        playlists = DataParser.parse_playlists(fetched_res.get("playlists", {}))
        
        community = DataParser.parse_community(
            fetched_res.get("community", {}),
            author_name,
            author_icon
        )
        
        context = {
            "request": request,
            "ucid": ucid,
            "author": author_name,
            "author_icon": author_icon,
            "banner_url": banner_url,
            "handle": channel_data.get("handle", ""),
            "video_count": channel_data.get("totalVideos") or channel_data.get("videoCount"),
            "sub_count": sub_count,
            "description": channel_data.get("descriptionHtml") or channel_data.get("description", ""),
            "videos": final_videos,
            "shorts": final_shorts,
            "playlists": playlists,
            "community": community,
            "sort_by": sort_by,
            "tab": tab,
        }
        
        _cache.set(cache_key, context, ttl_seconds=300)
        
        import random
        if random.random() < 0.01:
            _cache.cleanup_expired()
        
        logger.info(f"Channel loaded: {ucid} ({author_name})")
        return templates.TemplateResponse("channel.html", context)
    
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching channel: {ucid}")
        return templates.TemplateResponse("apitimeout.html", {"request": request})
    
    except Exception as e:
        logger.error(f"Channel fetch failed for {ucid}: {e}", exc_info=True)
        fallback_instances = await get_invidious_instances_from_url(INVIDIOUS_SEARCH_LIST_URL)
        return templates.TemplateResponse(
            "apiallerror.html",
            {"request": request, "instances": fallback_instances},
        )


@router.post("/cache/clear")
async def clear_cache():
    _cache.clear()
    logger.info("Cache cleared")
    return {"status": "cleared"}


@router.get("/cache/stats")
async def cache_stats():
    return {
        "cache_size": len(_cache._cache),
        "expired_entries": sum(
            1 for entry in _cache._cache.values()
            if entry.is_expired()
        ),
        "timestamp": datetime.now().isoformat(),
    }
