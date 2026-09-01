import asyncio
import random
import time
from typing import Optional, Dict, List, Any
from fastapi import APIRouter
from fastapi.templating import Jinja2Templates
from dataclasses import dataclass
from enum import Enum

from app.search import (
    fetch_invidious,
    fetch_with_inflight,
    client_session,
    no_redirect_client,
    get_invidious_instances_from_url,
    INVIDIOUS_VIDEO_LIST_URL,
    PIPED_INSTANCES,
    SENNIN_API_BASE,
    _get_rapid_api_keys,
    RAPID_API_HOST,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")

RAPID_API_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"


class StreamProvider(Enum):
    SIA = "sia"
    PIPED = "piped"
    RAPIDAPI = "rapidapi"
    ZERNIO = "zernio"
    INVIDIOUS = "invidious"
    SENNIN = "sennin"


@dataclass
class ProviderConfig:
    name: str
    base_url: str
    timeout: float
    weight: int
    description: str
    handler: Optional[callable] = None


@dataclass
class StreamUrl:
    url: str
    resolution: str
    format: str
    audio_url: str = ""


@dataclass
class StreamResult:
    stream_urls: List[StreamUrl]
    video_urls: List[str]
    stream_api_used: str
    title: Optional[str] = None
    author: Optional[str] = None
    author_id: Optional[str] = None
    description_html: Optional[str] = None
    view_count: int = 0
    like_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "streamUrls": [
                {
                    "url": s.url,
                    "resolution": s.resolution,
                    "format": s.format,
                    "audioUrl": s.audio_url,
                }
                for s in self.stream_urls
            ],
            "videoUrls": self.video_urls,
            "stream_api_used": self.stream_api_used,
            "title": self.title,
            "author": self.author,
            "authorId": self.author_id,
            "descriptionHtml": self.description_html,
            "viewCount": self.view_count,
            "likeCount": self.like_count,
        }


STREAM_API_CONFIG = {
    StreamProvider.SIA: ProviderConfig(
        name="sia",
        base_url="https://siatube.com/api/stream",
        timeout=2.5,
        weight=100,
        description="Sia Tube API",
    ),
    StreamProvider.PIPED: ProviderConfig(
        name="piped",
        base_url="piped_instances",
        timeout=2.5,
        weight=90,
        description="Piped インスタンス",
    ),
    StreamProvider.RAPIDAPI: ProviderConfig(
        name="rapidapi",
        base_url=f"https://{RAPID_API_HOST}/dl",
        timeout=2.5,
        weight=80,
        description="RapidAPI YouTubeStreamer",
    ),
    StreamProvider.ZERNIO: ProviderConfig(
        name="zernio",
        base_url="https://getlate.dev/api/tools/youtube-live-downloader",
        timeout=3.0,
        weight=70,
        description="Zernio ダウンローダ",
    ),
    StreamProvider.INVIDIOUS: ProviderConfig(
        name="invidious",
        base_url="invidious_instances",
        timeout=4.0,
        weight=60,
        description="Invidious API",
    ),
    StreamProvider.SENNIN: ProviderConfig(
        name="sennin",
        base_url="https://discerning-adventure-production-ebfc.up.railway.app/api/stream",
        timeout=3.5,
        weight=50,
        description="Sennin API",
    ),
}


INFO_API_CONFIG = {
    StreamProvider.INVIDIOUS: ProviderConfig(
        name="invidious",
        base_url="invidious_instances",
        timeout=4.0,
        weight=100,
        description="Invidious API",
    ),
    StreamProvider.SIA: ProviderConfig(
        name="sia",
        base_url="https://siatube.com/api/video",
        timeout=3.0,
        weight=90,
        description="Sia Tube API",
    ),
    StreamProvider.SENNIN: ProviderConfig(
        name="sennin",
        base_url="https://discerning-adventure-production-ebfc.up.railway.app/api/video",
        timeout=4.0,
        weight=80,
        description="Sennin API",
    ),
}


def _normalize_stream_urls(
    formats: List[Dict[str, Any]],
    format_type: str = "mp4/mixed",
    audio_url: str = ""
) -> List[StreamUrl]:
    urls = []

    for item in formats:
        url = item.get("url")
        if not url:
            continue

        resolution = item.get(
            "quality",
            item.get("qualityLabel", item.get("quality_label", "Auto"))
        )

        urls.append(StreamUrl(
            url=url,
            resolution=str(resolution),
            format=format_type,
            audio_url=audio_url,
        ))

    return urls


async def _fetch_with_timeout(
    url: str,
    timeout: float,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    use_no_redirect: bool = False,
) -> Optional[Dict[str, Any]]:
    try:
        session = no_redirect_client if use_no_redirect else client_session

        resp = await asyncio.wait_for(
            session.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            ),
            timeout=timeout + 0.5,
        )

        if resp.status_code == 200:
            return resp.json()

        return None

    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    except Exception:
        return None


async def fetch_sia_stream(v: str) -> StreamResult:
    try:
        url = f"https://siatube.com/api/stream/{v}"

        data = await _fetch_with_timeout(
            url,
            timeout=2.5
        )

        if not data:
            raise Exception("Sia API response empty")

        stream_urls = []
        video_urls = []

        streams = data.get("streams", {})

        if not isinstance(streams, dict):
            streams = {}

        muxed = (
            streams.get("muxed", [])
            or data.get("muxed", [])
            or data.get("formats", [])
            or []
        )

        if isinstance(muxed, list):
            muxed_urls = _normalize_stream_urls(
                muxed,
                "mp4/mixed"
            )

            stream_urls.extend(muxed_urls)

            video_urls.extend(
                s.url
                for s in muxed_urls
                if s.url
            )

        hls_data = data.get("m3u8", {})

        hls_url = (
            data.get("hls")
            or data.get("m3u8")
            or data.get("manifestUrl")
        )

        if isinstance(hls_data, dict):
            hls_list = hls_data.get("list", [])

            if isinstance(hls_list, list):
                for item in hls_list:
                    stream_url = item.get("streamUrl")

                    if not stream_url:
                        continue

                    quality = item.get("height") or item.get("formatNote") or "HLS"

                    if stream_url not in video_urls:
                        video_urls.append(stream_url)

                        stream_urls.append(
                            StreamUrl(
                                url=stream_url,
                                resolution=(
                                    f"{quality}p"
                                    if isinstance(quality, int)
                                    else str(quality)
                                ),
                                format="application/x-mpegURL",
                            )
                        )

        elif isinstance(hls_url, str) and hls_url:
            if hls_url not in video_urls:
                video_urls.append(hls_url)

                stream_urls.append(
                    StreamUrl(
                        url=hls_url,
                        resolution="HLS/Live",
                        format="application/x-mpegURL",
                    )
                )

        audio_only = (
            streams.get("audioOnly", [])
            or data.get("audioOnly", [])
            or []
        )

        audio_url = ""

        if isinstance(audio_only, list) and audio_only:
            for item in audio_only:
                candidate = (
                    item.get("streamUrl")
                    or item.get("url")
                    or ""
                )

                if candidate:
                    audio_url = candidate
                    break

        video_only = (
            streams.get("videoOnly", [])
            or data.get("videoOnly", [])
            or []
        )

        if isinstance(video_only, list):
            for item in video_only:
                stream_url = (
                    item.get("streamUrl")
                    or item.get("url")
                )

                if not stream_url:
                    continue

                quality = (
                    item.get("formatNote")
                    or item.get("quality")
                    or item.get("qualityLabel")
                    or "Auto"
                )

                ext = item.get("ext", "mp4")

                if ext == "webm":
                    format_type = "webm/videoOnly"
                else:
                    format_type = "mp4/videoOnly"

                stream_urls.append(
                    StreamUrl(
                        url=stream_url,
                        resolution=str(quality),
                        format=format_type,
                        audio_url=audio_url,
                    )
                )

        if not video_urls and stream_urls:
            video_urls = [
                s.url
                for s in stream_urls
                if s.url
            ]

        if not stream_urls:
            raise Exception("No streams found")

        return StreamResult(
            stream_urls=stream_urls,
            video_urls=video_urls,
            stream_api_used="sia",
        )

    except Exception as e:
        raise Exception(f"Sia failed: {str(e)}")


async def fetch_piped_stream(v: str) -> StreamResult:
    instances = list(PIPED_INSTANCES)
    random.shuffle(instances)

    last_error = None

    for instance in instances:
        try:
            url = f"{instance.rstrip('/')}/streams/{v}"

            data = await _fetch_with_timeout(
                url,
                timeout=2.5
            )

            if not data:
                continue

            stream_urls = []
            video_urls = []

            audio_url = ""

            for item in data.get("audioStreams", []):
                mime_type = item.get("mimeType", "")

                if mime_type.startswith("audio"):
                    audio_url = item.get("url", "")
                    break

            video_streams = data.get("videoStreams", [])

            combined_url = None
            hls_url = data.get("hls") or None
            fallback_url = None

            for item in video_streams:
                fmt = item.get("format", "").upper()

                if (
                    not item.get("videoOnly", True)
                    and fmt in ("MP4", "MPEG_4", "MPEG4")
                ):
                    combined_url = item.get("url")
                    break

            if not combined_url:
                for item in video_streams:
                    fmt = item.get("format", "").upper()

                    if (
                        not item.get("videoOnly", True)
                        and fmt not in ("HLS", "")
                    ):
                        combined_url = item.get("url")
                        break

            if not combined_url and not hls_url:
                for item in video_streams:
                    if not item.get("videoOnly", True):
                        combined_url = item.get("url")
                        break

            for item in video_streams:
                if (
                    "360" in str(item.get("quality", ""))
                    and item.get("videoOnly", True)
                ):
                    fallback_url = item.get("url")
                    break

            if combined_url:
                quality = ""

                for item in video_streams:
                    if item.get("url") == combined_url:
                        quality = item.get("quality", "")
                        break

                stream_urls.append(
                    StreamUrl(
                        url=combined_url,
                        resolution=quality,
                        format="mp4/mixed",
                    )
                )

                video_urls.append(combined_url)

            if hls_url:
                if hls_url not in video_urls:
                    stream_urls.append(
                        StreamUrl(
                            url=hls_url,
                            resolution="HLS/Live",
                            format="application/x-mpegURL",
                        )
                    )

                    video_urls.append(hls_url)

            for item in video_streams:
                url_str = item.get("url")

                if not url_str:
                    continue

                if not item.get("videoOnly", False):
                    continue

                quality = item.get("quality", "")

                stream_urls.append(
                    StreamUrl(
                        url=url_str,
                        resolution=quality,
                        format="webm/videoOnly",
                        audio_url=audio_url,
                    )
                )

            if not stream_urls and fallback_url:
                stream_urls.append(
                    StreamUrl(
                        url=fallback_url,
                        resolution="360p",
                        format="webm/videoOnly",
                        audio_url=audio_url,
                    )
                )

                video_urls.append(fallback_url)

            if not video_urls:
                video_urls = [
                    s.url
                    for s in stream_urls
                    if s.url
                ]

            if not stream_urls:
                continue

            return StreamResult(
                stream_urls=stream_urls,
                video_urls=video_urls,
                title=data.get("title"),
                author=data.get("uploader"),
                author_id=data.get(
                    "uploaderUrl",
                    ""
                ).replace("/channel/", ""),
                description_html=data.get(
                    "description",
                    ""
                ).replace("\n", "<br>"),
                view_count=data.get("views", 0),
                like_count=data.get("likes", 0),
                stream_api_used="piped",
            )

        except Exception as e:
            last_error = e
            continue

    raise Exception(
        f"Piped failed: "
        f"{str(last_error) if last_error else 'All instances failed'}"
    )


async def fetch_rapidapi_stream(v: str) -> StreamResult:
    keys = _get_rapid_api_keys()

    random.shuffle(keys)

    last_error = None

    for key in keys:
        try:
            url = f"https://{RAPID_API_HOST}/dl"

            headers = {
                "X-RapidAPI-Key": key,
                "X-RapidAPI-Host": RAPID_API_HOST,
            }

            data = await _fetch_with_timeout(
                url,
                headers=headers,
                params={"id": v},
                timeout=2.5,
            )

            if not data:
                continue

            status = data.get("status")

            if (
                status not in ("OK", None)
                and "formats" not in data
                and "adaptiveFormats" not in data
            ):
                last_error = Exception(
                    f"RapidAPI status: {status}"
                )
                continue

            formats = data.get("formats", [])
            adaptive_formats = data.get(
                "adaptiveFormats",
                []
            )

            stream_urls = []
            video_urls = []

            muxed_urls = _normalize_stream_urls(
                formats,
                "mp4/mixed"
            )

            stream_urls.extend(muxed_urls)

            video_urls.extend(
                s.url
                for s in muxed_urls
                if s.url
            )

            audio_url = ""

            for item in adaptive_formats:
                mime = item.get("mimeType", "")

                if mime.startswith("audio/"):
                    audio_url = item.get("url", "")

                    if audio_url:
                        break

            adaptive_urls = _normalize_stream_urls(
                adaptive_formats,
                "webm/videoOnly",
                audio_url,
            )

            stream_urls.extend(adaptive_urls)

            if not video_urls:
                video_urls = [
                    s.url
                    for s in stream_urls
                    if s.url
                ]

            if not stream_urls:
                continue

            return StreamResult(
                stream_urls=stream_urls,
                video_urls=video_urls,
                title=data.get("title"),
                stream_api_used="rapidapi",
            )

        except Exception as e:
            last_error = e
            continue

    raise Exception(
        f"RapidAPI failed: "
        f"{str(last_error) if last_error else 'All keys failed'}"
    )


async def fetch_zernio_stream(v: str) -> StreamResult:
    try:
        target_url = f"https://www.youtube.com/watch?v={v}"

        format_ids = [
            (2, "360p"),
            (1, "240p"),
            (3, "480p"),
            (4, "720p"),
            (5, "1080p"),
            (6, "1080p"),
            (7, "1440p"),
            (8, "144p"),
        ]

        last_error = None

        for format_id, resolution in format_ids:
            try:
                url = (
                    "https://getlate.dev/api/tools/"
                    "youtube-live-downloader"
                    f"?url={target_url}"
                    f"&formatId={format_id}"
                )

                resp = await asyncio.wait_for(
                    no_redirect_client.get(
                        url,
                        timeout=3.0
                    ),
                    timeout=3.5
                )

                if resp.status_code in (
                    301,
                    302,
                    303,
                    307,
                    308,
                ):
                    location = (
                        resp.headers.get("location")
                        or resp.headers.get("Location")
                    )

                    if location:
                        return StreamResult(
                            stream_urls=[
                                StreamUrl(
                                    url=location,
                                    resolution=resolution,
                                    format="mp4/mixed",
                                )
                            ],
                            video_urls=[location],
                            stream_api_used="zernio",
                        )

                last_error = Exception(
                    f"No redirect received: "
                    f"status={resp.status_code}"
                )

            except Exception as e:
                last_error = e
                continue

        raise Exception(
            str(last_error)
            if last_error
            else "No redirect received"
        )

    except Exception as e:
        raise Exception(f"Zernio failed: {str(e)}")


async def fetch_sennin_stream(v: str) -> StreamResult:
    try:
        url = f"{SENNIN_API_BASE}/api/stream/{v}"

        data = await _fetch_with_timeout(
            url,
            timeout=3.5
        )

        if not data:
            raise Exception("Sennin API response empty")

        formats = data.get("formats", [])

        stream_urls = _normalize_stream_urls(
            formats,
            "mp4/mixed"
        )

        video_urls = [
            s.url
            for s in stream_urls
            if s.url
        ]

        if not stream_urls:
            raise Exception("No streams found")

        return StreamResult(
            stream_urls=stream_urls,
            video_urls=video_urls,
            stream_api_used="sennin",
        )

    except Exception as e:
        raise Exception(f"Sennin failed: {str(e)}")


async def fetch_sia_comments(v: str) -> Dict[str, Any]:
    try:
        url = f"https://siatube.com/api/comments?videoId={v}"

        data = await _fetch_with_timeout(
            url,
            timeout=3.5
        )

        if (
            data
            and isinstance(data, dict)
            and "comments" in data
        ):
            return data

        raise Exception("Invalid response format")

    except Exception as e:
        raise Exception(
            f"Sia comments failed: {str(e)}"
        )


async def fetch_sennin_comments(
    v: str,
    sort: str = "top"
) -> Dict[str, Any]:
    try:
        url = f"{SENNIN_API_BASE}/api/comments"

        params = {
            "videoId": v,
            "sort": sort
        }

        data = await _fetch_with_timeout(
            url,
            params=params,
            timeout=4.0
        )

        if (
            data
            and data.get("success") is True
        ):
            return data

        raise Exception("Invalid response")

    except Exception as e:
        raise Exception(
            f"Sennin comments failed: {str(e)}"
        )


async def _fetch_stream_with_fallback(
    v: str,
    api: Optional[str] = None,
    force_instance: Optional[str] = None,
) -> Optional[StreamResult]:

    from app.video import (
        extract_invidious_streams,
        fetch_video_info_invidious_robust
    )

    provider_map = {
        "sia": fetch_sia_stream,
        "piped": fetch_piped_stream,
        "rapidapi": fetch_rapidapi_stream,
        "zernio": fetch_zernio_stream,
        "sennin": fetch_sennin_stream,
    }

    if api and api in provider_map:
        try:
            return await provider_map[api](v)
        except Exception:
            pass

    try:
        v_data = await fetch_video_info_invidious_robust(
            v,
            force_instance=force_instance
        )

        res_dict = extract_invidious_streams(v_data)

        res_dict["stream_api_used"] = "invidious_fallback"

        return StreamResult(
            stream_urls=[
                StreamUrl(
                    url=s["url"],
                    resolution=s.get(
                        "resolution",
                        "Auto"
                    ),
                    format=s.get(
                        "format",
                        "mp4/mixed"
                    ),
                    audio_url=s.get(
                        "audioUrl",
                        ""
                    ),
                )
                for s in res_dict.get(
                    "streamUrls",
                    []
                )
            ],
            video_urls=res_dict.get(
                "videoUrls",
                []
            ),
            stream_api_used=res_dict.get(
                "stream_api_used",
                "invidious_fallback"
            ),
            title=res_dict.get("title"),
            author=res_dict.get("author"),
            author_id=res_dict.get("authorId"),
            description_html=res_dict.get(
                "descriptionHtml"
            ),
            view_count=res_dict.get(
                "viewCount",
                0
            ),
            like_count=res_dict.get(
                "likeCount",
                0
            ),
        )

    except Exception:
        pass

    return None


async def fetch_fastest_stream_urls(
    v: str,
    api: Optional[str] = None,
    force_instance: Optional[str] = None,
    timeout: float = 2.5,
) -> Optional[Dict[str, Any]]:

    from app.video import (
        extract_invidious_streams,
        fetch_video_info_invidious_robust
    )

    cache_key = (
        f"fastest_stream:"
        f"{v}:"
        f"{api or ''}:"
        f"{force_instance or ''}"
    )

    async def _do_fetch() -> Optional[Dict[str, Any]]:

        if api:
            try:
                result = await _fetch_stream_with_fallback(
                    v,
                    api=api,
                    force_instance=force_instance
                )

                if result:
                    return result.to_dict()

            except Exception:
                pass

        # === 最優先取得フェーズ（Invidious ＆ RapidAPI を並列実行） ===

        async def _fetch_invidious_wrapper():
            v_data = await fetch_video_info_invidious_robust(
                v,
                force_instance=force_instance
            )

            res_dict = extract_invidious_streams(
                v_data
            )

            if res_dict.get("videoUrls"):
                res_dict["stream_api_used"] = "invidious"
                return res_dict

            return None

        async def _fetch_rapidapi_wrapper():
            res = await fetch_rapidapi_stream(v)

            if res and res.video_urls:
                return res.to_dict()

            return None

        priority_tasks = [
            asyncio.create_task(
                _fetch_invidious_wrapper()
            ),
            asyncio.create_task(
                _fetch_rapidapi_wrapper()
            ),
        ]

        done_priority, pending_priority = await asyncio.wait(
            priority_tasks,
            timeout=timeout + 1.0,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done_priority:
            try:
                result = task.result()

                if result:
                    for t in pending_priority:
                        t.cancel()

                    return result

            except Exception:
                continue

        for task in pending_priority:
            try:
                result = await task

                if result:
                    return result

            except Exception:
                continue

        # === 1枚目のstream取得ロジックに対応したフォールバック・レース処理 ===

        providers = [
            ("sia", fetch_sia_stream),
            ("piped", fetch_piped_stream),
            ("rapidapi", fetch_rapidapi_stream),
            ("zernio", fetch_zernio_stream),
        ]

        tasks = {
            name: asyncio.create_task(
                fetch_fn(v)
            )
            for name, fetch_fn in providers
        }

        done, pending = await asyncio.wait(
            tasks.values(),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for name, task in tasks.items():
            if task in done:
                try:
                    result = task.result()

                    if result and result.video_urls:
                        for t in pending:
                            t.cancel()

                        return result.to_dict()

                except Exception:
                    continue

        for task in pending:
            try:
                result = await task

                if result and result.video_urls:
                    return result.to_dict()

            except Exception:
                continue

        # === Invidious fallback ===

        try:
            result = await _fetch_stream_with_fallback(
                v,
                force_instance=force_instance
            )

            if result and result.video_urls:
                return result.to_dict()

        except Exception:
            pass

        return None

    return await fetch_with_inflight(
        cache_key,
        _do_fetch,
        ttl=120.0
    )


async def fetch_comments(
    v: str,
    force_instance: Optional[str] = None,
    api: Optional[str] = None,
) -> Optional[Dict[str, Any]]:

    cache_key = (
        f"comments:"
        f"{v}:"
        f"{force_instance or ''}:"
        f"{api or ''}"
    )

    async def _do_fetch() -> Optional[Dict[str, Any]]:

        if api == "sia":
            try:
                return await fetch_sia_comments(v)
            except Exception:
                pass

        elif api == "sennin":
            try:
                return await fetch_sennin_comments(v)
            except Exception:
                pass

        elif api == "invidious":
            try:
                return await fetch_invidious(
                    f"/comments/{v}",
                    force_instance=force_instance,
                    list_type="video"
                )
            except Exception:
                pass

        tasks = {
            "invidious": asyncio.create_task(
                fetch_invidious(
                    f"/comments/{v}",
                    force_instance=force_instance,
                    list_type="video",
                )
            ),
            "sia": asyncio.create_task(
                fetch_sia_comments(v)
            ),
            "sennin": asyncio.create_task(
                fetch_sennin_comments(v)
            ),
        }

        done, pending = await asyncio.wait(
            tasks.values(),
            timeout=3.0,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for name, task in tasks.items():
            if task in done:
                try:
                    result = task.result()

                    if result:
                        for t in pending:
                            t.cancel()

                        return result

                except Exception:
                    continue

        for task in pending:
            task.cancel()

        return None

    return await fetch_with_inflight(
        cache_key,
        _do_fetch,
        ttl=180.0
                    )
