import requests
import asyncio
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
import json
from datetime import datetime
import random

from app.search import router as search_router
from app.video import router as video_router
from app.stream import router as stream_router
from app.channel import router as channel_router
from app.short import router as short_router

SOURCES = {
    "Education - Toka_Kun_-1": "https://raw.githubusercontent.com/toka-kun/Education/refs/heads/main/keys/key1.json",
    "Education - Toka_Kun_-2": "https://raw.githubusercontent.com/toka-kun/Education/refs/heads/main/keys/key2.json",
    "Education - Toka_Kun_-3": "https://raw.githubusercontent.com/toka-kun/Education/refs/heads/main/keys/key3.json",
    "Education - Toka_Kun_-4": "https://raw.githubusercontent.com/toka-kun/Education/refs/heads/main/keys/key4.json",
    "Education - siawaseok": "https://raw.githubusercontent.com/siawaseok3/wakame/master/video_config.json",
    "Education - wakame": "https://raw.githubusercontent.com/wakame02/wktopu/refs/heads/main/edu.text",
    "Education - woolisbest4520-1": "https://raw.githubusercontent.com/wista-api-project/auto/refs/heads/main/edu/1.txt",
    "Education - woolisbest4520-2": "https://raw.githubusercontent.com/wista-api-project/auto/refs/heads/main/edu/2.txt",
    "Education - woolisbest4520-3": "https://raw.githubusercontent.com/wista-api-project/auto/refs/heads/main/edu/3.txt",
}

def get_education_embed_url(video_id: str, source_name: str) -> str:
    if source_name not in SOURCES:
        raise ValueError(f"未知のソース名です: {source_name}")

    raw_url = SOURCES[source_name]

    response = requests.get(raw_url, timeout=10)
    response.raise_for_status()

    params = ""

    if source_name.startswith("Education - Toka_Kun"):
        data = response.json()
        params = data.get("result", "")
    elif source_name == "Education - siawaseok":
        data = response.json()
        params = data.get("params", "")
    else:
        
        params = response.text.strip()

    embed_url = f"https://www.youtubeeducation.com/embed/{video_id}{params}"
    return embed_url

app = FastAPI()


app.mount("/img", StaticFiles(directory="img"), name="img")

templates = Jinja2Templates(directory="templates")
templates.env.add_extension("jinja2.ext.do")

app.include_router(search_router)
app.include_router(video_router)
app.include_router(stream_router)
app.include_router(channel_router)
app.include_router(short_router)


# --- 404 カスタムエラーハンドラー ---
@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html",
            {"request": request},
            status_code=404
        )
    return JSONResponse(content={"detail": exc.detail}, status_code=exc.status_code)


@app.get("/api/recommended")
async def get_recommended_api(request: Request):
    try:
        search_history_json = request.cookies.get("search_history", "[]")
        search_history = json.loads(search_history_json)
    except:
        search_history = []

    recent_keywords = (
        search_history[-5:]
        if search_history
        else ["ボカロ", "VTuber", "ゲーム実況", "音楽", "ニュース"]
    )

    async def fetch_keyword_results(kw):
        try:
            from app.search import fetch_invidious
            res = await fetch_invidious(
                "/search", {"q": kw, "page": 1, "type": "video"}, list_type="search"
            )
            if isinstance(res, list):
                return [
                    item
                    for item in res
                    if item.get("type") == "video" and item.get("videoId")
                ]
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

    return JSONResponse(content=recommended_videos)

@app.get("/api/channel_info")
async def get_channel_info_api(ucid: str):
    from app.search import fetch_invidious
    try:
        data = await fetch_invidious(f"/channels/{ucid}")
        if isinstance(data, dict):
            author_icon = ""
            if "authorThumbnails" in data and len(data["authorThumbnails"]) > 0:
                author_icon = data["authorThumbnails"][-1].get("url", "")
            return JSONResponse(content={
                "ucid": ucid,
                "name": data.get("author", ucid),
                "icon": author_icon,
                "handle": data.get("authorUrl", ""),
                "description": data.get("description", "")
            })
    except Exception:
        pass
    return JSONResponse(content={"ucid": ucid, "name": ucid, "icon": "", "handle": "", "description": ""})

# --- 追加: 教育用URL取得API ---
@app.get("/api/get_education_url")
def get_education_url_api(video_id: str, source: str):
    try:
        url = get_education_embed_url(video_id, source)
        return {"url": url}
    except Exception as e:
        return {"error": str(e)}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # 初回アクセス判定用フラグをCookieから読み込み
    has_visited = request.cookies.get("welcome_seen", "false") == "true"
    if not has_visited:
        return templates.TemplateResponse(
            "welcome.html",
            {"request": request},
        )
    return templates.TemplateResponse(
        "home.html",
        {"request": request},
    )


@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    try:
        history_list = json.loads(request.cookies.get("history", "[]"))
    except:
        history_list = []
    history_list.reverse()
    return templates.TemplateResponse(
        "history.html", {"request": request, "history": history_list}
    )


@app.get("/history/clear")
async def clear_history():
    response = RedirectResponse(url="/history")
    response.delete_cookie("history")
    return response


@app.get("/suggest")
async def suggest(keyword: str):
    from app.search import fetch_with_inflight
    cache_key = f"suggest:{keyword}"

    async def _do_fetch():
        from app.search import client_session
        try:
            url = "https://suggestqueries.google.com/complete/search"
            params = {
                "client": "firefox",
                "q": keyword,
                "hl": "ja",
                "ie": "utf-8",
                "oe": "utf-8",
            }
            resp = await client_session.get(url, params=params, timeout=2.0)
            
            if resp.status_code == 200:
                text = resp.content.decode("utf-8", errors="replace")
                data = json.loads(text)
                
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return data[1]
        except Exception:
            pass
        return []

    return await fetch_with_inflight(cache_key, _do_fetch, ttl=600.0)


@app.get("/proxy/thumb")
async def proxy_thumb(v: str):
    from app.search import fetch_with_inflight, client_session
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
    from app.search import client_session, get_invidious_instances_from_url, INVIDIOUS_VIDEO_LIST_URL
    
    async def check_instance(instance):
        start_time = asyncio.get_event_loop().time()
        try:
            resp = await client_session.get(
                f"{instance.rstrip('/')}/api/v1/stats", timeout=3.0
            )
            latency = (asyncio.get_event_loop().time() - start_time) * 1000
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "instance": instance,
                    "status": "Online",
                    "latency": f"{int(latency)}ms",
                    "version": data.get("software", {}).get("version", "unknown"),
                    "users": data.get("usage", {}).get("users", {}).get("total", 0),
                }
            return {
                "instance": instance,
                "status": f"Error {resp.status_code}",
                "latency": "-",
                "version": "-",
                "users": "-",
            }
        except:
            return {
                "instance": instance,
                "status": "Offline",
                "latency": "-",
                "version": "-",
                "users": "-",
            }

    video_instances = await get_invidious_instances_from_url(
        INVIDIOUS_VIDEO_LIST_URL
    )
    status_results = await asyncio.gather(
        *(check_instance(inst) for inst in video_instances)
    )
    return templates.TemplateResponse(
        "status.html", {"request": request, "instances": status_results}
    )


@app.get("/subscriptions", response_class=HTMLResponse)
async def subscriptions_page(request: Request):
    return templates.TemplateResponse("subscriptions.html", {"request": request})


@app.get("/bbs", response_class=HTMLResponse)
async def bbs_page(request: Request):
    return templates.TemplateResponse("bbs.html", {"request": request})


@app.get("/downloader", response_class=HTMLResponse)
async def ytdl_page(request: Request):
    return templates.TemplateResponse("bbs.html", {"request": request})


@app.get("/setting", response_class=HTMLResponse)
async def setting_page(request: Request):
    return templates.TemplateResponse("setting.html", {"request": request})

@app.get("/gameview", response_class=HTMLResponse)
async def gameview_page(request: Request):
    return templates.TemplateResponse("gameview.html", {"request": request})

@app.get("/other", response_class=HTMLResponse)
async def other_page(request: Request):
    return templates.TemplateResponse("other.html", {"request": request})

@app.get("/editor", response_class=HTMLResponse)
async def other_page(request: Request):
    return templates.TemplateResponse("editor.html", {"request": request})

@app.get("/other-sites", response_class=HTMLResponse)
async def other_page(request: Request):
    return templates.TemplateResponse("other-sites.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def other_page(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})
    
@app.get("/help", response_class=HTMLResponse)
async def help_page(request: Request):
    return templates.TemplateResponse("help.html", {"request": request})

@app.get("/embed/{video_id}", response_class=HTMLResponse)
async def embed_player(request: Request, video_id: str):
    return templates.TemplateResponse("embed.html", {"request": request})

@app.get("/embed", response_class=HTMLResponse)
async def embed_player_query(request: Request, v: str = Query(None)):
    return templates.TemplateResponse("embed.html", {"request": request})

@app.get("/select", response_class=HTMLResponse)
async def select_video_page(request: Request, v: str = ""):
    return templates.TemplateResponse(
        "select.html",
        {
            "request": request,
            "video_id": v
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
