"""
imhentai-manager  ―  FastAPI Web UI
"""
import asyncio
import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ── project root に移動してモジュールを読み込む ──
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from imhentai import (
    init_db,
    search_db,
    search_site,
    search_category_site,
    download_gallery,
    scrape_category_all_parallel,
    get_db_stats,
    get_download_history,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("app")

app = FastAPI(title="imhentai-manager")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")
templates = Jinja2Templates(directory=ROOT / "templates")

# ── DB 初期化 ──
init_db()

# ── ダウンロードジョブキュー (gallery_id → status string) ──
_dl_jobs: dict[str, dict] = {}
_dl_lock = threading.Lock()


# ─────────────────────────────────────────────
# Pages
# ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ─────────────────────────────────────────────
# API: Search
# ─────────────────────────────────────────────

@app.get("/api/search/site")
async def api_search_site(q: str = "", page: int = 1):
    """imhentaiサイト検索 → ギャラリー一覧"""
    if not q.strip():
        return {"results": [], "query": q, "page": page}
    results = await asyncio.to_thread(search_site, q, page)
    return {"results": results, "query": q, "page": page}


@app.get("/api/search/db")
async def api_search_db(q: str = ""):
    """DB内fuzzy検索 (tags / artists / groups / characters / parodies)"""
    if not q.strip():
        return {"results": []}
    results = await asyncio.to_thread(search_db, q)
    return {"results": results}


@app.get("/api/search/category")
async def api_search_category(cat: str, slug: str, page: int = 1):
    """カテゴリ内ギャラリー一覧 (DB結果クリック時)"""
    results = await asyncio.to_thread(search_category_site, cat, slug, page)
    return {"results": results, "cat": cat, "slug": slug, "page": page}


# ─────────────────────────────────────────────
# API: Download
# ─────────────────────────────────────────────

@app.post("/api/download")
async def api_download(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    gid = str(body.get("gallery_id", "")).strip()
    fmt = body.get("fmt", "ZIP").upper()
    workers = int(body.get("workers", 8))
    skip = bool(body.get("skip_if_exists", True))

    if not gid:
        return JSONResponse({"error": "gallery_id required"}, status_code=400)

    with _dl_lock:
        if gid in _dl_jobs and _dl_jobs[gid].get("status") == "running":
            return JSONResponse({"error": "already running", "gallery_id": gid}, status_code=409)
        _dl_jobs[gid] = {"status": "running", "log": [], "result": None}

    def run():
        try:
            result = download_gallery(int(gid), fmt=fmt, workers=workers, skip_if_exists=skip)
            with _dl_lock:
                _dl_jobs[gid]["status"] = "done"
                _dl_jobs[gid]["result"] = result
        except Exception as e:
            with _dl_lock:
                _dl_jobs[gid]["status"] = "error"
                _dl_jobs[gid]["result"] = str(e)

    background_tasks.add_task(asyncio.to_thread, run)
    return {"gallery_id": gid, "status": "started"}


@app.get("/api/download/status/{gid}")
async def api_dl_status(gid: str):
    with _dl_lock:
        job = _dl_jobs.get(gid)
    if not job:
        return {"gallery_id": gid, "status": "not_found"}
    return {"gallery_id": gid, **job}


# SSE: ダウンロード進捗ストリーム
@app.get("/api/download/stream/{gid}")
async def api_dl_stream(gid: str):
    async def event_gen():
        while True:
            with _dl_lock:
                job = _dl_jobs.get(gid)
            if not job:
                yield f"data: {json.dumps({'status':'not_found'})}\n\n"
                break
            yield f"data: {json.dumps({'status': job['status'], 'result': job.get('result')})}\n\n"
            if job["status"] in ("done", "error"):
                break
            await asyncio.sleep(1)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# ─────────────────────────────────────────────
# API: DB Management
# ─────────────────────────────────────────────

@app.get("/api/db/stats")
async def api_db_stats():
    stats = await asyncio.to_thread(get_db_stats)
    return stats


@app.post("/api/db/scrape")
async def api_db_scrape(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    cats = body.get("categories", ["artists", "tags", "groups", "parodies", "characters"])
    workers = int(body.get("workers", 8))

    scrape_id = "scrape_" + "_".join(cats)
    with _dl_lock:
        _dl_jobs[scrape_id] = {"status": "running", "result": None}

    def run_scrape():
        results = {}
        for cat in cats:
            try:
                n = scrape_category_all_parallel(cat, workers=workers)
                results[cat] = n
            except Exception as e:
                results[cat] = f"ERROR: {e}"
        with _dl_lock:
            _dl_jobs[scrape_id]["status"] = "done"
            _dl_jobs[scrape_id]["result"] = results

    background_tasks.add_task(asyncio.to_thread, run_scrape)
    return {"scrape_id": scrape_id, "status": "started"}


@app.get("/api/db/scrape/status/{scrape_id}")
async def api_scrape_status(scrape_id: str):
    with _dl_lock:
        job = _dl_jobs.get(scrape_id)
    if not job:
        return {"scrape_id": scrape_id, "status": "not_found"}
    return {"scrape_id": scrape_id, **job}


@app.get("/api/db/export")
async def api_db_export():
    """DB全件JSON export"""
    import sqlite3
    db_path = ROOT / "data" / "imhentai.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    data = {}
    for tbl in ("galleries", "artists", "tags", "groups", "parodies", "characters"):
        try:
            rows = conn.execute(f"SELECT * FROM {tbl}").fetchall()
            data[tbl] = [dict(r) for r in rows]
        except Exception:
            data[tbl] = []
    conn.close()

    export_path = Path("/tmp/imhentai_db_export.json")
    export_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    return FileResponse(str(export_path), filename="imhentai_db_export.json", media_type="application/json")


# ─────────────────────────────────────────────
# API: History
# ─────────────────────────────────────────────

@app.get("/api/history")
async def api_history(limit: int = 100):
    rows = await asyncio.to_thread(get_download_history, limit)
    return {"history": rows}


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
