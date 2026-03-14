"""
imhentai-manager  ―  FastAPI Web UI
"""
import asyncio
import json
import logging
import sys
import threading
import collections
from pathlib import Path

import uvicorn
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

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
    DB_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("app")

app = FastAPI(title="imhentai-manager")
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")
templates = Jinja2Templates(directory=ROOT / "templates")

init_db()

# ─────────────────────────────────────────────
# Download Queue
# ─────────────────────────────────────────────
# jobs: gid -> {status, result, fmt, workers, queued_at}
_jobs: dict[str, dict] = {}
_queue: collections.deque = collections.deque()   # queued gids
_queue_lock = threading.Lock()
_MAX_CONCURRENT = 3   # 同時実行数


def _queue_worker():
    """バックグラウンドスレッド: キューからジョブを取り出して実行"""
    import time
    running: set = set()

    while True:
        with _queue_lock:
            # 完了したものをrunningから除去
            done = {g for g in running if _jobs.get(g, {}).get("status") not in ("running", "queued")}
            running -= done

            # 空き枠があればキューから取り出す
            while len(running) < _MAX_CONCURRENT and _queue:
                gid = _queue.popleft()
                if _jobs.get(gid, {}).get("status") == "queued":
                    _jobs[gid]["status"] = "running"
                    running.add(gid)
                    t = threading.Thread(target=_run_download, args=(gid,), daemon=True)
                    t.start()

        time.sleep(0.5)


def _run_download(gid: str):
    job = _jobs.get(gid, {})
    fmt = job.get("fmt", "ZIP")
    workers = job.get("workers", 8)
    skip = job.get("skip_if_exists", True)
    try:
        result = download_gallery(int(gid), fmt=fmt, workers=workers, skip_if_exists=skip)
        with _queue_lock:
            _jobs[gid]["status"] = "done"
            _jobs[gid]["result"] = result
    except Exception as e:
        with _queue_lock:
            _jobs[gid]["status"] = "error"
            _jobs[gid]["result"] = str(e)


# キューワーカー起動
threading.Thread(target=_queue_worker, daemon=True).start()


def enqueue(gid: str, fmt: str = "ZIP", workers: int = 8, skip: bool = True) -> str:
    """キューにジョブを追加。返値: 'queued' | 'already_running' | 'already_done'"""
    with _queue_lock:
        existing = _jobs.get(gid, {}).get("status")
        if existing == "running":
            return "already_running"
        if existing == "queued":
            return "already_queued"
        _jobs[gid] = {
            "status": "queued",
            "fmt": fmt,
            "workers": workers,
            "skip_if_exists": skip,
            "result": None,
        }
        _queue.append(gid)
    return "queued"


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
    if not q.strip():
        return {"results": [], "query": q, "page": page}
    results = await asyncio.to_thread(search_site, q, page)
    return {"results": results, "query": q, "page": page}


@app.get("/api/search/db")
async def api_search_db(q: str = ""):
    if not q.strip():
        return {"results": []}
    results = await asyncio.to_thread(search_db, q)
    return {"results": results}


@app.get("/api/search/category")
async def api_search_category(cat: str, slug: str, page: int = 1):
    results = await asyncio.to_thread(search_category_site, cat, slug, page)
    return {"results": results, "cat": cat, "slug": slug, "page": page}


# ─────────────────────────────────────────────
# API: Download Queue
# ─────────────────────────────────────────────

@app.post("/api/download")
async def api_download(request: Request):
    body = await request.json()
    gid = str(body.get("gallery_id", "")).strip()
    fmt = body.get("fmt", "ZIP").upper()
    workers = int(body.get("workers", 8))
    skip = bool(body.get("skip_if_exists", True))

    if not gid:
        return JSONResponse({"error": "gallery_id required"}, status_code=400)

    status = enqueue(gid, fmt=fmt, workers=workers, skip=skip)
    return {"gallery_id": gid, "status": status}


@app.get("/api/download/status/{gid}")
async def api_dl_status(gid: str):
    with _queue_lock:
        job = dict(_jobs.get(gid, {}))
    if not job:
        return {"gallery_id": gid, "status": "not_found"}
    return {"gallery_id": gid, **job}


@app.get("/api/queue")
async def api_queue():
    """キュー全体の状態"""
    with _queue_lock:
        jobs_copy = dict(_jobs)
        queue_list = list(_queue)
    items = []
    for gid, job in jobs_copy.items():
        items.append({"gallery_id": gid, **job})
    items.sort(key=lambda x: ("done error".find(x.get("status","")) , x["gallery_id"]))
    return {
        "queue": items,
        "queued": queue_list,
        "total": len(items),
        "running": sum(1 for j in jobs_copy.values() if j.get("status") == "running"),
        "done": sum(1 for j in jobs_copy.values() if j.get("status") == "done"),
    }


# SSE: キュー全体の進捗ストリーム
@app.get("/api/queue/stream")
async def api_queue_stream():
    async def gen():
        last = None
        while True:
            with _queue_lock:
                jobs_copy = {k: dict(v) for k, v in _jobs.items()}
                q = list(_queue)
            summary = {
                "jobs": jobs_copy,
                "queue": q,
            }
            serialized = json.dumps(summary)
            if serialized != last:
                last = serialized
                yield f"data: {serialized}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")


# サーバー上のDL済みファイルをブラウザへ配信
@app.get("/api/download/file/{gid}")
async def api_dl_file(gid: str):
    from pathlib import Path as P
    for ext in ("zip", "pdf"):
        p = ROOT / "downloads" / f"{gid}.{ext}"
        if p.exists():
            return FileResponse(
                str(p),
                filename=p.name,
                media_type="application/octet-stream",
            )
    return JSONResponse({"error": "file not found"}, status_code=404)


# ─────────────────────────────────────────────
# API: DB Management
# ─────────────────────────────────────────────

@app.get("/api/db/stats")
async def api_db_stats():
    return await asyncio.to_thread(get_db_stats)


@app.post("/api/db/scrape")
async def api_db_scrape(request: Request):
    body = await request.json()
    cats = body.get("categories", ["artists", "tags", "groups", "parodies", "characters"])
    workers = int(body.get("workers", 8))

    scrape_id = "scrape_" + "_".join(cats)
    with _queue_lock:
        _jobs[scrape_id] = {"status": "running", "result": None}

    def run_scrape():
        results = {}
        for cat in cats:
            try:
                n = scrape_category_all_parallel(cat, workers=workers)
                results[cat] = n
            except Exception as e:
                results[cat] = f"ERROR: {e}"
        with _queue_lock:
            _jobs[scrape_id]["status"] = "done"
            _jobs[scrape_id]["result"] = results

    threading.Thread(target=run_scrape, daemon=True).start()
    return {"scrape_id": scrape_id, "status": "started"}


@app.get("/api/db/scrape/status/{scrape_id}")
async def api_scrape_status(scrape_id: str):
    with _queue_lock:
        job = dict(_jobs.get(scrape_id, {}))
    if not job:
        return {"scrape_id": scrape_id, "status": "not_found"}
    return {"scrape_id": scrape_id, **job}


@app.get("/api/db/export")
async def api_db_export():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    data = {}
    for tbl in ("galleries", "artists", "tags", "groups", "parodies", "characters"):
        try:
            data[tbl] = [dict(r) for r in conn.execute(f"SELECT * FROM {tbl}").fetchall()]
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
async def api_history(limit: int = 200):
    rows = await asyncio.to_thread(get_download_history, limit)
    return {"history": rows}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
