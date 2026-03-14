#!/usr/bin/env python3
"""
imhentai-manager コアライブラリ
imhentai.xxx のスクレイピング・DB管理・検索・ダウンロード
"""

import sqlite3
import os
import json
import time
import random
import logging
import difflib
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://imhentai.xxx"
HEADERS_DEFAULT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}

# カテゴリURL対応表
CATEGORY_URL = {
    "artists":    ("artists",    "artist"),
    "groups":     ("groups",     "group"),
    "tags":       ("tags",       "tag"),
    "parodies":   ("parodies",   "parody"),
    "characters": ("characters", "character"),
}


# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """SQLiteデータベースを初期化しテーブルを作成する"""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS artists (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT UNIQUE NOT NULL,
            url     TEXT,
            count   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS groups (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT UNIQUE NOT NULL,
            url     TEXT,
            count   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tags (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT UNIQUE NOT NULL,
            url     TEXT,
            count   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS parodies (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT UNIQUE NOT NULL,
            url     TEXT,
            count   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS characters (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT UNIQUE NOT NULL,
            url     TEXT,
            count   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS galleries (
            id          INTEGER PRIMARY KEY,
            title       TEXT NOT NULL,
            url         TEXT,
            cover_url   TEXT,
            pages       INTEGER DEFAULT 0,
            language    TEXT,
            category    TEXT,
            artist      TEXT,
            group_name  TEXT,
            tags        TEXT,
            parodies    TEXT,
            characters  TEXT,
            downloaded  INTEGER DEFAULT 0,
            file_path   TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()

    # マイグレーション: 既存DBに不足カラムを追加
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(galleries)")}
    if "file_path" not in existing_cols:
        conn.execute("ALTER TABLE galleries ADD COLUMN file_path TEXT")
    if "category" not in existing_cols:
        conn.execute("ALTER TABLE galleries ADD COLUMN category TEXT")
    conn.commit()

    return conn


def export_db_json(conn: sqlite3.Connection, output_path: str):
    """DBの全テーブルをJSONにエクスポートする"""
    data = {}
    tables = ["artists", "groups", "tags", "parodies", "characters", "galleries"]
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        data[table] = [dict(r) for r in rows]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"DB exported to {output_path}")


def db_stats(conn: sqlite3.Connection) -> dict:
    """テーブルごとの件数を返す"""
    tables = ["artists", "groups", "tags", "parodies", "characters", "galleries"]
    stats = {}
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            stats[t] = count
        except Exception:
            stats[t] = 0
    try:
        stats["downloaded"] = conn.execute(
            "SELECT COUNT(*) FROM galleries WHERE downloaded=1"
        ).fetchone()[0]
    except Exception:
        stats["downloaded"] = 0
    return stats


# ──────────────────────────────────────────────
# HTTP helper
# ──────────────────────────────────────────────

def get_session(user_agent: str = None) -> requests.Session:
    """共通Sessionを作成する"""
    s = requests.Session()
    headers = dict(HEADERS_DEFAULT)
    if user_agent:
        headers["User-Agent"] = user_agent
    s.headers.update(headers)
    return s


def rate_limited_get(session: requests.Session, url: str,
                     min_delay: float = 1.0, max_delay: float = 3.0,
                     retries: int = 3) -> requests.Response:
    """レート制限＋リトライ付きGET"""
    for attempt in range(retries):
        time.sleep(random.uniform(min_delay, max_delay))
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)  # exponential backoff


# ──────────────────────────────────────────────
# Scraper - カテゴリ
# ──────────────────────────────────────────────

def scrape_category_page(session: requests.Session, conn: sqlite3.Connection,
                          category: str, page: int = 1,
                          min_delay: float = 1.0, max_delay: float = 3.0) -> int:
    """
    カテゴリページ1枚をスクレイピングしてDBに保存する
    category: artists / groups / tags / parodies / characters
    返値: 取得したエントリ数
    """
    if category not in CATEGORY_URL:
        raise ValueError(f"Unknown category: {category}")

    list_slug, item_slug = CATEGORY_URL[category]
    url = f"{BASE_URL}/{list_slug}/?page={page}"
    logger.info(f"Scraping {url}")

    try:
        resp = rate_limited_get(session, url, min_delay, max_delay)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return 0

    soup = BeautifulSoup(resp.text, "lxml")

    # imhentai のカテゴリリストはアンカータグに /artist/ /tag/ 等が含まれる
    links = soup.select(f"a[href*='/{item_slug}/']")
    count = 0

    for link in links:
        href = link.get("href", "")
        # ナビ等の重複リンクを除外（パス階層が /item_slug/name/ の形式のもの）
        if not re.match(rf"^/{item_slug}/[^/]+/?$", href):
            continue

        full_text = link.get_text(strip=True)
        # バッジ（数字）を取得
        badge = link.select_one(".badge")
        badge_count = int(re.sub(r"\D", "", badge.get_text())) if badge else 0

        # 名前はbadgeを除いた部分
        if badge:
            name = full_text.replace(badge.get_text(strip=True), "").strip()
        else:
            # 末尾の数字を除去
            name = re.sub(r"\s*\d+\s*$", "", full_text).strip()

        if not name:
            continue

        conn.execute(
            f"INSERT INTO {category}(name, url, count) VALUES(?,?,?) "
            f"ON CONFLICT(name) DO UPDATE SET url=excluded.url, count=excluded.count",
            (name, href, badge_count)
        )
        count += 1

    conn.commit()
    logger.info(f"  {category} page {page}: {count} entries")
    return count


# ──────────────────────────────────────────────
# Scraper - ギャラリー
# ──────────────────────────────────────────────

def scrape_gallery_metadata(session: requests.Session, conn: sqlite3.Connection,
                             gallery_id: int,
                             min_delay: float = 1.0, max_delay: float = 3.0) -> dict | None:
    """ギャラリーページからメタデータを取得してDBに保存する"""
    url = f"{BASE_URL}/gallery/{gallery_id}/"
    logger.info(f"Scraping gallery {gallery_id}")

    try:
        resp = rate_limited_get(session, url, min_delay, max_delay)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch gallery {gallery_id}: {e}")
        return None

    if resp.status_code == 404:
        logger.warning(f"Gallery {gallery_id} not found (404)")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # タイトル
    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else f"Gallery {gallery_id}"
    if title == "404 - Not Found":
        return None

    # カバー画像
    cover_el = soup.select_one("img[data-src*='cover']")
    if not cover_el:
        cover_el = soup.select_one(".cover img, #cover img")
    cover_url = (cover_el.get("data-src") or cover_el.get("src", "")) if cover_el else ""

    # galleries_info からメタデータを取得
    info_ul = soup.select_one(".galleries_info")

    def get_tags_by_text(label: str) -> str:
        """galleries_infoのliからラベルでタグを取得"""
        if not info_ul:
            return ""
        for li in info_ul.select("li"):
            span = li.select_one(".tags_text")
            if span and label.lower() in span.get_text().lower():
                tags = [a.select_one("span") and
                        a.get_text(strip=True).replace(
                            a.select_one(".badge").get_text(strip=True), ""
                        ).strip()
                        if a.select_one(".badge") else a.get_text(strip=True)
                        for a in li.select("a.tag")]
                return ", ".join(t for t in tags if t)
        return ""

    def get_plain_by_text(label: str) -> str:
        """Pages: 108 のようなプレーンテキストを取得"""
        if not info_ul:
            return ""
        for li in info_ul.select("li"):
            text = li.get_text(strip=True)
            if text.startswith(label):
                return text.replace(label, "").strip()
        return ""

    pages_text = get_plain_by_text("Pages:")
    try:
        pages = int(pages_text) if pages_text else 0
    except ValueError:
        pages = 0

    meta = {
        "id":         gallery_id,
        "title":      title,
        "url":        url,
        "cover_url":  cover_url,
        "pages":      pages,
        "language":   get_tags_by_text("Language"),
        "category":   get_tags_by_text("Category"),
        "artist":     get_tags_by_text("Artist"),
        "group_name": get_tags_by_text("Group"),
        "tags":       get_tags_by_text("Tag"),
        "parodies":   get_tags_by_text("Parody"),
        "characters": get_tags_by_text("Character"),
    }

    conn.execute("""
        INSERT INTO galleries(id,title,url,cover_url,pages,language,category,
                              artist,group_name,tags,parodies,characters)
        VALUES(:id,:title,:url,:cover_url,:pages,:language,:category,
               :artist,:group_name,:tags,:parodies,:characters)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, cover_url=excluded.cover_url, pages=excluded.pages,
            language=excluded.language, category=excluded.category, artist=excluded.artist,
            group_name=excluded.group_name, tags=excluded.tags,
            parodies=excluded.parodies, characters=excluded.characters
    """, meta)
    conn.commit()
    return meta


def get_gallery_image_urls(session: requests.Session, gallery_id: int,
                            min_delay: float = 1.0, max_delay: float = 3.0) -> list[str]:
    """ギャラリーの全画像URLリストを取得する"""
    url = f"{BASE_URL}/gallery/{gallery_id}/"
    resp = rate_limited_get(session, url, min_delay, max_delay)
    soup = BeautifulSoup(resp.text, "lxml")

    image_urls = []
    # サムネイルの data-src から取得
    for img in soup.select("img[data-src]"):
        src = img.get("data-src", "")
        if not src or "cover" in src:
            continue
        # サムネイル(1t.jpg) → フルサイズ(1.jpg) へ変換
        full = re.sub(r"(\d+)t\.(jpg|png|gif|webp)", r"\1.\2", src)
        if full:
            image_urls.append(full)

    logger.info(f"Found {len(image_urls)} images for gallery {gallery_id}")
    return image_urls


# ──────────────────────────────────────────────
# Search
# ──────────────────────────────────────────────

def search_db(conn: sqlite3.Connection, query: str,
              search_type: str = "all") -> list[dict]:
    """
    DBを検索。見つからない場合はfuzzy matchで候補を返す。
    search_type: "artist" / "title" / "all"
    """
    results = []
    q = f"%{query}%"

    if search_type in ("artist", "all"):
        rows = conn.execute(
            "SELECT id, name, url, count FROM artists WHERE name LIKE ? ORDER BY count DESC LIMIT 20",
            (q,)
        ).fetchall()
        for r in rows:
            results.append({"type": "artist", **dict(r)})

    if search_type in ("title", "all"):
        rows = conn.execute(
            "SELECT id, title, url, artist FROM galleries WHERE title LIKE ? LIMIT 20",
            (q,)
        ).fetchall()
        for r in rows:
            results.append({"type": "gallery", **dict(r)})

    if results:
        return results

    # ── fuzzy match ──
    logger.info(f"No exact match for '{query}', running fuzzy search...")
    candidates = []

    if search_type in ("artist", "all"):
        all_names = [r["name"] for r in conn.execute("SELECT name FROM artists").fetchall()]
        matches = difflib.get_close_matches(query, all_names, n=5, cutoff=0.4)
        for m in matches:
            row = conn.execute(
                "SELECT id, name, url, count FROM artists WHERE name=?", (m,)
            ).fetchone()
            if row:
                score = difflib.SequenceMatcher(None, query.lower(), m.lower()).ratio()
                candidates.append({"type": "artist_fuzzy", "score": round(score, 3), **dict(row)})

    if search_type in ("title", "all"):
        all_titles = [r["title"] for r in conn.execute("SELECT title FROM galleries").fetchall()]
        matches = difflib.get_close_matches(query, all_titles, n=5, cutoff=0.4)
        for m in matches:
            row = conn.execute(
                "SELECT id, title, url, artist FROM galleries WHERE title=?", (m,)
            ).fetchone()
            if row:
                score = difflib.SequenceMatcher(None, query.lower(), m.lower()).ratio()
                candidates.append({"type": "gallery_fuzzy", "score": round(score, 3), **dict(row)})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:5]


# ──────────────────────────────────────────────
# Downloader
# ──────────────────────────────────────────────

def download_gallery_zip(session: requests.Session, gallery_id: int,
                          output_dir: str,
                          min_delay: float = 0.5, max_delay: float = 1.5,
                          progress_cb=None) -> str:
    """ギャラリーをZIPでダウンロードする。progress_cb(done, total)を呼び出す"""
    import zipfile

    image_urls = get_gallery_image_urls(session, gallery_id, min_delay, max_delay)
    if not image_urls:
        raise RuntimeError(f"No images found for gallery {gallery_id}")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    zip_path = Path(output_dir) / f"{gallery_id}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, img_url in enumerate(image_urls, 1):
            if progress_cb:
                progress_cb(i, len(image_urls))
            try:
                resp = rate_limited_get(session, img_url, 0.3, 0.8)
                ext = img_url.rsplit(".", 1)[-1].split("?")[0] or "jpg"
                zf.writestr(f"{i:04d}.{ext}", resp.content)
            except Exception as e:
                logger.warning(f"Failed {img_url}: {e}")

    logger.info(f"ZIP saved: {zip_path}")
    return str(zip_path)


def download_gallery_pdf(session: requests.Session, gallery_id: int,
                          output_dir: str,
                          min_delay: float = 0.5, max_delay: float = 1.5,
                          progress_cb=None) -> str:
    """ギャラリーをPDFでダウンロードする"""
    from fpdf import FPDF
    from PIL import Image
    import io

    image_urls = get_gallery_image_urls(session, gallery_id, min_delay, max_delay)
    if not image_urls:
        raise RuntimeError(f"No images found for gallery {gallery_id}")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    pdf_path = Path(output_dir) / f"{gallery_id}.pdf"
    tmp_dir = Path(output_dir) / f"_tmp_{gallery_id}"
    tmp_dir.mkdir(exist_ok=True)

    pdf = FPDF()
    pdf.set_auto_page_break(False)

    for i, img_url in enumerate(image_urls, 1):
        if progress_cb:
            progress_cb(i, len(image_urls))
        try:
            resp = rate_limited_get(session, img_url, 0.3, 0.8)
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            w, h = img.size
            pdf_w = 210
            pdf_h = int(h * pdf_w / w)
            pdf.add_page(format=(pdf_w, pdf_h))
            tmp_file = tmp_dir / f"{i:04d}.jpg"
            img.save(str(tmp_file), "JPEG", quality=85)
            pdf.image(str(tmp_file), 0, 0, pdf_w, pdf_h)
        except Exception as e:
            logger.warning(f"Failed {img_url}: {e}")

    pdf.output(str(pdf_path))

    # 一時ファイル削除
    import shutil
    shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"PDF saved: {pdf_path}")
    return str(pdf_path)


# ──────────────────────────────────────────────
# Google Drive (スタブ → 本番実装準備済み)
# ──────────────────────────────────────────────

def upload_to_gdrive(file_path: str, folder_id: str = "",
                     credentials_file: str = "credentials.json",
                     delete_old: bool = True) -> str:
    """
    Google Driveにアップロード
    credentials.jsonが存在する場合は本番実装を使用
    """
    creds_path = Path(credentials_file)

    if not creds_path.exists():
        logger.warning("[GDrive] credentials.json not found - stub mode")
        print(f"⚠ Google Drive未設定: {file_path} はアップロードされませんでした")
        print(f"  credentials.json を配置して有効化できます")
        return ""

    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from google.oauth2.credentials import Credentials
        import googleapiclient

        creds = Credentials.from_authorized_user_file(str(creds_path))
        service = build("drive", "v3", credentials=creds)

        file_name = Path(file_path).name

        # 古いファイルを削除
        if delete_old and folder_id:
            results = service.files().list(
                q=f"name='{file_name}' and '{folder_id}' in parents",
                fields="files(id, name)"
            ).execute()
            for f in results.get("files", []):
                service.files().delete(fileId=f["id"]).execute()
                logger.info(f"[GDrive] Deleted old file: {f['name']} ({f['id']})")

        # アップロード
        media = MediaFileUpload(file_path, resumable=True)
        file_meta = {"name": file_name}
        if folder_id:
            file_meta["parents"] = [folder_id]

        uploaded = service.files().create(
            body=file_meta, media_body=media, fields="id, webViewLink"
        ).execute()

        link = uploaded.get("webViewLink", "")
        logger.info(f"[GDrive] Uploaded: {file_name} → {link}")
        return link

    except Exception as e:
        logger.error(f"[GDrive] Upload failed: {e}")
        return ""
