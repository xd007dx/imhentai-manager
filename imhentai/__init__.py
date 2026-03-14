#!/usr/bin/env python3
"""imhentai-manager: imhentai.com の検索・ダウンロード管理ツール"""

import sqlite3
import os
import json
import time
import random
import logging
import difflib
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://imhentai.xxx"
HEADERS_DEFAULT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}


# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """SQLiteデータベースを初期化し、テーブルを作成する"""
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
            artist      TEXT,
            group_name  TEXT,
            tags        TEXT,
            parodies    TEXT,
            characters  TEXT,
            downloaded  INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    logger.info(f"DB initialized: {db_path}")
    return conn


def export_db_json(conn: sqlite3.Connection, output_path: str):
    """DBの全テーブルをJSONファイルにエクスポートする"""
    data = {}
    tables = ["artists", "groups", "tags", "parodies", "characters", "galleries"]
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        data[table] = [dict(r) for r in rows]
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"DB exported to {output_path}")
    print(f"✓ Exported to {output_path}")


# ──────────────────────────────────────────────
# HTTP helper
# ──────────────────────────────────────────────

def get_session(user_agent: str = None) -> requests.Session:
    """共通Sessionオブジェクトを作成する"""
    s = requests.Session()
    headers = dict(HEADERS_DEFAULT)
    if user_agent:
        headers["User-Agent"] = user_agent
    s.headers.update(headers)
    return s


def rate_limited_get(session: requests.Session, url: str,
                     min_delay: float = 1.0, max_delay: float = 3.0) -> requests.Response:
    """レート制限付きGETリクエスト"""
    time.sleep(random.uniform(min_delay, max_delay))
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp


# ──────────────────────────────────────────────
# Scraper
# ──────────────────────────────────────────────

def _upsert_category(conn: sqlite3.Connection, table: str, name: str, url: str, count: int):
    conn.execute(
        f"INSERT INTO {table}(name, url, count) VALUES(?,?,?) "
        f"ON CONFLICT(name) DO UPDATE SET url=excluded.url, count=excluded.count",
        (name, url, count)
    )


def scrape_category_page(session: requests.Session, conn: sqlite3.Connection,
                          category: str, page: int = 1,
                          min_delay: float = 1.0, max_delay: float = 3.0) -> int:
    """
    指定カテゴリ（artists/groups/tags/parodies/characters）の
    1ページ分をスクレイピングしてDBに保存する。
    返値: 取得したエントリ数
    """
    # テーブル名のマッピング
    table_map = {
        "artists": "artists",
        "groups": "groups",
        "tags": "tags",
        "parodies": "parodies",
        "characters": "characters",
    }
    if category not in table_map:
        raise ValueError(f"Unknown category: {category}")
    table = table_map[category]

    url = f"{BASE_URL}/{category}/?page={page}"
    logger.info(f"Scraping {url}")
    try:
        resp = rate_limited_get(session, url, min_delay, max_delay)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return 0

    soup = BeautifulSoup(resp.text, "lxml")
    # imhentai のカテゴリページ構造に合わせてパース
    items = soup.select("div.tag_list a, ul.tags li a, .tag_item a")
    if not items:
        # フォールバック: 汎用リンク探索
        items = soup.select("a[href*='/" + category + "/']")

    count = 0
    for item in items:
        name = item.get_text(strip=True)
        href = item.get("href", "")
        # カウント数を取得（例: "Kariya (42)" → 42）
        import re
        m = re.search(r'\((\d+)\)', name)
        num = int(m.group(1)) if m else 0
        clean_name = re.sub(r'\s*\(\d+\)\s*$', '', name).strip()
        if clean_name:
            _upsert_category(conn, table, clean_name, href, num)
            count += 1

    conn.commit()
    logger.info(f"  → {count} entries saved for {category} page {page}")
    return count


def scrape_gallery_metadata(session: requests.Session, conn: sqlite3.Connection,
                             gallery_id: int,
                             min_delay: float = 1.0, max_delay: float = 3.0) -> dict | None:
    """ギャラリーページからメタデータをスクレイピングしてDBに保存する"""
    url = f"{BASE_URL}/g/{gallery_id}/"
    logger.info(f"Scraping gallery {gallery_id}: {url}")
    try:
        resp = rate_limited_get(session, url, min_delay, max_delay)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch gallery {gallery_id}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    def get_tag_list(label: str) -> str:
        """指定ラベルのタグリストをカンマ区切りで返す"""
        for row in soup.select(".galleries_info .field_name, .tag-container .field-name"):
            if label.lower() in row.get_text().lower():
                parent = row.parent
                tags = [a.get_text(strip=True) for a in parent.select("a")]
                return ", ".join(tags)
        return ""

    title_el = soup.select_one("h1.gallery_title, h1.title, #gallery_id h1")
    title = title_el.get_text(strip=True) if title_el else f"Gallery {gallery_id}"

    cover_el = soup.select_one(".cover img, #cover img")
    cover_url = cover_el.get("src", "") if cover_el else ""

    pages_el = soup.select_one(".pages_tag span, .pages span")
    try:
        pages = int(pages_el.get_text(strip=True)) if pages_el else 0
    except ValueError:
        pages = 0

    meta = {
        "id": gallery_id,
        "title": title,
        "url": url,
        "cover_url": cover_url,
        "pages": pages,
        "artist": get_tag_list("artist"),
        "group_name": get_tag_list("group"),
        "tags": get_tag_list("tag"),
        "parodies": get_tag_list("paro"),
        "characters": get_tag_list("character"),
        "language": get_tag_list("language"),
    }

    conn.execute("""
        INSERT INTO galleries(id, title, url, cover_url, pages, language, artist,
                              group_name, tags, parodies, characters)
        VALUES(:id,:title,:url,:cover_url,:pages,:language,:artist,
               :group_name,:tags,:parodies,:characters)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, cover_url=excluded.cover_url,
            pages=excluded.pages, language=excluded.language,
            artist=excluded.artist, group_name=excluded.group_name,
            tags=excluded.tags, parodies=excluded.parodies,
            characters=excluded.characters
    """, meta)
    conn.commit()
    return meta


# ──────────────────────────────────────────────
# Search
# ──────────────────────────────────────────────

def search_db(conn: sqlite3.Connection, query: str,
              search_type: str = "all") -> list[dict]:
    """
    SQLiteを検索する。
    search_type: "artist" / "title" / "all"
    見つからない場合はfuzzy matchで候補を返す。
    """
    results = []
    q = f"%{query}%"

    if search_type in ("artist", "all"):
        rows = conn.execute(
            "SELECT id, name, url, count FROM artists WHERE name LIKE ? ORDER BY count DESC LIMIT 20", (q,)
        ).fetchall()
        for r in rows:
            results.append({"type": "artist", **dict(r)})

    if search_type in ("title", "all"):
        rows = conn.execute(
            "SELECT id, title, url, artist FROM galleries WHERE title LIKE ? LIMIT 20", (q,)
        ).fetchall()
        for r in rows:
            results.append({"type": "gallery", **dict(r)})

    if results:
        return results

    # ── fuzzy match ──
    logger.info(f"No exact match for '{query}', running fuzzy search...")
    candidates = []

    if search_type in ("artist", "all"):
        all_artists = conn.execute("SELECT id, name, url, count FROM artists").fetchall()
        names = [r["name"] for r in all_artists]
        matches = difflib.get_close_matches(query, names, n=5, cutoff=0.4)
        for m in matches:
            row = next(r for r in all_artists if r["name"] == m)
            score = difflib.SequenceMatcher(None, query.lower(), m.lower()).ratio()
            candidates.append({"type": "artist_fuzzy", "score": round(score, 3), **dict(row)})

    if search_type in ("title", "all"):
        all_galleries = conn.execute("SELECT id, title, url, artist FROM galleries").fetchall()
        titles = [r["title"] for r in all_galleries]
        matches = difflib.get_close_matches(query, titles, n=5, cutoff=0.4)
        for m in matches:
            row = next(r for r in all_galleries if r["title"] == m)
            score = difflib.SequenceMatcher(None, query.lower(), m.lower()).ratio()
            candidates.append({"type": "gallery_fuzzy", "score": round(score, 3), **dict(row)})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:5]


# ──────────────────────────────────────────────
# Downloader
# ──────────────────────────────────────────────

def _get_image_urls(session: requests.Session, gallery_id: int,
                    min_delay: float = 1.0, max_delay: float = 3.0) -> list[str]:
    """ギャラリーの全画像URLリストを取得する"""
    url = f"{BASE_URL}/g/{gallery_id}/"
    resp = rate_limited_get(session, url, min_delay, max_delay)
    soup = BeautifulSoup(resp.text, "lxml")

    image_urls = []
    # imhentai の画像リンク構造をパース
    thumb_links = soup.select(".thumbs a, #append_thumbs a")
    for link in thumb_links:
        img = link.select_one("img")
        if img:
            # サムネイルURLからフルサイズURLへ変換
            src = img.get("data-src") or img.get("src", "")
            # t.jpg → .jpg (imhentai のサムネイル命名規則)
            full = src.replace("/t.", "/").replace("t1/", "").replace("t2/", "")
            if full:
                image_urls.append(full)

    logger.info(f"Found {len(image_urls)} images for gallery {gallery_id}")
    return image_urls


def download_gallery_zip(session: requests.Session, gallery_id: int,
                          output_dir: str,
                          min_delay: float = 1.0, max_delay: float = 3.0) -> str:
    """ギャラリーをZIPファイルとしてダウンロードする"""
    import zipfile

    image_urls = _get_image_urls(session, gallery_id, min_delay, max_delay)
    if not image_urls:
        raise RuntimeError(f"No images found for gallery {gallery_id}")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    zip_path = Path(output_dir) / f"{gallery_id}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, img_url in enumerate(image_urls, 1):
            try:
                resp = rate_limited_get(session, img_url, 0.5, 1.5)
                ext = img_url.rsplit(".", 1)[-1].split("?")[0] or "jpg"
                zf.writestr(f"{i:04d}.{ext}", resp.content)
                print(f"  [{i}/{len(image_urls)}] Downloaded", end="\r")
            except Exception as e:
                logger.warning(f"Failed to download {img_url}: {e}")

    print(f"\n✓ Saved ZIP: {zip_path}")
    logger.info(f"ZIP saved: {zip_path}")
    return str(zip_path)


def download_gallery_pdf(session: requests.Session, gallery_id: int,
                          output_dir: str,
                          min_delay: float = 1.0, max_delay: float = 3.0) -> str:
    """ギャラリーをPDFファイルとしてダウンロードする"""
    from fpdf import FPDF
    from PIL import Image
    import io

    image_urls = _get_image_urls(session, gallery_id, min_delay, max_delay)
    if not image_urls:
        raise RuntimeError(f"No images found for gallery {gallery_id}")

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    pdf_path = Path(output_dir) / f"{gallery_id}.pdf"

    pdf = FPDF()
    pdf.set_auto_page_break(False)

    for i, img_url in enumerate(image_urls, 1):
        try:
            resp = rate_limited_get(session, img_url, 0.5, 1.5)
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            w, h = img.size
            # A4比率で調整 (mm単位)
            pdf_w = 210
            pdf_h = int(h * pdf_w / w)
            pdf.add_page(format=(pdf_w, pdf_h))

            # 一時ファイルに保存してFPDFに渡す
            tmp = Path(output_dir) / f"_tmp_{i}.jpg"
            img.save(str(tmp), "JPEG", quality=85)
            pdf.image(str(tmp), 0, 0, pdf_w, pdf_h)
            tmp.unlink(missing_ok=True)

            print(f"  [{i}/{len(image_urls)}] Processed", end="\r")
        except Exception as e:
            logger.warning(f"Failed to process {img_url}: {e}")

    pdf.output(str(pdf_path))
    print(f"\n✓ Saved PDF: {pdf_path}")
    logger.info(f"PDF saved: {pdf_path}")
    return str(pdf_path)


# ──────────────────────────────────────────────
# Google Drive (スタブ)
# ──────────────────────────────────────────────

def upload_to_gdrive(file_path: str, folder_id: str = "",
                     credentials_file: str = "credentials.json",
                     delete_old: bool = True) -> str:
    """
    Google Driveにファイルをアップロードする（スタブ実装）
    TODO: google-api-python-client で本番実装
    """
    logger.warning("[STUB] Google Drive upload not yet implemented.")
    logger.warning(f"  Would upload: {file_path}")
    logger.warning(f"  Target folder: {folder_id or '(root)'}")
    logger.warning(f"  Delete old files: {delete_old}")
    print(f"⚠ Google Drive upload is not yet configured.")
    print(f"  File to upload: {file_path}")
    print(f"  Set up credentials.json to enable this feature.")
    return ""
