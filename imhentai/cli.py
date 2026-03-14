#!/usr/bin/env python3
"""
imhentai-manager CLI エントリポイント
使い方: python -m imhentai.cli <command> [options]
"""

import argparse
import logging
import sys
import os
from pathlib import Path

import yaml

from imhentai import (
    init_db, export_db_json, get_session,
    scrape_category_page, scrape_gallery_metadata,
    search_db, download_gallery_zip, download_gallery_pdf,
    upload_to_gdrive,
)

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "download_dir": "./downloads",
    "database_path": "./data/imhentai.db",
    "rate_limit": {"min_delay": 1.0, "max_delay": 3.0},
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "gdrive": {"credentials_file": "credentials.json", "folder_id": ""},
}


def load_config(config_path: str = "config.yaml") -> dict:
    """設定ファイルを読み込む（存在しない場合はデフォルト値を使用）"""
    cfg = dict(DEFAULT_CONFIG)
    if Path(config_path).exists():
        with open(config_path, encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f) or {}
        cfg.update(user_cfg)
    return cfg


# ──────────────────────────────────────────────
# コマンド: db
# ──────────────────────────────────────────────

def cmd_db(args, cfg):
    conn = init_db(cfg["database_path"])
    session = get_session(cfg["user_agent"])
    rl = cfg["rate_limit"]

    if args.db_cmd == "update":
        categories = []
        if args.artists:    categories.append("artists")
        if args.groups:     categories.append("groups")
        if args.tags:       categories.append("tags")
        if args.parodies:   categories.append("parodies")
        if args.characters: categories.append("characters")
        if not categories:
            categories = ["artists", "groups", "tags", "parodies", "characters"]

        for cat in categories:
            total = 0
            for page in range(1, args.pages + 1):
                print(f"Scraping {cat} page {page}/{args.pages}...")
                n = scrape_category_page(session, conn, cat, page,
                                          rl["min_delay"], rl["max_delay"])
                total += n
                if n == 0:
                    print(f"  No more results for {cat}, stopping.")
                    break
            print(f"✓ {cat}: {total} entries saved.")

    elif args.db_cmd == "export":
        output = args.output or "./data/db.json"
        export_db_json(conn, output)

    else:
        print("Unknown db command. Use: db update / db export")


# ──────────────────────────────────────────────
# コマンド: search
# ──────────────────────────────────────────────

def cmd_search(args, cfg):
    conn = init_db(cfg["database_path"])
    query = args.query

    if args.artist:
        search_type = "artist"
    elif args.title:
        search_type = "title"
    else:
        search_type = "all"

    print(f"🔍 Searching for: '{query}' (type: {search_type})")
    results = search_db(conn, query, search_type)

    if not results:
        print("❌ No results found.")
        return

    fuzzy = any(r["type"].endswith("_fuzzy") for r in results)
    if fuzzy:
        print(f"\n⚠ Exact match not found. Did you mean one of these?\n")
    else:
        print(f"\n✅ Found {len(results)} result(s):\n")

    for i, r in enumerate(results, 1):
        t = r["type"]
        if t in ("artist", "artist_fuzzy"):
            score = f" (similarity: {r.get('score', ''):.0%})" if fuzzy else ""
            print(f"  {i}. [Artist]{score} {r['name']}  (count: {r.get('count', '?')})")
        elif t in ("gallery", "gallery_fuzzy"):
            score = f" (similarity: {r.get('score', ''):.0%})" if fuzzy else ""
            print(f"  {i}. [Gallery]{score} ID:{r['id']}  {r['title']}")
            if r.get("artist"):
                print(f"       Artist: {r['artist']}")


# ──────────────────────────────────────────────
# コマンド: download
# ──────────────────────────────────────────────

def cmd_download(args, cfg):
    conn = init_db(cfg["database_path"])
    session = get_session(cfg["user_agent"])
    rl = cfg["rate_limit"]
    out_dir = cfg["download_dir"]
    fmt = args.format.lower()

    gallery_id = int(args.gallery_id)
    print(f"⬇ Downloading gallery {gallery_id} as {fmt.upper()}...")

    # メタデータ取得・保存
    meta = scrape_gallery_metadata(session, conn, gallery_id,
                                    rl["min_delay"], rl["max_delay"])
    if meta:
        print(f"  Title: {meta['title']}")
        print(f"  Artist: {meta['artist']}")
        print(f"  Pages: {meta['pages']}")

    # ダウンロード
    if fmt == "zip":
        file_path = download_gallery_zip(session, gallery_id, out_dir,
                                          rl["min_delay"], rl["max_delay"])
    elif fmt == "pdf":
        file_path = download_gallery_pdf(session, gallery_id, out_dir,
                                          rl["min_delay"], rl["max_delay"])
    else:
        print(f"❌ Unknown format: {fmt}. Use zip or pdf.")
        return

    # ダウンロード済みフラグをDBに記録
    conn.execute("UPDATE galleries SET downloaded=1 WHERE id=?", (gallery_id,))
    conn.commit()

    # Googleドライブアップロード
    if args.upload_drive:
        gdrive_cfg = cfg["gdrive"]
        upload_to_gdrive(
            file_path,
            folder_id=gdrive_cfg.get("folder_id", ""),
            credentials_file=gdrive_cfg.get("credentials_file", "credentials.json"),
            delete_old=True,
        )


# ──────────────────────────────────────────────
# コマンド: batch
# ──────────────────────────────────────────────

def cmd_batch(args, cfg):
    """テキストファイルのギャラリーIDリストを一括ダウンロードする"""
    if not Path(args.file).exists():
        print(f"❌ File not found: {args.file}")
        return

    with open(args.file) as f:
        ids = [line.strip() for line in f if line.strip().isdigit()]

    print(f"📋 Batch download: {len(ids)} galleries")
    for i, gid in enumerate(ids, 1):
        print(f"\n[{i}/{len(ids)}] Gallery {gid}")
        # args を一時的に上書き
        args.gallery_id = gid
        args.format = args.format or "zip"
        try:
            cmd_download(args, cfg)
        except Exception as e:
            logger.error(f"Failed to download gallery {gid}: {e}")
            print(f"  ⚠ Skipped due to error: {e}")


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="imhentai",
        description="imhentai.com 検索・ダウンロード管理ツール",
    )
    parser.add_argument("--config", default="config.yaml", help="設定ファイルのパス")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを表示")

    sub = parser.add_subparsers(dest="command")

    # ── db ──
    db_p = sub.add_parser("db", help="データベース操作")
    db_sub = db_p.add_subparsers(dest="db_cmd")

    db_update = db_sub.add_parser("update", help="サイトからデータをスクレイピング")
    db_update.add_argument("--artists",    action="store_true")
    db_update.add_argument("--groups",     action="store_true")
    db_update.add_argument("--tags",       action="store_true")
    db_update.add_argument("--parodies",   action="store_true")
    db_update.add_argument("--characters", action="store_true")
    db_update.add_argument("--pages", type=int, default=5, help="取得するページ数")

    db_export = db_sub.add_parser("export", help="DBをJSONエクスポート")
    db_export.add_argument("--output", "-o", help="出力ファイルパス")

    # ── search ──
    search_p = sub.add_parser("search", help="アーティスト・作品を検索")
    search_p.add_argument("query", help="検索クエリ")
    search_p.add_argument("--artist", "-a", action="store_true", help="アーティスト名で検索")
    search_p.add_argument("--title",  "-t", action="store_true", help="作品タイトルで検索")

    # ── download ──
    dl_p = sub.add_parser("download", help="ギャラリーをダウンロード")
    dl_p.add_argument("gallery_id", help="ギャラリーID")
    dl_p.add_argument("--format", "-f", default="zip", choices=["zip", "pdf"],
                      help="保存形式 (zip/pdf)")
    dl_p.add_argument("--upload-drive", action="store_true",
                      help="Google Driveにアップロード")

    # ── batch ──
    batch_p = sub.add_parser("batch", help="リストファイルから一括ダウンロード")
    batch_p.add_argument("--file", required=True, help="ギャラリーIDリストファイル")
    batch_p.add_argument("--format", "-f", default="zip", choices=["zip", "pdf"])
    batch_p.add_argument("--upload-drive", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = load_config(args.config)

    if args.command == "db":
        cmd_db(args, cfg)
    elif args.command == "search":
        cmd_search(args, cfg)
    elif args.command == "download":
        cmd_download(args, cfg)
    elif args.command == "batch":
        cmd_batch(args, cfg)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
