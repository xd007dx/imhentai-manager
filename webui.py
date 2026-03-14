#!/usr/bin/env python3
"""
imhentai-manager Web UI (Gradio)
Phase 2機能込み:
  - 進捗バー付きダウンロード
  - 重複スキップ
  - ダウンロード済み管理
  - DBスクレイピング
  - Fuzzy match検索
  - Google Drive連携
"""

import gradio as gr
import logging
import json
import threading
from pathlib import Path
from datetime import datetime
import yaml

from imhentai import (
    init_db, export_db_json, db_stats,
    get_session, get_category_last_page,
    scrape_category_all_parallel,
    scrape_gallery_metadata,
    search_db, download_gallery_zip, download_gallery_pdf,
    upload_to_gdrive, get_gallery_image_urls,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────

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

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if Path("config.yaml").exists():
        with open("config.yaml", encoding="utf-8") as f:
            cfg.update(yaml.safe_load(f) or {})
    return cfg

def get_conn():
    cfg = load_config()
    return init_db(cfg["database_path"])


# ──────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────

CUSTOM_CSS = """
/* ── 全体 ── */
.gradio-container {
    max-width: 1100px !important;
    margin: 0 auto !important;
    font-family: 'Segoe UI', sans-serif;
}

/* ── ヘッダー ── */
.app-header {
    text-align: center;
    padding: 24px 0 8px;
    border-bottom: 2px solid #e2e8f0;
    margin-bottom: 16px;
}
.app-header h1 { font-size: 2rem; margin: 0; }
.app-header p  { color: #64748b; margin: 4px 0 0; }

/* ── タブ ── */
.tab-nav button {
    font-size: 1rem !important;
    padding: 10px 20px !important;
}

/* ── ボタン ── */
.btn-primary  { background: #6366f1 !important; }
.btn-success  { background: #22c55e !important; }
.btn-warning  { background: #f59e0b !important; }
.btn-danger   { background: #ef4444 !important; }

/* ── 統計カード ── */
.stat-card {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
}
.stat-num { font-size: 2rem; font-weight: bold; color: #6366f1; }
.stat-label { color: #64748b; font-size: 0.85rem; }

/* ── 結果テキスト ── */
textarea { font-family: monospace !important; font-size: 0.9rem !important; }

/* ── ダウンロード済みバッジ ── */
.dl-done { color: #22c55e; }
.dl-pend { color: #94a3b8; }
"""


# ──────────────────────────────────────────────
# ヘルパー
# ──────────────────────────────────────────────

def fmt_stats() -> str:
    """DB統計をフォーマットして返す"""
    conn = get_conn()
    s = db_stats(conn)
    lines = [
        "📊 **データベース統計**",
        f"  🎨 Artists:    {s.get('artists', 0):,} 件",
        f"  👥 Groups:     {s.get('groups', 0):,} 件",
        f"  🏷️  Tags:       {s.get('tags', 0):,} 件",
        f"  📺 Parodies:   {s.get('parodies', 0):,} 件",
        f"  👤 Characters: {s.get('characters', 0):,} 件",
        f"  📚 Galleries:  {s.get('galleries', 0):,} 件 (DL済み: {s.get('downloaded', 0):,})",
    ]
    return "\n".join(lines)

def is_downloaded(gallery_id: int) -> bool:
    """ギャラリーがDL済みかどうか確認する"""
    conn = get_conn()
    row = conn.execute(
        "SELECT downloaded, file_path FROM galleries WHERE id=?", (gallery_id,)
    ).fetchone()
    if row and row["downloaded"]:
        # ファイルが実際に存在するか確認
        fp = row["file_path"]
        if fp and Path(fp).exists():
            return True
    return False


# ──────────────────────────────────────────────
# タブ1: 🔍 検索
# ──────────────────────────────────────────────

def do_search(query, search_type, progress=gr.Progress()):
    if not query.strip():
        return "⚠ クエリを入力してください。", gr.update(choices=[], visible=False)

    progress(0.3, desc="検索中...")
    conn = get_conn()
    type_map = {"全体": "all", "🎨 アーティスト": "artist", "📚 作品タイトル": "title"}
    results = search_db(conn, query.strip(), type_map.get(search_type, "all"))
    progress(1.0)

    if not results:
        return "❌ 結果が見つかりませんでした。\n\n💡 先にDB管理タブでスクレイピングを実行してください。", \
               gr.update(choices=[], visible=False)

    fuzzy = any(r["type"].endswith("_fuzzy") for r in results)
    lines = []
    choices = []

    if fuzzy:
        lines.append(f"⚠️  「{query}」の完全一致なし → 近似候補:\n")
    else:
        lines.append(f"✅  {len(results)} 件ヒット:\n")

    for i, r in enumerate(results, 1):
        t = r["type"]
        if "artist" in t:
            score = f"  [類似度: {r.get('score', 0):.0%}]" if fuzzy else ""
            line = f"{i}. 🎨 {r['name']}{score}  (作品数: {r.get('count', '?')})"
            lines.append(line)
            choices.append(r["name"])
        elif "gallery" in t:
            score = f"  [類似度: {r.get('score', 0):.0%}]" if fuzzy else ""
            dl = "✅" if is_downloaded(r["id"]) else "⬜"
            line = f"{i}. 📚 {dl} ID:{r['id']}{score}  {r['title']}"
            if r.get("artist"):
                line += f"\n       🎨 {r['artist']}"
            lines.append(line)
            choices.append(f"ID:{r['id']} - {r['title']}")

    return "\n".join(lines), gr.update(choices=choices, visible=bool(choices))


def search_tab():
    with gr.Tab("🔍 検索"):
        gr.Markdown("## アーティスト・作品を検索")
        with gr.Row():
            query_input = gr.Textbox(
                label="検索クエリ",
                placeholder="例: Kariya、Tekoki Maniax、Abubu ...",
                scale=4,
            )
            search_type = gr.Radio(
                ["全体", "🎨 アーティスト", "📚 作品タイトル"],
                label="検索対象",
                value="全体",
                scale=2,
            )
        search_btn = gr.Button("🔍 検索", variant="primary", size="lg")
        result_text = gr.Textbox(label="検索結果", lines=14, interactive=False)
        candidates = gr.Radio(
            label="📌 候補から選択（ダウンロードタブのIDに自動入力されます）",
            visible=False,
            interactive=True,
        )

        search_btn.click(do_search, [query_input, search_type], [result_text, candidates])
        query_input.submit(do_search, [query_input, search_type], [result_text, candidates])

    return candidates


# ──────────────────────────────────────────────
# タブ2: ⬇️ ダウンロード
# ──────────────────────────────────────────────

def do_download(gallery_id_str, fmt, upload_drive, skip_if_exists, dl_workers, progress=gr.Progress()):
    gid_str = str(gallery_id_str).strip()
    m = re.search(r"\d+", gid_str)
    if not m:
        return "❌ 有効なギャラリーIDを入力してください。", None

    gid = int(m.group())
    cfg = load_config()
    session = get_session(cfg["user_agent"])
    conn = get_conn()
    out_dir = cfg["download_dir"]
    workers = int(dl_workers)

    # 重複スキップ
    if skip_if_exists and is_downloaded(gid):
        row = conn.execute("SELECT title, file_path FROM galleries WHERE id=?", (gid,)).fetchone()
        title = row["title"] if row else f"Gallery {gid}"
        fp = row["file_path"] if row else ""
        return f"⏭️  スキップ: 「{title}」はDL済みです\n  ファイル: {fp}", None

    # メタデータ取得
    progress(0.05, desc="📖 メタデータ取得中...")
    try:
        meta = scrape_gallery_metadata(session, conn, gid)
    except Exception as e:
        return f"❌ メタデータ取得失敗: {e}", None

    if not meta:
        return f"❌ ギャラリー {gid} が見つかりません (404)", None

    info = (
        f"📖 タイトル : {meta['title']}\n"
        f"🎨 アーティスト: {meta['artist'] or '不明'}\n"
        f"📄 ページ数 : {meta['pages']}\n"
        f"🏷️  タグ     : {(meta['tags'] or '')[:80]}{'...' if len(meta.get('tags',''))>80 else ''}\n"
    )

    # 並列ダウンロード進捗コールバック
    def cb(done, total):
        progress(
            0.1 + 0.85 * done / total,
            desc=f"⬇️  [{done}/{total}] {workers}並列ダウンロード中..."
        )

    progress(0.1, desc=f"⬇️  {workers}並列でダウンロード開始...")
    try:
        if fmt == "ZIP":
            file_path = download_gallery_zip(session, gid, out_dir, workers, cb)
        else:
            file_path = download_gallery_pdf(session, gid, out_dir, workers, cb)
    except Exception as e:
        return f"{info}\n❌ ダウンロード失敗: {e}", None

    conn.execute(
        "UPDATE galleries SET downloaded=1, file_path=? WHERE id=?",
        (file_path, gid)
    )
    conn.commit()

    msg = f"{info}\n✅ 保存完了: {file_path}"

    if upload_drive:
        progress(0.97, desc="☁️  Google Drive アップロード中...")
        gdrive_cfg = cfg["gdrive"]
        link = upload_to_gdrive(
            file_path,
            folder_id=gdrive_cfg.get("folder_id", ""),
            credentials_file=gdrive_cfg.get("credentials_file", "credentials.json"),
            delete_old=True,
        )
        msg += f"\n☁️  Drive: {link}" if link else "\n⚠️  Google Drive未設定"

    progress(1.0, desc="✅ 完了!")
    return msg, file_path


def download_tab(search_candidates):
    import re as _re

    def fill_id_from_candidate(candidate):
        if not candidate:
            return gr.update()
        m = _re.search(r"ID:(\d+)", candidate)
        if m:
            return gr.update(value=m.group(1))
        return gr.update(value=candidate)

    with gr.Tab("⬇️ ダウンロード"):
        gr.Markdown("## ギャラリーをダウンロード")
        with gr.Row():
            gallery_id_input = gr.Textbox(
                label="ギャラリーID",
                placeholder="例: 503632",
                scale=3,
            )
            fmt_radio = gr.Radio(["ZIP", "PDF"], label="保存形式", value="ZIP", scale=2)
        with gr.Row():
            dl_workers_slider = gr.Slider(1, 16, value=8, step=1,
                                           label="⚡ 並列ダウンロード数", scale=3)
            skip_chk = gr.Checkbox(label="⏭️ DL済みはスキップ", value=True, scale=2)
            upload_chk = gr.Checkbox(label="☁️ Google Driveにアップ", scale=2)
        dl_btn = gr.Button("⬇️ ダウンロード開始", variant="primary", size="lg")
        dl_result = gr.Textbox(label="結果", lines=10, interactive=False)
        dl_file = gr.File(label="ダウンロードしたファイル")

        search_candidates.change(
            fill_id_from_candidate,
            inputs=[search_candidates],
            outputs=[gallery_id_input],
        )
        dl_btn.click(
            do_download,
            inputs=[gallery_id_input, fmt_radio, upload_chk, skip_chk, dl_workers_slider],
            outputs=[dl_result, dl_file],
        )

    return gallery_id_input


# ──────────────────────────────────────────────
# タブ3: 🗄️ DB管理
# ──────────────────────────────────────────────

def do_db_update(cats, workers_val, progress=gr.Progress()):
    if not cats:
        return "⚠️ 少なくとも1つのカテゴリを選択してください。"

    cat_map = {
        "🎨 Artists":    "artists",
        "👥 Groups":     "groups",
        "🏷️ Tags":       "tags",
        "📺 Parodies":   "parodies",
        "👤 Characters": "characters",
    }
    selected = [cat_map[c] for c in cats if c in cat_map]
    session = get_session(load_config()["user_agent"])
    conn = get_conn()
    log_lines = []
    workers = int(workers_val)

    # 各カテゴリの最終ページを事前取得
    log_lines.append(f"📡 最終ページ数を確認中... (並列数: {workers})")
    last_pages = {}
    for cat in selected:
        lp = get_category_last_page(session, cat)
        last_pages[cat] = lp
        log_lines.append(f"  {cat}: {lp} ページ")

    total_pages = sum(last_pages.values())
    global_done = [0]

    import time
    start_all = time.time()

    for ci, cat in enumerate(selected):
        lp = last_pages[cat]
        cat_done = [0]
        cat_total = [0]
        log_lines.append(f"\n🔄 {cat} 取得開始（{lp}p × {workers}並列）")

        def cb(done, total, category=cat, max_p=lp):
            cat_done[0] = done
            cat_total[0] = total
            global_done[0] += 1
            pct = global_done[0] / total_pages
            elapsed = time.time() - start_all
            eta = (elapsed / global_done[0]) * (total_pages - global_done[0]) if global_done[0] else 0
            progress(
                min(pct, 0.99),
                desc=f"🔄 {category} [{done}/{total}]  ETA: {eta:.0f}s"
            )

        n = scrape_category_all_parallel(session, conn, cat, workers=workers, progress_cb=cb)
        elapsed = time.time() - start_all
        log_lines.append(f"✅ {cat}: {n:,} 件保存  ({elapsed:.0f}s 経過)")

    progress(1.0, desc="✅ 完了!")
    log_lines.append(f"\n⏱ 合計時間: {time.time() - start_all:.0f}s")
    log_lines.append("\n" + fmt_stats())
    return "\n".join(log_lines)


def do_db_export():
    """DBをJSONに変換してブラウザからダウンロードできるファイルパスを返す"""
    conn = get_conn()
    import tempfile, os
    # 一時ファイルに書き出す（Gradioがファイルとして返せる場所）
    tmp_path = f"/tmp/imhentai_db_export.json"
    try:
        export_db_json(conn, tmp_path)
        size = Path(tmp_path).stat().st_size / 1024
        msg = f"✅ エクスポート完了 ({size:.1f} KB) — ダウンロードボタンをクリック"
        return msg, tmp_path
    except Exception as e:
        return f"❌ エクスポート失敗: {e}", None


def db_tab():
    with gr.Tab("🗄️ DB管理"):
        gr.Markdown("## データベース管理")

        with gr.Row():
            with gr.Column(scale=3):
                gr.Markdown("### 📥 スクレイピング（全ページ自動取得）")
                cat_checkboxes = gr.CheckboxGroup(
                    ["🎨 Artists", "👥 Groups", "🏷️ Tags", "📺 Parodies", "👤 Characters"],
                    label="取得するカテゴリ",
                    value=["🎨 Artists", "👥 Groups"],
                )
                scrape_workers_slider = gr.Slider(1, 16, value=8, step=1,
                                                   label="⚡ 並列スクレイピング数")
                gr.Markdown(
                    "_⚠️ 全ページ取得は時間がかかります_\n"
                    "_(Artists≈1057p / Groups≈638p / Tags≈338p / Parodies≈120p / Characters≈516p)_\n"
                    "_8並列推奨: Artists全件≈約5分_",
                )
                update_btn = gr.Button("🔄 スクレイピング開始（全ページ）", variant="primary")

            with gr.Column(scale=2):
                gr.Markdown("### 📤 エクスポート & 統計")
                export_btn = gr.Button("📤 JSONをダウンロード", variant="secondary")
                export_file = gr.File(label="ダウンロード", visible=True)
                stats_btn = gr.Button("📊 統計を更新", variant="secondary")

        db_log = gr.Textbox(label="ログ / 統計", lines=18, interactive=False,
                            value=fmt_stats)

        update_btn.click(do_db_update, [cat_checkboxes, scrape_workers_slider], [db_log])
        export_btn.click(
            do_db_export,
            inputs=[],
            outputs=[db_log, export_file],
        )
        stats_btn.click(lambda: fmt_stats(), outputs=[db_log])


# ──────────────────────────────────────────────
# タブ4: 📋 ダウンロード履歴
# ──────────────────────────────────────────────

def load_history(filter_dl="全て"):
    conn = get_conn()
    if filter_dl == "✅ DL済みのみ":
        where = "WHERE downloaded=1"
    elif filter_dl == "⬜ 未DLのみ":
        where = "WHERE downloaded=0"
    else:
        where = ""

    rows = conn.execute(
        f"SELECT id, title, artist, pages, downloaded, file_path, created_at "
        f"FROM galleries {where} ORDER BY created_at DESC LIMIT 200"
    ).fetchall()

    if not rows:
        return [["—", "（履歴なし）", "—", 0, "—", "—", "—"]]

    return [
        [
            r["id"],
            r["title"],
            r["artist"] or "—",
            r["pages"],
            "✅ 済" if r["downloaded"] else "⬜ 未",
            r["file_path"] or "—",
            (r["created_at"] or "")[:16],
        ]
        for r in rows
    ]


def history_tab():
    with gr.Tab("📋 ダウンロード履歴"):
        gr.Markdown("## ダウンロード履歴")
        with gr.Row():
            filter_radio = gr.Radio(
                ["全て", "✅ DL済みのみ", "⬜ 未DLのみ"],
                label="フィルター",
                value="全て",
                scale=3,
            )
            refresh_btn = gr.Button("🔄 更新", scale=1)

        history_table = gr.Dataframe(
            headers=["ID", "タイトル", "アーティスト", "ページ", "状態", "ファイルパス", "登録日時"],
            datatype=["number", "str", "str", "number", "str", "str", "str"],
            value=load_history,
            interactive=False,
            wrap=True,
            row_count=20,
        )

        refresh_btn.click(load_history, inputs=[filter_radio], outputs=[history_table])
        filter_radio.change(load_history, inputs=[filter_radio], outputs=[history_table])


# ──────────────────────────────────────────────
# アプリ構築
# ──────────────────────────────────────────────

import re

def build_app():
    with gr.Blocks(title="imhentai-manager") as app:
        gr.HTML("""
        <div class="app-header" style="text-align:center;padding:20px 0 10px;border-bottom:2px solid #e2e8f0;margin-bottom:12px;">
            <h1 style="font-size:1.8rem;margin:0;">📚 imhentai-manager</h1>
            <p style="color:#64748b;margin:4px 0 0;font-size:0.95rem;">imhentai.xxx の検索・ダウンロード管理ツール</p>
        </div>
        """)

        candidates = search_tab()
        download_tab(candidates)
        db_tab()
        history_tab()

    return app


if __name__ == "__main__":
    app = build_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=8080,
        share=False,
        inbrowser=False,
        theme=gr.themes.Soft(
            primary_hue="indigo",
            secondary_hue="slate",
            neutral_hue="slate",
        ),
        css=CUSTOM_CSS,
        favicon_path=None,
    )
