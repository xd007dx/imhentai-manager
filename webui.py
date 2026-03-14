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
    scrape_category_page, scrape_gallery_metadata,
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

def do_download(gallery_id_str, fmt, upload_drive, skip_if_exists, progress=gr.Progress()):
    gid_str = str(gallery_id_str).strip()

    # IDの抽出（"ID:123456 - Title" 形式にも対応）
    m = re.search(r"\d+", gid_str)
    if not m:
        return "❌ 有効なギャラリーIDを入力してください。", None

    import re
    gid = int(m.group())
    cfg = load_config()
    session = get_session(cfg["user_agent"])
    conn = get_conn()
    rl = cfg["rate_limit"]
    out_dir = cfg["download_dir"]

    # 重複スキップチェック
    if skip_if_exists and is_downloaded(gid):
        row = conn.execute("SELECT title, file_path FROM galleries WHERE id=?", (gid,)).fetchone()
        title = row["title"] if row else f"Gallery {gid}"
        fp = row["file_path"] if row else ""
        return f"⏭️  スキップ: 「{title}」はダウンロード済みです\n  ファイル: {fp}", None

    # メタデータ取得
    progress(0.1, desc="📖 メタデータ取得中...")
    try:
        meta = scrape_gallery_metadata(session, conn, gid, rl["min_delay"], rl["max_delay"])
    except Exception as e:
        return f"❌ メタデータ取得失敗: {e}", None

    if not meta:
        return f"❌ ギャラリー {gid} が見つかりませんでした (404)", None

    info = (
        f"📖 タイトル : {meta['title']}\n"
        f"🎨 アーティスト: {meta['artist'] or '不明'}\n"
        f"📄 ページ数 : {meta['pages']}\n"
        f"🏷️  タグ     : {(meta['tags'] or '')[:80]}{'...' if len(meta.get('tags',''))>80 else ''}\n"
    )

    # 進捗コールバック
    def cb(done, total):
        progress(0.2 + 0.7 * done / total,
                 desc=f"⬇️  ダウンロード中... [{done}/{total}]")

    # ダウンロード
    progress(0.2, desc="⬇️  ダウンロード開始...")
    try:
        if fmt == "ZIP":
            file_path = download_gallery_zip(session, gid, out_dir,
                                              rl["min_delay"], rl["max_delay"], cb)
        else:
            file_path = download_gallery_pdf(session, gid, out_dir,
                                              rl["min_delay"], rl["max_delay"], cb)
    except Exception as e:
        return f"{info}\n❌ ダウンロード失敗: {e}", None

    # DB更新
    conn.execute(
        "UPDATE galleries SET downloaded=1, file_path=? WHERE id=?",
        (file_path, gid)
    )
    conn.commit()

    msg = f"{info}\n✅ 保存完了: {file_path}"

    # Google Driveアップロード
    if upload_drive:
        progress(0.95, desc="☁️  Google Drive アップロード中...")
        gdrive_cfg = cfg["gdrive"]
        link = upload_to_gdrive(
            file_path,
            folder_id=gdrive_cfg.get("folder_id", ""),
            credentials_file=gdrive_cfg.get("credentials_file", "credentials.json"),
            delete_old=True,
        )
        if link:
            msg += f"\n☁️  Drive: {link}"
        else:
            msg += "\n⚠️  Google Drive未設定 (credentials.json が必要)"

    progress(1.0, desc="✅ 完了!")
    return msg, file_path


def download_tab(search_candidates):
    import re as _re

    def fill_id_from_candidate(candidate):
        """検索タブの候補選択 → IDフィールドに自動入力"""
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
            skip_chk = gr.Checkbox(label="⏭️ DL済みはスキップ", value=True, scale=2)
            upload_chk = gr.Checkbox(label="☁️ Google Driveにアップ", scale=2)
        dl_btn = gr.Button("⬇️ ダウンロード開始", variant="primary", size="lg")
        dl_result = gr.Textbox(label="結果", lines=10, interactive=False)
        dl_file = gr.File(label="ダウンロードしたファイル")

        # 検索タブからの候補選択で自動入力
        search_candidates.change(
            fill_id_from_candidate,
            inputs=[search_candidates],
            outputs=[gallery_id_input],
        )

        dl_btn.click(
            do_download,
            inputs=[gallery_id_input, fmt_radio, upload_chk, skip_chk],
            outputs=[dl_result, dl_file],
        )

    return gallery_id_input


# ──────────────────────────────────────────────
# タブ3: 🗄️ DB管理
# ──────────────────────────────────────────────

def do_db_update(cats, progress=gr.Progress()):
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
    rl = load_config()["rate_limit"]
    log_lines = []

    # まず各カテゴリの最終ページ数を取得
    log_lines.append("📡 最終ページ数を確認中...")
    last_pages = {}
    for cat in selected:
        lp = get_category_last_page(session, cat)
        last_pages[cat] = lp
        log_lines.append(f"  {cat}: {lp} ページ")

    total_steps = sum(last_pages.values())
    step = 0

    for cat in selected:
        total = 0
        max_page = last_pages[cat]
        log_lines.append(f"\n🔄 {cat} 取得開始（全 {max_page} ページ）")

        for page in range(1, max_page + 1):
            step += 1
            progress(
                step / total_steps,
                desc=f"🔄 {cat} [{page}/{max_page}]..."
            )
            try:
                n = scrape_category_page(session, conn, cat, page,
                                          rl["min_delay"], rl["max_delay"])
                total += n
                if n == 0:
                    log_lines.append(f"  p{page}: 結果なし → 終了")
                    break
                # 10ページごとにログ出力
                if page % 10 == 0 or page == max_page:
                    log_lines.append(f"  p{page}/{max_page}: 累計 {total:,} 件")
            except Exception as e:
                log_lines.append(f"  ⚠️ p{page} エラー: {e}")
                break

        log_lines.append(f"✅ {cat}: 合計 {total:,} 件保存")

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
                gr.Markdown(
                    "_⚠️ 全ページ取得は時間がかかります_\n"
                    "_(Artists≈1057p / Groups≈638p / Tags≈338p / Parodies≈120p / Characters≈516p)_",
                )
                update_btn = gr.Button("🔄 スクレイピング開始（全ページ）", variant="primary")

            with gr.Column(scale=2):
                gr.Markdown("### 📤 エクスポート & 統計")
                export_btn = gr.Button("📤 JSONをダウンロード", variant="secondary")
                export_file = gr.File(label="ダウンロード", visible=True)
                stats_btn = gr.Button("📊 統計を更新", variant="secondary")

        db_log = gr.Textbox(label="ログ / 統計", lines=18, interactive=False,
                            value=fmt_stats)

        update_btn.click(do_db_update, [cat_checkboxes], [db_log])
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
