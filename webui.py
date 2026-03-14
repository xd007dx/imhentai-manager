#!/usr/bin/env python3
"""
imhentai-manager Web UI
Gradio ベースのインターフェース

タブ構成:
  1. 🔍 検索
  2. ⬇️ ダウンロード
  3. 🗄️ DB管理
  4. 📋 ダウンロード履歴
"""

import gradio as gr
import logging
import json
from pathlib import Path
import yaml

# コア機能をインポート
from imhentai import (
    init_db, export_db_json, get_session,
    scrape_category_page, scrape_gallery_metadata,
    search_db, download_gallery_zip, download_gallery_pdf,
    upload_to_gdrive,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 設定読み込み
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

def get_session_cfg():
    cfg = load_config()
    return get_session(cfg["user_agent"]), cfg


# ──────────────────────────────────────────────
# タブ1: 🔍 検索
# ──────────────────────────────────────────────

def do_search(query, search_type):
    if not query.strip():
        return "クエリを入力してください。", gr.update(choices=[], visible=False)

    conn = get_conn()
    type_map = {"全体": "all", "アーティスト": "artist", "作品タイトル": "title"}
    results = search_db(conn, query.strip(), type_map.get(search_type, "all"))

    if not results:
        return "❌ 結果が見つかりませんでした。", gr.update(choices=[], visible=False)

    fuzzy = any(r["type"].endswith("_fuzzy") for r in results)
    lines = []
    choices = []

    if fuzzy:
        lines.append("⚠️ 完全一致が見つかりませんでした。近似候補:\n")
    else:
        lines.append(f"✅ {len(results)} 件見つかりました:\n")

    for i, r in enumerate(results, 1):
        if r["type"] in ("artist", "artist_fuzzy"):
            score = f" (類似度: {r.get('score', 0):.0%})" if fuzzy else ""
            line = f"{i}. [アーティスト]{score}  {r['name']}  (作品数: {r.get('count', '?')})"
            lines.append(line)
            choices.append(r["name"])
        elif r["type"] in ("gallery", "gallery_fuzzy"):
            score = f" (類似度: {r.get('score', 0):.0%})" if fuzzy else ""
            line = f"{i}. [ギャラリー]{score}  ID:{r['id']}  {r['title']}"
            if r.get("artist"):
                line += f"\n      Artist: {r['artist']}"
            lines.append(line)
            choices.append(f"ID:{r['id']} - {r['title']}")

    return "\n".join(lines), gr.update(choices=choices, visible=bool(choices))


def search_tab():
    with gr.Tab("🔍 検索"):
        gr.Markdown("## アーティスト・作品を検索")
        with gr.Row():
            query_input = gr.Textbox(
                label="検索クエリ",
                placeholder="例: Kariya, Tekoki Maniax ...",
                scale=4,
            )
            search_type = gr.Radio(
                ["全体", "アーティスト", "作品タイトル"],
                label="検索対象",
                value="全体",
                scale=2,
            )
        search_btn = gr.Button("🔍 検索", variant="primary")
        result_text = gr.Textbox(label="検索結果", lines=12, interactive=False)
        candidates = gr.Radio(label="候補から選択（ダウンロードタブに転送）",
                               visible=False, interactive=True)

        search_btn.click(
            do_search,
            inputs=[query_input, search_type],
            outputs=[result_text, candidates],
        )
        query_input.submit(
            do_search,
            inputs=[query_input, search_type],
            outputs=[result_text, candidates],
        )
    return candidates


# ──────────────────────────────────────────────
# タブ2: ⬇️ ダウンロード
# ──────────────────────────────────────────────

def do_download(gallery_id, fmt, upload_drive, progress=gr.Progress()):
    if not str(gallery_id).strip().isdigit():
        return "❌ 有効なギャラリーIDを入力してください。", None

    cfg = load_config()
    session, _ = get_session_cfg()
    conn = get_conn()
    rl = cfg["rate_limit"]
    gid = int(gallery_id)

    progress(0, desc="メタデータ取得中...")
    try:
        meta = scrape_gallery_metadata(session, conn, gid, rl["min_delay"], rl["max_delay"])
    except Exception as e:
        return f"❌ メタデータ取得失敗: {e}", None

    meta_text = ""
    if meta:
        meta_text = (
            f"📖 タイトル: {meta['title']}\n"
            f"🎨 アーティスト: {meta['artist']}\n"
            f"📄 ページ数: {meta['pages']}\n"
            f"🏷️ タグ: {meta['tags'][:80]}{'...' if len(meta.get('tags','')) > 80 else ''}\n\n"
        )

    progress(0.3, desc=f"{fmt.upper()} ダウンロード中...")
    try:
        if fmt == "ZIP":
            file_path = download_gallery_zip(
                session, gid, cfg["download_dir"], rl["min_delay"], rl["max_delay"]
            )
        else:
            file_path = download_gallery_pdf(
                session, gid, cfg["download_dir"], rl["min_delay"], rl["max_delay"]
            )
    except Exception as e:
        return f"{meta_text}❌ ダウンロード失敗: {e}", None

    conn.execute("UPDATE galleries SET downloaded=1 WHERE id=?", (gid,))
    conn.commit()

    msg = f"{meta_text}✅ 保存完了: {file_path}"

    if upload_drive:
        progress(0.9, desc="Google Drive にアップロード中...")
        gdrive_cfg = cfg["gdrive"]
        upload_to_gdrive(
            file_path,
            folder_id=gdrive_cfg.get("folder_id", ""),
            credentials_file=gdrive_cfg.get("credentials_file", "credentials.json"),
            delete_old=True,
        )
        msg += "\n⚠️ Google Drive連携は未設定です（credentials.json が必要）"

    progress(1.0, desc="完了!")
    return msg, file_path


def download_tab():
    with gr.Tab("⬇️ ダウンロード"):
        gr.Markdown("## ギャラリーをダウンロード")
        with gr.Row():
            gallery_id_input = gr.Textbox(
                label="ギャラリーID",
                placeholder="例: 123456",
                scale=3,
            )
            fmt_radio = gr.Radio(["ZIP", "PDF"], label="保存形式", value="ZIP", scale=2)
            upload_drive_chk = gr.Checkbox(label="Google Drive にアップロード", scale=2)
        dl_btn = gr.Button("⬇️ ダウンロード開始", variant="primary")
        dl_result = gr.Textbox(label="結果", lines=8, interactive=False)
        dl_file = gr.File(label="ダウンロードしたファイル", visible=True)

        dl_btn.click(
            do_download,
            inputs=[gallery_id_input, fmt_radio, upload_drive_chk],
            outputs=[dl_result, dl_file],
        )
    return gallery_id_input


# ──────────────────────────────────────────────
# タブ3: 🗄️ DB管理
# ──────────────────────────────────────────────

def do_db_update(cats, pages, progress=gr.Progress()):
    if not cats:
        return "⚠️ 少なくとも1つのカテゴリを選択してください。"

    cat_map = {
        "Artists": "artists", "Groups": "groups", "Tags": "tags",
        "Parodies": "parodies", "Characters": "characters",
    }
    selected = [cat_map[c] for c in cats if c in cat_map]
    session, cfg = get_session_cfg()
    conn = get_conn()
    rl = cfg["rate_limit"]
    log_lines = []

    for ci, cat in enumerate(selected):
        total = 0
        for page in range(1, pages + 1):
            progress(
                (ci * pages + page) / (len(selected) * pages),
                desc=f"{cat} page {page}/{pages}..."
            )
            try:
                n = scrape_category_page(session, conn, cat, page,
                                          rl["min_delay"], rl["max_delay"])
                total += n
                if n == 0:
                    log_lines.append(f"  {cat} page {page}: 結果なし（終了）")
                    break
                log_lines.append(f"  {cat} page {page}: {n} 件取得")
            except Exception as e:
                log_lines.append(f"  ⚠️ {cat} page {page} エラー: {e}")
                break
        log_lines.append(f"✅ {cat}: 合計 {total} 件保存\n")

    # DB統計
    stats = db_stats_text(conn)
    return "\n".join(log_lines) + "\n\n" + stats


def do_db_export(output_path):
    conn = get_conn()
    path = output_path.strip() or "./data/db.json"
    try:
        export_db_json(conn, path)
        return f"✅ エクスポート完了: {path}"
    except Exception as e:
        return f"❌ エクスポート失敗: {e}"


def db_stats_text(conn=None):
    if conn is None:
        conn = get_conn()
    lines = ["📊 **データベース統計**"]
    for table in ["artists", "groups", "tags", "parodies", "characters", "galleries"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            downloaded = ""
            if table == "galleries":
                dl = conn.execute("SELECT COUNT(*) FROM galleries WHERE downloaded=1").fetchone()[0]
                downloaded = f" (DL済み: {dl})"
            lines.append(f"  {table}: {count} 件{downloaded}")
        except Exception:
            lines.append(f"  {table}: N/A")
    return "\n".join(lines)


def db_tab():
    with gr.Tab("🗄️ DB管理"):
        gr.Markdown("## データベース管理")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### スクレイピング")
                cat_checkboxes = gr.CheckboxGroup(
                    ["Artists", "Groups", "Tags", "Parodies", "Characters"],
                    label="取得するカテゴリ",
                    value=["Artists", "Groups"],
                )
                pages_slider = gr.Slider(1, 50, value=5, step=1, label="取得ページ数")
                update_btn = gr.Button("🔄 スクレイピング開始", variant="primary")

            with gr.Column():
                gr.Markdown("### JSONエクスポート")
                export_path = gr.Textbox(
                    label="出力ファイルパス",
                    value="./data/db.json",
                )
                export_btn = gr.Button("📤 エクスポート")
                stats_btn = gr.Button("📊 統計を表示")

        db_log = gr.Textbox(label="ログ / 統計", lines=15, interactive=False)

        update_btn.click(do_db_update, inputs=[cat_checkboxes, pages_slider], outputs=[db_log])
        export_btn.click(do_db_export, inputs=[export_path], outputs=[db_log])
        stats_btn.click(lambda: db_stats_text(), outputs=[db_log])


# ──────────────────────────────────────────────
# タブ4: 📋 ダウンロード履歴
# ──────────────────────────────────────────────

def load_history():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, title, artist, pages, downloaded, created_at FROM galleries ORDER BY created_at DESC LIMIT 100"
    ).fetchall()
    if not rows:
        return [["（履歴なし）", "", "", "", "", ""]]
    return [[r["id"], r["title"], r["artist"] or "", r["pages"],
             "✅" if r["downloaded"] else "❌", r["created_at"]] for r in rows]


def history_tab():
    with gr.Tab("📋 ダウンロード履歴"):
        gr.Markdown("## ダウンロード履歴")
        refresh_btn = gr.Button("🔄 更新")
        history_table = gr.Dataframe(
            headers=["ID", "タイトル", "アーティスト", "ページ数", "DL済み", "日時"],
            datatype=["number", "str", "str", "number", "str", "str"],
            value=load_history,
            interactive=False,
            wrap=True,
        )
        refresh_btn.click(load_history, outputs=[history_table])


# ──────────────────────────────────────────────
# アプリ組み立て
# ──────────────────────────────────────────────

def build_app():
    with gr.Blocks(title="imhentai-manager") as app:
        gr.Markdown(
            """
            # 📚 imhentai-manager
            imhentai.com の検索・ダウンロード管理ツール
            """
        )

        search_tab()
        download_tab()
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
        theme=gr.themes.Soft(),
        css="""
        .gradio-container { max-width: 1100px; margin: auto; }
        h1 { text-align: center; }
        """,
    )
