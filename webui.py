#!/usr/bin/env python3
"""
imhentai-manager Web UI (Gradio)
ピンク×ブラック ダークテーマ
"""

import gradio as gr
import logging
import json
import re
import threading
from pathlib import Path
from datetime import datetime
import yaml

from imhentai import (
    init_db, export_db_json, db_stats,
    get_session, get_category_last_page,
    scrape_category_all_parallel,
    scrape_gallery_metadata,
    search_db,
    search_site, search_category_site,
    download_gallery_zip, download_gallery_pdf,
    upload_to_gdrive,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────
DEFAULT_CONFIG = {
    "download_dir": "./downloads",
    "database_path": "./data/imhentai.db",
    "rate_limit": {"min_delay": 0.5, "max_delay": 1.0},
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
# CSS (ピンク×ブラック ダークテーマ)
# ──────────────────────────────────────────────
DARK_CSS = """
/* ── Base ── */
body { background: #0d0d1a !important; }
.gradio-container {
    background: #0d0d1a !important;
    color: #f0f0f0 !important;
    max-width: 1200px !important;
    margin: 0 auto !important;
    font-family: 'Segoe UI', sans-serif !important;
}

/* ── Header ── */
.app-header {
    text-align: center;
    padding: 24px 0 12px;
    border-bottom: 2px solid #ff69b4;
    margin-bottom: 16px;
}
.app-header h1 {
    font-size: 2rem;
    margin: 0;
    color: #ff69b4;
    text-shadow: 0 0 20px rgba(255,105,180,0.6);
}
.app-header p { color: #a0a0c0; margin: 4px 0 0; }

/* ── Tabs ── */
.tab-nav { border-bottom: 2px solid #2d2d4e !important; }
.tab-nav button {
    background: #1a1a2e !important;
    color: #a0a0c0 !important;
    border: none !important;
    border-bottom: 3px solid transparent !important;
    font-size: 1rem !important;
    padding: 10px 20px !important;
    transition: all 0.2s;
}
.tab-nav button:hover { color: #ff69b4 !important; }
.tab-nav button.selected {
    color: #ff69b4 !important;
    border-bottom: 3px solid #ff69b4 !important;
    background: #1a1a2e !important;
}

/* ── Blocks / Panels ── */
.block, .panel, .form {
    background: #1a1a2e !important;
    border: 1px solid #2d2d4e !important;
    border-radius: 10px !important;
}
label { color: #c0c0e0 !important; }

/* ── Inputs ── */
input[type=text], input[type=number], textarea, select {
    background: #0d0d1a !important;
    color: #f0f0f0 !important;
    border: 1px solid #3d3d6e !important;
    border-radius: 6px !important;
}
input[type=text]:focus, textarea:focus {
    border-color: #ff69b4 !important;
    box-shadow: 0 0 8px rgba(255,105,180,0.3) !important;
}

/* ── Buttons ── */
button.primary, .btn-primary {
    background: linear-gradient(135deg, #ff69b4, #c2185b) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: bold !important;
    transition: all 0.2s;
}
button.primary:hover { box-shadow: 0 0 15px rgba(255,105,180,0.5) !important; }
button.secondary {
    background: #2d2d4e !important;
    color: #ff69b4 !important;
    border: 1px solid #ff69b4 !important;
    border-radius: 8px !important;
}

/* ── Sliders / Checkboxes ── */
input[type=range] { accent-color: #ff69b4; }
input[type=checkbox] { accent-color: #ff69b4; }
input[type=radio] { accent-color: #ff69b4; }

/* ── Table ── */
.dataframe table { background: #1a1a2e !important; }
.dataframe th { background: #2d2d4e !important; color: #ff69b4 !important; }
.dataframe td { background: #1a1a2e !important; color: #f0f0f0 !important; border-color: #2d2d4e !important; }

/* ── Gallery Grid ── */
.gallery-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    padding: 8px;
    background: transparent;
}
.gallery-card {
    position: relative;
    width: 150px;
    background: #1a1a2e;
    border: 1px solid #2d2d4e;
    border-radius: 8px;
    overflow: visible;
    cursor: pointer;
    transition: transform 0.2s, box-shadow 0.2s;
}
.gallery-card:hover {
    transform: scale(1.04);
    border-color: #ff69b4;
    box-shadow: 0 0 12px rgba(255,105,180,0.4);
    z-index: 10;
}
.gallery-card img.thumb {
    width: 150px;
    height: 210px;
    object-fit: cover;
    border-radius: 8px 8px 0 0;
    display: block;
}
.gallery-card .card-title {
    font-size: 0.7rem;
    color: #d0d0f0;
    padding: 4px 6px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 150px;
}
.gallery-card:hover .popup { display: block; }
.popup {
    display: none;
    position: absolute;
    top: 0;
    left: 165px;
    z-index: 999;
    background: #1a1a2e;
    border: 2px solid #ff69b4;
    border-radius: 10px;
    padding: 10px;
    width: 260px;
    box-shadow: 0 0 20px rgba(255,105,180,0.4);
}
.popup img { width: 240px; border-radius: 6px; }
.popup .pop-title { color: #f0f0f0; font-size: 0.8rem; margin-top: 6px; line-height: 1.3; }
.popup .pop-id { color: #ff69b4; font-size: 0.75rem; }
.dl-btn {
    background: linear-gradient(135deg, #ff69b4, #c2185b);
    color: white;
    border: none;
    width: 100%;
    padding: 5px;
    border-radius: 0 0 8px 8px;
    cursor: pointer;
    font-size: 0.75rem;
    font-weight: bold;
}
.dl-btn:hover { background: linear-gradient(135deg, #ff85c8, #e91e8c); }
.db-result-section {
    background: #1a1a2e;
    border: 1px solid #2d2d4e;
    border-radius: 10px;
    padding: 12px;
}
.db-result-item {
    padding: 8px 12px;
    margin: 4px 0;
    background: #0d0d1a;
    border: 1px solid #3d3d6e;
    border-radius: 6px;
    cursor: pointer;
    transition: border-color 0.2s;
}
.db-result-item:hover { border-color: #ff69b4; color: #ff69b4; }
.type-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: bold;
    margin-right: 6px;
}
.type-tag { background: #6a0f4a; color: #ffb3d9; }
.type-artist { background: #0f3a6a; color: #b3d9ff; }
.type-group { background: #0f6a3a; color: #b3ffd9; }
.type-parody { background: #6a4a0f; color: #ffd9b3; }
.type-character { background: #4a0f6a; color: #d9b3ff; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0d0d1a; }
::-webkit-scrollbar-thumb { background: #ff69b4; border-radius: 3px; }

/* ── Mobile ── */
@media (max-width: 768px) {
    .gradio-container { padding: 8px !important; }
    .gallery-card { width: 120px; }
    .gallery-card img.thumb { width: 120px; height: 170px; }
    .popup { left: 0; top: 180px; width: 200px; }
    .tab-nav button { padding: 8px 12px !important; font-size: 0.85rem !important; }
    .app-header h1 { font-size: 1.4rem; }
}
@media (max-width: 480px) {
    .gallery-grid { gap: 8px; }
    .gallery-card { width: 100px; }
    .gallery-card img.thumb { width: 100px; height: 140px; }
}
"""

# ──────────────────────────────────────────────
# Gallery HTML生成
# ──────────────────────────────────────────────

def make_gallery_html(galleries: list, show_dl_btn: bool = True) -> str:
    if not galleries:
        return '<div style="color:#a0a0c0;padding:20px;text-align:center;">結果がありません</div>'

    cards = []
    for g in galleries:
        thumb = g.get("thumb_url", "")
        title = g.get("title", "").replace('"', "&quot;").replace("<", "&lt;").replace("'", "&#39;")
        gid = g.get("id", 0)
        # DLボタン: クリックでGradio Textbox(#click-gid-box input)にIDを書き込む
        dl_btn = (
            f'<button class="dl-btn" data-gid="{gid}" '
            f'onclick="var b=this,g=b.getAttribute(\'data-gid\'),'
            f'i=document.querySelector(\'#click-gid-box input,#click-gid-box textarea\');'
            f'if(i){{i.value=g;i.dispatchEvent(new Event(\'input\',{{bubbles:true}}));}}'
            f'b.textContent=\'✓ \'+g;">⬇ DL</button>'
        ) if show_dl_btn else ""

        cards.append(
            f'<div class="gallery-card">'
            f'<img class="thumb" src="{thumb}" alt="{title}" loading="lazy" onerror="this.style.visibility=\'hidden\'" />'
            f'<div class="popup">'
            f'<img src="{thumb}" onerror="this.style.visibility=\'hidden\'" />'
            f'<div class="pop-id">ID: {gid}</div>'
            f'<div class="pop-title">{title}</div>'
            f'</div>'
            f'<div class="card-title" title="{title}">{title}</div>'
            f'{dl_btn}'
            f'</div>'
        )

    return f'<div class="gallery-grid">{"".join(cards)}</div>'


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def fmt_stats() -> str:
    conn = get_conn()
    s = db_stats(conn)
    return "\n".join([
        "📊 **データベース統計**",
        f"  🎨 Artists:    {s.get('artists',0):,}",
        f"  👥 Groups:     {s.get('groups',0):,}",
        f"  🏷️  Tags:       {s.get('tags',0):,}",
        f"  📺 Parodies:   {s.get('parodies',0):,}",
        f"  👤 Characters: {s.get('characters',0):,}",
        f"  📚 Galleries:  {s.get('galleries',0):,} (DL済み: {s.get('downloaded',0):,})",
    ])

def is_downloaded(gallery_id: int) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT downloaded, file_path FROM galleries WHERE id=?", (gallery_id,)).fetchone()
    if row and row["downloaded"] and row["file_path"] and Path(row["file_path"]).exists():
        return True
    return False

def do_download_by_id(gallery_id_str, fmt="ZIP", workers=8, progress=gr.Progress()):
    """IDを指定してダウンロード（検索タブから呼ばれる）"""
    if not gallery_id_str or not str(gallery_id_str).strip():
        return "IDが指定されていません"
    m = re.search(r"\d+", str(gallery_id_str))
    if not m:
        return "❌ 無効なID"
    gid = int(m.group())
    cfg = load_config()
    session = get_session(cfg["user_agent"])
    conn = get_conn()

    progress(0.05, desc="📖 メタデータ取得中...")
    meta = scrape_gallery_metadata(session, conn, gid)
    if not meta:
        return f"❌ ギャラリー {gid} が見つかりません"

    info = f"📖 {meta['title']}\n🎨 {meta['artist'] or '不明'}  📄 {meta['pages']}p"

    def cb(done, total):
        progress(0.1 + 0.85 * done / total, desc=f"⬇️ [{done}/{total}] {workers}並列...")

    progress(0.1, desc="⬇️ ダウンロード中...")
    try:
        if fmt == "ZIP":
            fp = download_gallery_zip(session, gid, cfg["download_dir"], workers, cb)
        else:
            fp = download_gallery_pdf(session, gid, cfg["download_dir"], workers, cb)
    except Exception as e:
        return f"{info}\n❌ 失敗: {e}"

    conn.execute("UPDATE galleries SET downloaded=1, file_path=? WHERE id=?", (fp, gid))
    conn.commit()
    progress(1.0, desc="✅ 完了!")
    return f"{info}\n✅ 保存: {fp}"


# ──────────────────────────────────────────────
# Tab 1: 🔍 検索 (全面改修)
# ──────────────────────────────────────────────

def do_search(keyword, page_state, progress=gr.Progress()):
    """サイト検索 + DB検索を同時実行"""
    if not keyword.strip():
        return gr.update(value=make_gallery_html([])), "クエリを入力してください", 1, gr.update(choices=[], visible=False)

    session = get_session(load_config()["user_agent"])
    conn = get_conn()
    page = int(page_state) if page_state else 1

    # 並列実行（DBは各スレッドで独立接続）
    site_results = [None]
    db_results = [None]

    def run_site():
        site_results[0] = search_site(session, keyword, page)

    def run_db():
        # SQLiteはスレッドごとに接続を作る
        conn_t = init_db(load_config()["database_path"])
        db_results[0] = search_db(conn_t, keyword, "all")
        conn_t.close()

    t1 = threading.Thread(target=run_site)
    t2 = threading.Thread(target=run_db)
    t1.start(); t2.start()
    t1.join(); t2.join()

    # サイト検索HTML
    site_html_content = make_gallery_html(site_results[0] or [])

    # DB検索テキスト + ラジオ候補
    db_items = db_results[0] or []
    fuzzy = any(r["type"].endswith("_fuzzy") for r in db_items)
    db_lines = []
    choices = []

    if not db_items:
        db_lines.append("DBに該当なし")
    else:
        if fuzzy:
            db_lines.append(f"⚠️ 近似候補:")
        else:
            db_lines.append(f"✅ {len(db_items)} 件:")

        type_icon = {"artist":"🎨","artist_fuzzy":"🎨","group":"👥","group_fuzzy":"👥",
                     "tag":"🏷️","tag_fuzzy":"🏷️","gallery":"📚","gallery_fuzzy":"📚"}

        for r in db_items:
            t = r["type"]
            icon = type_icon.get(t, "📌")
            score = f" ({r.get('score',0):.0%})" if fuzzy else ""
            if "gallery" in t:
                label = f"{icon} [Gallery]{score} ID:{r['id']} {r['name'] if 'name' in r else r.get('title','')}"
            else:
                cat = t.replace("_fuzzy","")
                label = f"{icon} [{cat.upper()}]{score} {r['name']}  ({r.get('count','?')} 件)"
                choices.append(f"{cat}|{r['name']}")
            db_lines.append(label)

    db_text = "\n".join(db_lines)
    radio_update = gr.update(choices=choices, visible=bool(choices), value=None)
    return gr.update(value=site_html_content), db_text, page, radio_update


def do_db_item_click(selected, progress=gr.Progress()):
    """DB検索結果のカテゴリ選択 → そのカテゴリのギャラリーをサイトから取得して表示"""
    if not selected:
        return gr.update(value=make_gallery_html([])), "カテゴリを選択してください"

    parts = selected.split("|", 1)
    if len(parts) != 2:
        return gr.update(value=make_gallery_html([])), "形式エラー"

    cat, name = parts
    slug = name.lower().replace(" ", "-")
    session = get_session(load_config()["user_agent"])

    cat_url_map = {
        "artist": "artist",
        "group": "group",
        "tag": "tag",
        "parody": "parody",
        "character": "character",
    }
    cat_slug = cat_url_map.get(cat, cat)

    galleries = search_category_site(session, cat_slug, slug, page=1)
    html_content = make_gallery_html(galleries)
    info = f"「{name}」({cat}) の結果: {len(galleries)} 件"
    return gr.update(value=html_content), info


def search_tab():
    with gr.Tab("🔍 検索"):
        gr.Markdown("## サイト検索 & DB検索")

        with gr.Row():
            search_input = gr.Textbox(
                placeholder="例: lolicon、Kariya、futanari...",
                label="検索ワード",
                scale=5,
            )
            search_btn = gr.Button("🔍 検索", variant="primary", scale=1)

        with gr.Row():
            dl_fmt = gr.Radio(["ZIP", "PDF"], label="DL形式", value="ZIP", scale=2)
            dl_workers = gr.Slider(1, 16, value=8, step=1, label="⚡ 並列数", scale=3)

        page_state = gr.State(value=1)

        with gr.Row():
            # 左: サイト検索結果
            with gr.Column(scale=3):
                gr.Markdown("### 🌐 サイト検索結果")
                gr.Markdown("_サムネにホバーで詳細表示 / IDをコピーしてマニュアルDLタブへ_")
                site_html = gr.HTML(
                    value='<div style="color:#a0a0c0;padding:20px;">検索してください</div>'
                )
                with gr.Row():
                    prev_btn = gr.Button("◀ 前へ", size="sm", scale=1)
                    page_label = gr.Textbox(value="1", label="ページ", scale=1, interactive=False)
                    next_btn = gr.Button("次へ ▶", size="sm", scale=1)

                # クリックされたギャラリーID入力欄（HTML内のボタンから入力）
                gr.Markdown("**⬇️ DLするギャラリーID:**")
                with gr.Row():
                    click_gid = gr.Textbox(
                        placeholder="サムネのDLボタン押下 or 手動入力",
                        label="",
                        scale=3,
                        elem_id="click-gid-box",
                    )
                    dl_now_btn = gr.Button("⬇ DL実行", variant="primary", scale=1)

            # 右: DB検索結果
            with gr.Column(scale=2):
                gr.Markdown("### 🗄️ DB検索結果")
                gr.Markdown("_カテゴリを選択 → 左側にそのギャラリー一覧を表示_")
                db_text = gr.Textbox(
                    label="DB検索結果",
                    lines=8,
                    interactive=False,
                    value="検索してください",
                )
                db_radio = gr.Radio(
                    label="📌 カテゴリを選択してギャラリー表示",
                    visible=False,
                    interactive=True,
                )

        dl_status = gr.Textbox(label="📥 ダウンロード状態", lines=4, interactive=False)

        # イベント
        def on_search(kw, ps):
            html, txt, pg, radio = do_search(kw, 1)
            return html, txt, 1, "1", radio

        def on_next(kw, ps):
            new_p = int(ps) + 1
            html, txt, pg, radio = do_search(kw, new_p)
            return html, txt, new_p, str(new_p), radio

        def on_prev(kw, ps):
            new_p = max(1, int(ps) - 1)
            html, txt, pg, radio = do_search(kw, new_p)
            return html, txt, new_p, str(new_p), radio

        def on_dl_now(gid_str, fmt, workers):
            if not gid_str or not gid_str.strip():
                return "IDを入力またはDLボタンを押してください"
            return do_download_by_id(gid_str.strip(), fmt, int(workers))

        def on_db_select(selected):
            html, info = do_db_item_click(selected)
            return html, info

        search_btn.click(on_search, [search_input, page_state],
                         [site_html, db_text, page_state, page_label, db_radio])
        search_input.submit(on_search, [search_input, page_state],
                            [site_html, db_text, page_state, page_label, db_radio])
        next_btn.click(on_next, [search_input, page_state],
                       [site_html, db_text, page_state, page_label, db_radio])
        prev_btn.click(on_prev, [search_input, page_state],
                       [site_html, db_text, page_state, page_label, db_radio])
        dl_now_btn.click(on_dl_now, [click_gid, dl_fmt, dl_workers], [dl_status])
        db_radio.change(on_db_select, [db_radio], [site_html, db_text])


# ──────────────────────────────────────────────
# Tab 2: 🔧 マニュアルDL
# ──────────────────────────────────────────────

def do_manual_download(gid_str, fmt, upload_drive, skip, workers, progress=gr.Progress()):
    if not gid_str.strip():
        return "❌ IDを入力してください", None
    m = re.search(r"\d+", gid_str)
    if not m:
        return "❌ 無効なID", None
    gid = int(m.group())
    cfg = load_config()
    session = get_session(cfg["user_agent"])
    conn = get_conn()

    if skip and is_downloaded(gid):
        row = conn.execute("SELECT title, file_path FROM galleries WHERE id=?", (gid,)).fetchone()
        return f"⏭ スキップ: 「{row['title']}」はDL済み\n{row['file_path']}", None

    progress(0.05, desc="📖 メタデータ取得中...")
    meta = scrape_gallery_metadata(session, conn, gid)
    if not meta:
        return f"❌ ギャラリー {gid} が見つかりません", None

    info = f"📖 {meta['title']}\n🎨 {meta['artist'] or '不明'}  📄 {meta['pages']}p\n🏷️ {(meta['tags'] or '')[:80]}\n"

    def cb(done, total):
        progress(0.1 + 0.85 * done / total, desc=f"⬇️ [{done}/{total}] {workers}並列...")

    progress(0.1, desc="⬇️ ダウンロード中...")
    try:
        fp = download_gallery_zip(session, gid, cfg["download_dir"], workers, cb) if fmt == "ZIP" \
            else download_gallery_pdf(session, gid, cfg["download_dir"], workers, cb)
    except Exception as e:
        return f"{info}❌ 失敗: {e}", None

    conn.execute("UPDATE galleries SET downloaded=1, file_path=? WHERE id=?", (fp, gid))
    conn.commit()
    msg = f"{info}✅ 保存: {fp}"

    if upload_drive:
        progress(0.97, desc="☁️ Drive...")
        gdrive_cfg = cfg["gdrive"]
        link = upload_to_gdrive(fp, gdrive_cfg.get("folder_id",""),
                                gdrive_cfg.get("credentials_file","credentials.json"))
        msg += f"\n☁️ {link}" if link else "\n⚠️ Drive未設定"

    progress(1.0, desc="✅ 完了!")
    return msg, fp


def manual_dl_tab():
    with gr.Tab("🔧 マニュアルDL"):
        gr.Markdown("## ギャラリーIDを直接指定してダウンロード")
        with gr.Row():
            gid_input = gr.Textbox(label="ギャラリーID", placeholder="例: 1607100", scale=3)
            fmt_radio = gr.Radio(["ZIP", "PDF"], label="形式", value="ZIP", scale=2)
        with gr.Row():
            workers_sl = gr.Slider(1, 16, value=8, step=1, label="⚡ 並列数", scale=3)
            skip_chk = gr.Checkbox(label="⏭ DL済みスキップ", value=True, scale=2)
            drive_chk = gr.Checkbox(label="☁️ Google Drive", scale=2)
        dl_btn = gr.Button("⬇️ ダウンロード開始", variant="primary", size="lg")
        dl_result = gr.Textbox(label="結果", lines=8, interactive=False)
        dl_file = gr.File(label="ファイル")
        dl_btn.click(do_manual_download,
                     [gid_input, fmt_radio, drive_chk, skip_chk, workers_sl],
                     [dl_result, dl_file])


# ──────────────────────────────────────────────
# Tab 3: 🗄️ DB管理
# ──────────────────────────────────────────────

def do_db_update(cats, workers_val, progress=gr.Progress()):
    if not cats:
        return "⚠️ カテゴリを選択してください"
    cat_map = {"🎨 Artists":"artists","👥 Groups":"groups","🏷️ Tags":"tags",
               "📺 Parodies":"parodies","👤 Characters":"characters"}
    selected = [cat_map[c] for c in cats if c in cat_map]
    session = get_session(load_config()["user_agent"])
    conn = get_conn()
    workers = int(workers_val)
    log_lines = []

    log_lines.append(f"📡 最終ページ確認中... (並列: {workers})")
    last_pages = {}
    for cat in selected:
        lp = get_category_last_page(session, cat)
        last_pages[cat] = lp
        log_lines.append(f"  {cat}: {lp}p")

    total_pages = sum(last_pages.values())
    global_done = [0]
    import time; start_all = time.time()

    for cat in selected:
        lp = last_pages[cat]
        log_lines.append(f"\n🔄 {cat} 開始（{lp}p × {workers}並列）")

        def cb(done, total, c=cat, mp=lp):
            global_done[0] += 1
            pct = global_done[0] / total_pages
            elapsed = time.time() - start_all
            eta = (elapsed / global_done[0]) * (total_pages - global_done[0]) if global_done[0] else 0
            progress(min(pct, 0.99), desc=f"🔄 {c} [{done}/{total}]  ETA:{eta:.0f}s")

        n = scrape_category_all_parallel(session, conn, cat, workers=workers, progress_cb=cb)
        log_lines.append(f"✅ {cat}: {n:,} 件  ({time.time()-start_all:.0f}s)")

    progress(1.0, desc="✅ 完了!")
    log_lines.append(f"\n⏱ 合計: {time.time()-start_all:.0f}s")
    log_lines.append("\n" + fmt_stats())
    return "\n".join(log_lines)


def do_db_export():
    conn = get_conn()
    tmp = "/tmp/imhentai_db_export.json"
    try:
        export_db_json(conn, tmp)
        size = Path(tmp).stat().st_size / 1024
        return f"✅ {size:.1f} KB", tmp
    except Exception as e:
        return f"❌ {e}", None


def db_tab():
    with gr.Tab("🗄️ DB管理"):
        gr.Markdown("## データベース管理")
        with gr.Row():
            with gr.Column(scale=3):
                gr.Markdown("### 📥 スクレイピング（全ページ自動）")
                cat_cb = gr.CheckboxGroup(
                    ["🎨 Artists","👥 Groups","🏷️ Tags","📺 Parodies","👤 Characters"],
                    label="カテゴリ", value=["🎨 Artists","👥 Groups"])
                sc_workers = gr.Slider(1, 16, value=8, step=1, label="⚡ 並列数")
                gr.Markdown("_Artists≈1057p / Groups≈638p / Tags≈338p / Parodies≈120p / Chars≈516p — 8並列でArtists全件≈5分_")
                update_btn = gr.Button("🔄 スクレイピング開始（全ページ）", variant="primary")
            with gr.Column(scale=2):
                gr.Markdown("### 📤 エクスポート & 統計")
                export_btn = gr.Button("📤 JSONをダウンロード", variant="secondary")
                export_file = gr.File(label="ダウンロード")
                stats_btn = gr.Button("📊 統計を更新", variant="secondary")
        db_log = gr.Textbox(label="ログ / 統計", lines=16, interactive=False, value=fmt_stats)
        update_btn.click(do_db_update, [cat_cb, sc_workers], [db_log])
        export_btn.click(do_db_export, [], [db_log, export_file])
        stats_btn.click(lambda: fmt_stats(), outputs=[db_log])


# ──────────────────────────────────────────────
# Tab 4: 📋 ダウンロード履歴
# ──────────────────────────────────────────────

def load_history(filter_dl="全て"):
    conn = get_conn()
    where = {"✅ DL済み":"WHERE downloaded=1","⬜ 未DL":"WHERE downloaded=0"}.get(filter_dl,"")
    rows = conn.execute(
        f"SELECT id,title,artist,pages,downloaded,file_path,created_at FROM galleries {where} ORDER BY created_at DESC LIMIT 200"
    ).fetchall()
    if not rows:
        return [["—","（履歴なし）","—",0,"—","—","—"]]
    return [[r["id"],r["title"],r["artist"] or "—",r["pages"],
             "✅" if r["downloaded"] else "⬜",r["file_path"] or "—",(r["created_at"] or "")[:16]]
            for r in rows]


def history_tab():
    with gr.Tab("📋 ダウンロード履歴"):
        gr.Markdown("## ダウンロード履歴")
        with gr.Row():
            filter_r = gr.Radio(["全て","✅ DL済み","⬜ 未DL"], label="フィルター", value="全て", scale=3)
            refresh_btn = gr.Button("🔄 更新", scale=1)
        hist_table = gr.Dataframe(
            headers=["ID","タイトル","アーティスト","ページ","状態","ファイルパス","登録日時"],
            datatype=["number","str","str","number","str","str","str"],
            value=load_history, interactive=False, wrap=True, row_count=20)
        refresh_btn.click(load_history, [filter_r], [hist_table])
        filter_r.change(load_history, [filter_r], [hist_table])


# ──────────────────────────────────────────────
# アプリ構築
# ──────────────────────────────────────────────

def build_app():
    with gr.Blocks(title="imhentai-manager") as app:
        gr.HTML("""
        <div class="app-header">
          <h1>📚 imhentai-manager</h1>
          <p>imhentai.xxx の検索・ダウンロード管理ツール</p>
        </div>
        """)
        search_tab()
        manual_dl_tab()
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
            primary_hue="pink",
            secondary_hue="slate",
            neutral_hue="slate",
        ),
        css=DARK_CSS,
    )
