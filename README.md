# imhentai-manager

imhentai.com の検索・ダウンロード管理ツール（Python製CLIツール）

## 機能

- **データベース**: Tags / Parodies / Artists / Characters / Groups を SQLite に登録・JSON エクスポート
- **検索**: アーティスト名・作品名で検索。見つからない場合は fuzzy match で近似候補を提示
- **ダウンロード**: ZIP または PDF 形式で保存
- **Google Drive 連携**: 指定ファイルをアップロード（旧ファイル削除後に新規アップ）

## インストール

```bash
pip install -r requirements.txt
pip install -e .
```

## 使い方

### 設定ファイルの作成

```bash
cp config.yaml.example config.yaml
# config.yaml を編集して download_dir などを設定
```

### データベース更新

```bash
# 全カテゴリを5ページ分スクレイピング
imhentai db update --pages 5

# アーティストとタグのみ
imhentai db update --artists --tags --pages 10

# DB を JSON にエクスポート
imhentai db export --output ./data/db.json
```

### 検索

```bash
# 全体検索
imhentai search "Kariya"

# アーティスト名で検索
imhentai search --artist "Kariya"

# 作品タイトルで検索
imhentai search --title "Tekoki Maniax"
```

見つからない場合は fuzzy match で近似候補を最大5件表示します。

### ダウンロード

```bash
# ZIP でダウンロード
imhentai download 123456 --format zip

# PDF でダウンロード
imhentai download 123456 --format pdf

# Google Drive にもアップロード
imhentai download 123456 --format zip --upload-drive
```

### 一括ダウンロード

```bash
# gallery_ids.txt にギャラリーIDを1行ずつ記載
imhentai batch --file gallery_ids.txt --format zip
```

## 今後実装予定 (Phase 2)

- [ ] ダウンロード済み管理（重複スキップ）
- [ ] バッチ処理の進捗バー（tqdm）
- [ ] プロキシ対応
- [ ] GUI（tkinter or Web UI）
- [ ] CBZ 形式対応
- [ ] メタデータ埋め込み（作者・タグ等を PDF/ZIP に記載）
- [ ] 差分更新（新着チェック）
- [ ] Google Drive 本番連携（credentials.json 設定）
- [ ] Discord 通知（ダウンロード完了時）
- [ ] 検索結果のページネーション
- [ ] お気に入りリスト管理

## ディレクトリ構成

```
imhentai-manager/
├── imhentai/
│   ├── __init__.py   # コア機能（DB・スクレイパー・検索・ダウンロード）
│   └── cli.py        # CLI エントリポイント
├── data/             # DB・JSON の保存先
├── downloads/        # ダウンロードファイルの保存先
├── config.yaml       # 設定ファイル（要作成）
├── config.yaml.example
├── requirements.txt
└── setup.py
```

## 注意事項

- サイトへの過度なアクセスはしないでください（レート制限あり）
- 個人利用の範囲でご使用ください
