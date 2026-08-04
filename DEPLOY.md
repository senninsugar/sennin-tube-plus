各クラウドサービスへの詳細なデプロイ手順をまとめた DEPLOY.md 形式のドキュメントです。
# 🚀 TokyoNOW!tube デプロイメントガイド
このドキュメントでは、様々なプラットフォームに **TokyoNOW!tube** をデプロイする具体的な手順を説明します。
## 1. Render (最も推奨)
GitHubと連携するだけで、Dockerfileを自動認識してデプロイできる無料枠のあるPaaSです。
### 手順
 1. Render にサインインします。
 2. **"New +"** ボタンをクリックし、**"Web Service"** を選択します。
 3. GitHubリポジトリ senninsugar/sennin-tube-plus を連携します。
 4. 設定画面で以下を入力します：
   * **Name**: sennin-tube-plus
   * **Region**: Singapore (日本から最速)
   * **Runtime**: Docker
 5. **"Advanced"** を開き、環境変数を追加します：
   * PYTHONUNBUFFERED: 1
 6. **"Create Web Service"** をクリックします。
   * 数分で https://sennin-tube-plus.onrender.com のようなURLで公開されます。
## 2. Railway
非常に高速で、設定がほとんど不要なプラットフォームです。
### 手順
 1. Railway にログインします。
 2. **"New Project"** > **"Deploy from GitHub repo"** を選択。
 3. リポジトリを選択し、**"Deploy Now"** をクリック。
 4. 自動的に Dockerfile が検出されビルドが始まります。
 5. **"Settings"** タブ内の **"Public Networking"** で **"Generate Domain"** をクリックして公開URLを発行します。
## 3. Koyeb
最近人気のある、Docker対応のPaaSです。
### 手順
 1. Koyeb にログインし、**"Create Service"** をクリック。
 2. **"GitHub"** を選択し、リポジトリを指定。
 3. **"Builder"** で Dockerfile を選択。
 4. **"Port"** 設定を 8000 に変更します（重要）。
 5. **"Deploy"** をクリック。
## 4. Google Cloud Run (サーバーレス)
アクセスがある時だけ起動するスケーラブルな構成です。
### 事前準備
 * Google Cloud SDK をインストール。
### 手順
 1. **プロジェクトの設定**:
   ```bash
   gcloud config set project [YOUR_PROJECT_ID]
   
   ```
 2. **ビルドとデプロイ**:
   ```bash
   gcloud run deploy sennin-tube \
     --source . \
     --region asia-northeast1 \
     --allow-unauthenticated \
     --port 8000 \
     --max-instances 1
   
   ```
## 5. 自前サーバー / VPS (Docker Compose)
独自のサーバーやUbuntu VPSなどで永続的に運用する場合です。
### 手順
 1. **リポジトリのクローン**:
   ```bash
   git clone https://github.com/senninsugar/sennin-tube-plus.git
   cd sennin-tube-plus
   
   ```
 2. **docker-compose.yml の作成**:
   ```yaml
   services:
     app:
       build: .
       ports:
         - "8000:8000"
       restart: always
   
   ```
 3. **起動**:
   ```bash
   docker compose up -d
   
   ```
## 🛠 トラブルシューティング
### 1. アプリが起動しない（Portエラー）
多くのPaaSはデフォルトでポート 80 や 10000 を期待します。このアプリは 8000 で動作するため、サービス側の設定（Environmental Variables や Port 設定）で PORT: 8000 を指定するか、管理画面からポート番号を 8000 に変更してください。
### 2. 動画が再生されない
Invidiousのインスタンスがダウンしているか、YouTube側にブロックされている可能性があります。main.py の INVIDIOUS_INSTANCES を最新の稼働しているインスタンスに更新してください。
### 3. メモリ不足
無料プラン（512MBなど）で動作しますが、アクセスが集中するとメモリ不足になる場合があります。その場合は limits 設定を調整してください。
**リポジトリURL:** GitHub - sennin-tube-plus
