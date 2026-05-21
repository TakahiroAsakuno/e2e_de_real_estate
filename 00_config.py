# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 00 | 環境設定
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 5 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC 全ノートブック共通の <b>変数・スキーマ・Volume</b> をセットアップします。<br>
# MAGIC 他のノートブックの先頭で <code>%run ./00_config</code> を実行して使います。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,変数設定
MY_CATALOG  = "asakuno_demo_v4"             # カタログは事前作成済み前提
MY_SCHEMA   = "real_estate_e2e_demo"      # このデモ専用スキーマ
MY_VOLUME   = "raw_data"                  # CSV / Shapefile / PDF / MP3 を配置する Volume

VOLUME_PATH = f"/Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}"

# AI Functions で使うモデル
LLM_MODEL = "databricks-claude-opus-4-7"

# Whisper 文字起こし用 Model Serving エンドポイント名
# UC 上の Whisper モデルを Model Serving にデプロイした際のエンドポイント名を指定
WHISPER_ENDPOINT = "asakuno_whisper_large_v3"

# COMMAND ----------

# DBTITLE 1,対象エリア（首都圏 + 大阪 + 福岡 + 愛知 の 7 都府県）
# 全国フィルタリング用。KSJ Shapefile / reinfolib API のクエリ条件で使う
TARGET_PREFECTURES = {
    "11": "埼玉県",
    "12": "千葉県",
    "13": "東京都",
    "14": "神奈川県",
    "23": "愛知県",
    "27": "大阪府",
    "40": "福岡県",
}
TARGET_PREF_CODES = list(TARGET_PREFECTURES.keys())

# COMMAND ----------

# DBTITLE 1,H3 解像度設計（README「Geo JOIN の標準フロー」参照）
H3_RES_ADMIN = 6   # 行政区域（広域集計用）。H3 v4.x 公式：平均辺長 約 3.7km
H3_RES_MESH  = 8   # メッシュ人口 / 地図ヒートマップ。H3 v4.x 公式：平均辺長 約 531m
H3_RES_GEO   = 9   # 用途地域 / ハザード / 地価 / 物件 JOIN の主解像度。H3 v4.x 公式：平均辺長 約 201m

# COMMAND ----------

# DBTITLE 1,UC 権限グループ（多段位置情報マスキング、README セクション 10 参照）
# is_account_group_member(...) で判定するグループ名
# 事前に Account Console でグループ作成し、ユーザーを所属させておく必要あり
GROUP_ADMIN   = "real_estate_admin"     # 全列 exact 参照可
GROUP_ANALYST = "real_estate_analyst"   # 緯度経度は H3 r8 セル中心まで
GROUP_VIEWER  = "real_estate_viewer"    # 緯度経度は市区町村ポリゴン代表点まで

# COMMAND ----------

# DBTITLE 1,reinfolib API（不動産情報ライブラリ）認証情報
# API キーは Databricks Secret に登録する想定。スコープ名はデモプロジェクトに揃えて衝突を避ける:
#   databricks secrets create-scope --scope real_estate_e2e_demo
#   databricks secrets put-secret --scope real_estate_e2e_demo --key reinfolib_api_key --string-value "<your-key>"
# 01_データ準備.py で dbutils.secrets.get(scope=..., key=...) を使って取得
REINFOLIB_SECRET_SCOPE = "real_estate_e2e_demo"
REINFOLIB_SECRET_KEY   = "reinfolib_api_key"

# COMMAND ----------

# DBTITLE 1,設定値の確認
print(f"MY_CATALOG             : {MY_CATALOG}")
print(f"MY_SCHEMA              : {MY_SCHEMA}")
print(f"MY_VOLUME              : {MY_VOLUME}")
print(f"VOLUME_PATH            : {VOLUME_PATH}")
print(f"LLM_MODEL              : {LLM_MODEL}")
print(f"WHISPER_ENDPOINT       : {WHISPER_ENDPOINT}")
print(f"TARGET_PREFECTURES     : {TARGET_PREFECTURES}")
print(f"H3_RES (ADMIN/MESH/GEO): {H3_RES_ADMIN} / {H3_RES_MESH} / {H3_RES_GEO}")
print(f"GROUP (ADMIN/ANALYST/VIEWER): {GROUP_ADMIN} / {GROUP_ANALYST} / {GROUP_VIEWER}")
print(f"REINFOLIB_SECRET       : scope={REINFOLIB_SECRET_SCOPE}, key={REINFOLIB_SECRET_KEY}")

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {MY_CATALOG}.{MY_SCHEMA} CASCADE")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🏗️ Unity Catalog セットアップ
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🔗 Unity Catalog の3階層構造</strong><br>
# MAGIC <code>カタログ</code> &gt; <code>スキーマ</code> &gt; <code>テーブル / ビュー / Volume / 関数 / Metrics View / VS Index</code><br>
# MAGIC カタログは管理者が事前作成済みの前提です。ここではスキーマと Volume を作成します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,スキーマ作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}")
print(f"✅ スキーマ '{MY_CATALOG}.{MY_SCHEMA}' を確認/作成しました")

# COMMAND ----------

# DBTITLE 1,Volume作成
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")
print(f"✅ Volume '{MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}' を確認/作成しました")

# COMMAND ----------

# DBTITLE 1,デフォルトカタログ・スキーマ設定
# USE CATALOG / USE SCHEMA を実行しておくと、以降のSQLでカタログ・スキーマ名を省略できる
spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")
print(f"実行済み: USE CATALOG {MY_CATALOG}")
print(f"実行済み: USE SCHEMA {MY_SCHEMA}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ セットアップ完了</strong><br>
# MAGIC 他のノートブックの先頭で <code>%run ./00_config</code> を実行すると、<br>
# MAGIC <code>MY_CATALOG</code> / <code>MY_SCHEMA</code> / <code>MY_VOLUME</code> / <code>VOLUME_PATH</code> / <code>LLM_MODEL</code> / <code>WHISPER_ENDPOINT</code> /<br>
# MAGIC <code>TARGET_PREFECTURES</code> / <code>TARGET_PREF_CODES</code> / <code>H3_RES_*</code> / <code>GROUP_*</code> / <code>REINFOLIB_SECRET_*</code> が使えます。<br>
# MAGIC <code>USE CATALOG</code> / <code>USE SCHEMA</code> 設定済みのため、SQL でカタログ・スキーマ名の省略も可能です。
# MAGIC </div>
