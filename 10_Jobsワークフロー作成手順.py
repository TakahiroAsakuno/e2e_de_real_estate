# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 10 | Jobs ワークフロー作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜SDP + 地理空間 NB + AI NB + Metric / MV 更新 を Jobs DAG として組む</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 10 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC データ準備 → SDP パイプライン → 地理空間パイプライン → AI+Geo 補完処理 → MV 更新 を Jobs DAG として組み、定期実行可能にします。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## DAG 構成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">タスク依存関係（5 ノード）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC [Task1: 01_データ準備（増分 CSV / Shapefile / OSM 取得）]
# MAGIC          │
# MAGIC          ├──► [Task2: SDP パイプライン実行（E2E_real_estate_pipeline）]
# MAGIC          │              │
# MAGIC          │              ▼
# MAGIC          └──► [Task3: 03_地理空間データパイプライン.py]
# MAGIC                          │
# MAGIC                          ▼
# MAGIC               [Task4: 05_AIとGeoによる補完処理.py]
# MAGIC                          │
# MAGIC                          ▼
# MAGIC               [Task5: 09_ダッシュボード作成手順 の MV 更新セル（REFRESH MATERIALIZED VIEW）]
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. Job を作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 1. Jobs & Pipelines → Create → Job</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 1-A.</b> 左サイドバー <b>Jobs & Pipelines</b> → 右上の <b>「Create」</b> → <b>「Job」</b><br>
# MAGIC <b>Step 1-B.</b> Job 名: <code>不動産 E2E ETL</code>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. タスクを 5 つ追加
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 2. タスク 5 件作成</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC | # | Task 名 | Task type | Source | Depends on |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | `task_01_data_prep` | Notebook | `01_データ準備` | なし |
# MAGIC | 2 | `task_02_sdp_pipeline` | Pipeline | `E2E_real_estate_pipeline`（NB 04 で作成） | `task_01_data_prep` |
# MAGIC | 3 | `task_03_geo_pipeline` | Notebook | `03_地理空間データパイプライン` | `task_01_data_prep` |
# MAGIC | 4 | `task_05_ai_geo_enrich` | Notebook | `05_AIとGeoによる補完処理` | `task_02_sdp_pipeline`, `task_03_geo_pipeline` |
# MAGIC | 5 | `task_mv_refresh` | SQL（Run SQL file） | 下記の REFRESH 文（インライン） | `task_05_ai_geo_enrich` |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### task_mv_refresh の SQL（インラインまたは別 SQL ファイル）

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC USE CATALOG komae_demo_v4;
# MAGIC USE SCHEMA real_estate_e2e_demo;
# MAGIC
# MAGIC REFRESH MATERIALIZED VIEW mv_dashboard_kpi;
# MAGIC REFRESH MATERIALIZED VIEW mv_h3_price_heatmap;
# MAGIC REFRESH MATERIALIZED VIEW mv_hazard_discount;
# MAGIC REFRESH MATERIALIZED VIEW mv_sales_funnel;
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. スケジュール設定
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 3. 定期実行スケジュール</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC Job 詳細画面 → 右上 <b>「Add schedule」</b><br>
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><b>Trigger</b>: Scheduled</li>
# MAGIC   <li><b>Cron</b>: <code>0 0 6 * * ?</code>（毎朝 6:00 JST）</li>
# MAGIC   <li><b>Timezone</b>: Asia/Tokyo</li>
# MAGIC </ul>
# MAGIC ※ デモでは手動実行で OK。スケジュールはスキップ可。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. 通知設定
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 4. 失敗時の通知（Slack / Email）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC Job 詳細画面 → 左サイド <b>「Notifications」</b><br>
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><b>On failure</b>: Email 通知先を追加</li>
# MAGIC   <li><b>On success</b>: 任意（成功通知も欲しい場合のみ）</li>
# MAGIC   <li>Slack 連携は Workspace の Webhook 設定経由</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. 動作確認
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 5. Run now で初回実行</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC Job 詳細画面 → 右上 <b>「Run now」</b><br>
# MAGIC DAG ビューで Task 1 → 2/3 → 4 → 5 の順に実行されるのを確認。<br>
# MAGIC 各 Task のログ・Spark UI も DAG 上からドリルダウン可能。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Jobs ワークフロー作成完了</strong><br>
# MAGIC 次は <code>11_Genie Codeインタラクティブ分析.py</code> で経営判断シナリオ 3 本（ハザード起因値引き分析 / 駅徒歩×価格弾力性 / 営業所配置最適化）を Genie で深掘り体験します。
# MAGIC </div>
