# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 09 | AI/BI Dashboard 作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜MV 作成 + Genie Code でダッシュボード自動生成 + H3 地図ウィジェット</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 20 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,デフォルトカタログ・スキーマ
spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. Materialized View（mv_*）4 件を作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 1. Dashboard 専用 MV を作成（差分更新対応）</h3>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,1/4: mv_dashboard_kpi（経営 KPI）
spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW mv_dashboard_kpi AS
SELECT
  CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE) AS month,
  c.office_id,
  COUNT(*) AS n_contracts,
  SUM(c.settled_price) AS total_settled_price,
  AVG(CAST(c.commission AS DOUBLE) / NULLIF(c.settled_price, 0)) AS avg_commission_rate
FROM sl_contracts c
GROUP BY CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE), c.office_id
""")
spark.sql("COMMENT ON TABLE mv_dashboard_kpi IS 'Dashboard 専用 MV：経営 KPI（月次成約数・成約価格・仲介手数料率）。差分更新対応。'")
print("✅ mv_dashboard_kpi")

# COMMAND ----------

# DBTITLE 1,2/4: mv_h3_price_heatmap（H3 r8 セルごとの平均成約価格、地図ヒートマップ用）
spark.sql(f"""
CREATE OR REPLACE MATERIALIZED VIEW mv_h3_price_heatmap AS
SELECT
  h3_longlatash3(p.lng, p.lat, {H3_RES_MESH}) AS h3_r8,
  CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE) AS month,
  p.property_type,
  COUNT(*) AS n_contracts,
  AVG(c.settled_price) AS avg_settled_price,
  -- H3 セル中心座標を保持（地図ウィジェット用）
  st_y(st_geomfromwkb(h3_centeraswkb(h3_longlatash3(p.lng, p.lat, {H3_RES_MESH})))) AS center_lat,
  st_x(st_geomfromwkb(h3_centeraswkb(h3_longlatash3(p.lng, p.lat, {H3_RES_MESH})))) AS center_lng
FROM sl_contracts c
JOIN sl_properties p ON c.property_id = p.property_id
GROUP BY h3_longlatash3(p.lng, p.lat, {H3_RES_MESH}),
         CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE),
         p.property_type
""")
spark.sql("COMMENT ON TABLE mv_h3_price_heatmap IS 'Dashboard 専用 MV：H3 r8 セルごとの平均成約価格（地図ヒートマップ用）。'")
print("✅ mv_h3_price_heatmap")

# COMMAND ----------

# DBTITLE 1,3/4: mv_hazard_discount（ハザード区分別 × 月の値引き率）
spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW mv_hazard_discount AS
SELECT
  CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE) AS month,
  CASE
    WHEN pge.landslide_hazard_grade = '特別警戒区域' THEN 'レッドゾーン'
    WHEN pge.landslide_hazard_grade = '警戒区域'     THEN 'イエローゾーン'
    WHEN pge.flood_depth_class IS NOT NULL          THEN '浸水想定区域'
    ELSE 'ハザードなし'
  END AS hazard_type,
  pge.area_code,
  COUNT(*) AS n_contracts,
  AVG(CAST(c.discount_amount AS DOUBLE) / NULLIF(c.listing_price, 0)) AS avg_discount_rate,
  AVG(c.settled_price) AS avg_settled_price
FROM sl_contracts c
JOIN sl_property_geo_enriched pge ON c.property_id = pge.property_id
GROUP BY CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE),
  CASE
    WHEN pge.landslide_hazard_grade = '特別警戒区域' THEN 'レッドゾーン'
    WHEN pge.landslide_hazard_grade = '警戒区域'     THEN 'イエローゾーン'
    WHEN pge.flood_depth_class IS NOT NULL          THEN '浸水想定区域'
    ELSE 'ハザードなし'
  END,
  pge.area_code
""")
spark.sql("COMMENT ON TABLE mv_hazard_discount IS 'Dashboard 専用 MV：ハザード区分別 × 月の値引き率と成約価格。'")
print("✅ mv_hazard_discount")

# COMMAND ----------

# DBTITLE 1,4/4: mv_sales_funnel（反響→内見→申込→成約のファネル件数・転換率）
spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW mv_sales_funnel AS
SELECT
  CAST(DATE_TRUNC('MONTH', i.inquiry_date) AS DATE) AS month,
  i.office_id,
  i.funnel_stage,
  COUNT(*) AS n_inquiries
FROM sl_inquiries i
GROUP BY CAST(DATE_TRUNC('MONTH', i.inquiry_date) AS DATE), i.office_id, i.funnel_stage
""")
spark.sql("COMMENT ON TABLE mv_sales_funnel IS 'Dashboard 専用 MV：内見ファネル（反響→内見→申込→成約→失注）の月次件数。'")
print("✅ mv_sales_funnel")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. AI/BI Dashboard を作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 2. Dashboard 作成（Genie Code で自動生成）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 2-A.</b> 左サイドバー <b>Dashboards</b> → 右上 <b>「New dashboard」</b><br>
# MAGIC <b>Step 2-B.</b> 名前: <code>不動産仲介 E2E ダッシュボード</code><br>
# MAGIC <b>Step 2-C.</b> Data → MV / Metric View を Source に追加：
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><code>mv_dashboard_kpi</code></li>
# MAGIC   <li><code>mv_h3_price_heatmap</code></li>
# MAGIC   <li><code>mv_hazard_discount</code></li>
# MAGIC   <li><code>mv_sales_funnel</code></li>
# MAGIC   <li><code>metric_sales_summary</code> / <code>metric_property</code> / <code>metric_funnel</code></li>
# MAGIC </ul>
# MAGIC <b>Step 2-D.</b> Canvas → <b>「Generate visualization」</b> ボタンで Genie Code 起動<br>
# MAGIC <b>Step 2-E.</b> 以下のプロンプトを順に投入してウィジェットを自動生成
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. Genie Code プロンプト（ウィジェット作成）

# COMMAND ----------

# MAGIC %md
# MAGIC ### プロンプト 1：営業所別 × 月別の成約数と仲介手数料率をヒートマップで
# MAGIC ```
# MAGIC mv_dashboard_kpi をソースに、Y軸：office_id、X軸：month、色：n_contracts、ラベル：avg_commission_rate を表示するヒートマップを作成
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 2：物件種別の成約構成比をドーナツチャートで
# MAGIC ```
# MAGIC metric_sales_summary をソースに、property_type 別の MEASURE(n_contracts) をドーナツチャートで表示
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 3：築年 × 成約価格の散布図、浸水想定区域該当の有無で色分け
# MAGIC ```
# MAGIC sl_contracts と sl_properties と sl_property_geo_enriched を JOIN して、X軸：built_year、Y軸：settled_price、色：flood_depth_class IS NOT NULL の散布図を作成
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 4：H3 r8 セルごとの平均成約価格を地図ヒートマップで
# MAGIC ```
# MAGIC mv_h3_price_heatmap をソースに、Map 系ウィジェットで center_lat / center_lng を位置、色：avg_settled_price のヒートマップを作成
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 5：駅徒歩分 × 価格弾力性を物件種別別に折れ線で
# MAGIC ```
# MAGIC metric_property をソースに、X軸：walk_band、Y軸：MEASURE(avg_listing_price)、色：property_type の折れ線グラフを作成
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 6：地価公示乖離率の分布をエリア別ヒストグラムで
# MAGIC ```
# MAGIC sl_property_geo_enriched をソースに、area_code 別に price_vs_landprice_ratio のヒストグラム
# MAGIC ```
# MAGIC
# MAGIC ### プロンプト 7：funnel_stage 別の月次ファネルをサンキー図で
# MAGIC ```
# MAGIC mv_sales_funnel をソースに、month 別の funnel_stage 件数をサンキー図で表示。順序は反響→内見→申込→成約
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. Genie 連携を有効化
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 4. Dashboard から Genie への連携</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 4-A.</b> Dashboard 右上 <b>「Settings」</b> → <b>「Genie Space」</b><br>
# MAGIC <b>Step 4-B.</b> Step 1（NB 08）で作成した Genie Space <code>不動産仲介 E2E 分析</code> を選択<br>
# MAGIC <b>Step 4-C.</b> Save。各ウィジェットの右上から <b>「Ask Genie」</b> でフォローアップ質問できるように
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. Databricks One ポータルへの公開
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 5. Databricks One（業務担当者向け）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC Databricks One は <b>UI 設定のみで専用 NB なし</b>。Dashboard を「Publish to Databricks One」ボタンで公開し、<br>
# MAGIC 営業担当者向けのシンプルなポータル（質問入力欄 + 主要ウィジェット）を構成します。<br>
# MAGIC （詳細手順は Databricks 公式 Databricks One ドキュメント参照）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Dashboard + Genie 連携 + Databricks One 作成完了</strong><br>
# MAGIC 次は <code>10_Jobsワークフロー作成手順.py</code> で SDP + 地理空間 NB + AI NB を Jobs DAG として組みます。
# MAGIC </div>
