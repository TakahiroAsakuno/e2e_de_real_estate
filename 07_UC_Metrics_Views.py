# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 07 | UC Metric Views（統一 KPI 定義）
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜成約 / 物件 / 顧客 / ファネル の KPI を YAML で一元定義</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 10 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC 不動産仲介の 4 つの Metric View（<code>metric_sales_summary</code> / <code>metric_property</code> / <code>metric_customer</code> / <code>metric_funnel</code>）を<br>
# MAGIC <b>Catalog Explorer + Genie Code（日本語プロンプト）</b>で作成し、Genie / AI/BI Dashboard / SQL から<b>同じ定義</b>で参照可能にします。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📐 Metric View の構成要素</strong><br>
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><b>source</b>：集計対象のテーブル（Silver / Gold）</li>
# MAGIC   <li><b>dimensions</b>：GROUP BY の候補（営業所・月・物件種別・ハザード区分 等）</li>
# MAGIC   <li><b>measures</b>：集計ロジック（成約数・成約価格・仲介手数料率 等）。クエリ時に <code>MEASURE()</code> で呼出</li>
# MAGIC </ul>
# MAGIC <strong>前提</strong>：Databricks Runtime 17.2+ または サーバーレス SQL（YAML version 1.1 のため）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 作成手順（UI 操作）
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Catalog Explorer + Genie Code で作成</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step A.</b> 左サイドバー <b>Catalog</b> → <code>komae_demo_v4</code> → <code>real_estate_e2e_demo</code> を選択<br>
# MAGIC <b>Step B.</b> 上部の <b>「Create」</b> → <b>「Metric View」</b> を選択<br>
# MAGIC <b>Step C.</b> <b>「Generate with Genie」</b> をクリックし、日本語プロンプトを入力<br>
# MAGIC <b>Step D.</b> 生成された YAML を確認・微修正して「Create」<br>
# MAGIC <b>Step E.</b> 4 つの Metric View 分、同様に作成
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1/4: metric_sales_summary（成約 KPI）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
# MAGIC <b>Genie Code プロンプト例</b>：<br>
# MAGIC 「sl_contracts と sl_properties を JOIN した Metric View を作成。ディメンションは営業所 ID、契約月、物件種別、エリアコード、ハザード区分。メジャーは成約数、合計成約価格、平均成約価格、合計値引額、平均値引率、合計仲介手数料、平均仲介手数料率。」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```yaml
# MAGIC version: 1.1
# MAGIC source: |
# MAGIC   SELECT
# MAGIC     c.contract_id,
# MAGIC     c.office_id,
# MAGIC     c.contract_date,
# MAGIC     CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE) AS contract_month,
# MAGIC     c.settled_price,
# MAGIC     c.listing_price,
# MAGIC     c.discount_amount,
# MAGIC     c.commission,
# MAGIC     p.property_type,
# MAGIC     p.area_code,
# MAGIC     CASE
# MAGIC       WHEN pge.landslide_hazard_grade = '特別警戒区域' THEN 'レッドゾーン'
# MAGIC       WHEN pge.landslide_hazard_grade = '警戒区域'     THEN 'イエローゾーン'
# MAGIC       WHEN pge.flood_depth_class IS NOT NULL          THEN '浸水想定区域'
# MAGIC       ELSE 'ハザードなし'
# MAGIC     END AS hazard_type
# MAGIC   FROM sl_contracts c
# MAGIC   JOIN sl_properties p ON c.property_id = p.property_id
# MAGIC   LEFT JOIN sl_property_geo_enriched pge ON c.property_id = pge.property_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: office_id
# MAGIC     expr: office_id
# MAGIC   - name: contract_month
# MAGIC     expr: contract_month
# MAGIC   - name: property_type
# MAGIC     expr: property_type
# MAGIC   - name: area_code
# MAGIC     expr: area_code
# MAGIC   - name: hazard_type
# MAGIC     expr: hazard_type
# MAGIC
# MAGIC measures:
# MAGIC   - name: n_contracts
# MAGIC     expr: COUNT(contract_id)
# MAGIC   - name: total_settled_price
# MAGIC     expr: SUM(settled_price)
# MAGIC   - name: avg_settled_price
# MAGIC     expr: AVG(settled_price)
# MAGIC   - name: total_discount_amount
# MAGIC     expr: SUM(discount_amount)
# MAGIC   - name: avg_discount_rate
# MAGIC     expr: AVG(CAST(discount_amount AS DOUBLE) / NULLIF(listing_price, 0))
# MAGIC   - name: total_commission
# MAGIC     expr: SUM(commission)
# MAGIC   - name: avg_commission_rate
# MAGIC     expr: AVG(CAST(commission AS DOUBLE) / NULLIF(settled_price, 0))
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/4: metric_property（物件 KPI）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
# MAGIC <b>Genie Code プロンプト例</b>：<br>
# MAGIC 「sl_properties と sl_property_geo_enriched を JOIN した Metric View。ディメンションは物件種別、用途地域、駅徒歩区分（5分以内/10分以内/15分超）、ステータス。メジャーは在庫件数、平均売出価格、平均査定価格、平均売出日数。」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```yaml
# MAGIC version: 1.1
# MAGIC source: |
# MAGIC   SELECT
# MAGIC     p.property_id,
# MAGIC     p.property_type,
# MAGIC     p.status,
# MAGIC     p.listing_price,
# MAGIC     p.assessment_price,
# MAGIC     p.listed_at,
# MAGIC     p.walk_minutes,
# MAGIC     DATEDIFF(CURRENT_DATE(), p.listed_at) AS days_on_market,
# MAGIC     CASE
# MAGIC       WHEN p.walk_minutes <= 5  THEN '5分以内'
# MAGIC       WHEN p.walk_minutes <= 10 THEN '10分以内'
# MAGIC       WHEN p.walk_minutes <= 15 THEN '15分以内'
# MAGIC       ELSE '15分超'
# MAGIC     END AS walk_band,
# MAGIC     pge.zoning_name,
# MAGIC     pge.flood_depth_class,
# MAGIC     pge.landslide_hazard_grade
# MAGIC   FROM sl_properties p
# MAGIC   LEFT JOIN sl_property_geo_enriched pge ON p.property_id = pge.property_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: property_type
# MAGIC     expr: property_type
# MAGIC   - name: status
# MAGIC     expr: status
# MAGIC   - name: zoning_name
# MAGIC     expr: zoning_name
# MAGIC   - name: walk_band
# MAGIC     expr: walk_band
# MAGIC   - name: flood_depth_class
# MAGIC     expr: flood_depth_class
# MAGIC
# MAGIC measures:
# MAGIC   - name: n_properties
# MAGIC     expr: COUNT(property_id)
# MAGIC   - name: avg_listing_price
# MAGIC     expr: AVG(listing_price)
# MAGIC   - name: avg_assessment_price
# MAGIC     expr: AVG(assessment_price)
# MAGIC   - name: avg_days_on_market
# MAGIC     expr: AVG(days_on_market)
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3/4: metric_customer（顧客 KPI）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
# MAGIC <b>Genie Code プロンプト例</b>：<br>
# MAGIC 「sl_customers と gd_customer_rm_segment を JOIN した Metric View。ディメンションは RM セグメント、ライフステージ、年代、性別。メジャーは顧客数、平均成約価格、平均取引件数。」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```yaml
# MAGIC version: 1.1
# MAGIC source: |
# MAGIC   SELECT
# MAGIC     c.customer_id,
# MAGIC     c.life_stage,
# MAGIC     c.gender,
# MAGIC     CASE
# MAGIC       WHEN c.age < 30 THEN '20代'
# MAGIC       WHEN c.age < 40 THEN '30代'
# MAGIC       WHEN c.age < 50 THEN '40代'
# MAGIC       WHEN c.age < 60 THEN '50代'
# MAGIC       ELSE '60代以上'
# MAGIC     END AS age_band,
# MAGIC     seg.rm_segment,
# MAGIC     seg.n_contracts,
# MAGIC     seg.total_settled_price
# MAGIC   FROM sl_customers c
# MAGIC   LEFT JOIN gd_customer_rm_segment seg ON c.customer_id = seg.customer_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: rm_segment
# MAGIC     expr: rm_segment
# MAGIC   - name: life_stage
# MAGIC     expr: life_stage
# MAGIC   - name: age_band
# MAGIC     expr: age_band
# MAGIC   - name: gender
# MAGIC     expr: gender
# MAGIC
# MAGIC measures:
# MAGIC   - name: n_customers
# MAGIC     expr: COUNT(customer_id)
# MAGIC   - name: avg_total_settled_price
# MAGIC     expr: AVG(total_settled_price)
# MAGIC   - name: avg_n_contracts
# MAGIC     expr: AVG(n_contracts)
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 4/4: metric_funnel（内見ファネル KPI）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
# MAGIC <b>Genie Code プロンプト例</b>：<br>
# MAGIC 「sl_inquiries の funnel_stage（反響/内見/申込/成約/失注）を集計する Metric View。ディメンションは営業所、契約月、来店区分、物件種別。メジャーは各ファネルステージ件数と転換率（反響→内見、内見→申込、申込→成約）。」
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```yaml
# MAGIC version: 1.1
# MAGIC source: |
# MAGIC   SELECT
# MAGIC     i.inquiry_id,
# MAGIC     i.office_id,
# MAGIC     CAST(DATE_TRUNC('MONTH', i.inquiry_date) AS DATE) AS inquiry_month,
# MAGIC     i.visit_kind,
# MAGIC     i.funnel_stage,
# MAGIC     p.property_type
# MAGIC   FROM sl_inquiries i
# MAGIC   LEFT JOIN sl_properties p ON i.property_id = p.property_id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: office_id
# MAGIC     expr: office_id
# MAGIC   - name: inquiry_month
# MAGIC     expr: inquiry_month
# MAGIC   - name: visit_kind
# MAGIC     expr: visit_kind
# MAGIC   - name: property_type
# MAGIC     expr: property_type
# MAGIC
# MAGIC measures:
# MAGIC   - name: n_total
# MAGIC     expr: COUNT(inquiry_id)
# MAGIC   - name: n_kakyou
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '反響' THEN inquiry_id END)
# MAGIC   - name: n_naiken
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '内見' THEN inquiry_id END)
# MAGIC   - name: n_moushikomi
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '申込' THEN inquiry_id END)
# MAGIC   - name: n_seiyaku
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '成約' THEN inquiry_id END)
# MAGIC   - name: n_shitsuchu
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '失注' THEN inquiry_id END)
# MAGIC   - name: rate_kakyou_to_naiken
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '内見' THEN inquiry_id END) * 1.0 / NULLIF(COUNT(CASE WHEN funnel_stage IN ('反響','内見','申込','成約','失注') THEN inquiry_id END), 0)
# MAGIC   - name: rate_naiken_to_moushikomi
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '申込' THEN inquiry_id END) * 1.0 / NULLIF(COUNT(CASE WHEN funnel_stage = '内見' THEN inquiry_id END), 0)
# MAGIC   - name: rate_moushikomi_to_seiyaku
# MAGIC     expr: COUNT(CASE WHEN funnel_stage = '成約' THEN inquiry_id END) * 1.0 / NULLIF(COUNT(CASE WHEN funnel_stage = '申込' THEN inquiry_id END), 0)
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## クエリ例（SQL から MEASURE() で呼出）

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- 営業所別の月次成約数・成約価格・値引き率
# MAGIC SELECT
# MAGIC   office_id,
# MAGIC   contract_month,
# MAGIC   MEASURE(n_contracts)         AS n_contracts,
# MAGIC   MEASURE(total_settled_price) AS total_settled_price,
# MAGIC   MEASURE(avg_discount_rate)   AS avg_discount_rate
# MAGIC FROM komae_demo_v4.real_estate_e2e_demo.metric_sales_summary
# MAGIC GROUP BY office_id, contract_month
# MAGIC ORDER BY office_id, contract_month;
# MAGIC
# MAGIC -- ハザード区分別の値引き率
# MAGIC SELECT
# MAGIC   hazard_type,
# MAGIC   MEASURE(n_contracts)       AS n_contracts,
# MAGIC   MEASURE(avg_discount_rate) AS avg_discount_rate
# MAGIC FROM komae_demo_v4.real_estate_e2e_demo.metric_sales_summary
# MAGIC GROUP BY hazard_type
# MAGIC ORDER BY avg_discount_rate DESC;
# MAGIC
# MAGIC -- 営業所×月次のファネル転換率
# MAGIC SELECT
# MAGIC   office_id,
# MAGIC   inquiry_month,
# MAGIC   MEASURE(rate_kakyou_to_naiken)      AS rate_kakyou_to_naiken,
# MAGIC   MEASURE(rate_naiken_to_moushikomi)  AS rate_naiken_to_moushikomi,
# MAGIC   MEASURE(rate_moushikomi_to_seiyaku) AS rate_moushikomi_to_seiyaku
# MAGIC FROM komae_demo_v4.real_estate_e2e_demo.metric_funnel
# MAGIC GROUP BY office_id, inquiry_month;
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Metric Views 作成完了</strong><br>
# MAGIC 4 つの Metric View が Catalog Explorer に登録され、Genie / Dashboard / SQL から同じ定義でクエリ可能です。<br>
# MAGIC 次は <code>08_Genie作成手順.py</code> で Genie Space を作成し、これらの Metric View を Source として接続します。
# MAGIC </div>
