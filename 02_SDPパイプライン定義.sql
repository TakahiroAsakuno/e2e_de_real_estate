-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # 02 | SDP パイプライン定義
-- MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
-- MAGIC   <div style="display: flex; align-items: center;">
-- MAGIC     <div>
-- MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜Bronze → Silver → Gold（Lakeflow SDP / SQL）</p>
-- MAGIC     </div>
-- MAGIC     <div style="margin-left: auto;">
-- MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 25 min</span>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>⚠️ このノートブックの位置づけ</strong><br>
-- MAGIC これは <b>SDP（Lakeflow Spark Declarative Pipelines）に登録するソース定義ファイル</b>です。<br>
-- MAGIC ノートブックとして直接 Run しても何も起きません。次の <code>04_SDPパイプライン設定手順</code> の UI 操作で<br>
-- MAGIC <b>このファイルをパイプラインのソースとして登録 → 実行</b> します。<br><br>
-- MAGIC <strong>📌 パイプライン設定パラメータ</strong>：本 SQL は <code>${volume_path}</code> を参照しています。NB 04 のパイプライン設定で
-- MAGIC <code>volume_path</code> パラメータに <code>/Volumes/komae_demo_v4/real_estate_e2e_demo/raw_data</code> を設定してください。
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>📐 構成概要</strong><br>
-- MAGIC <b>Bronze（6 streaming tables）</b>：Auto Loader（<code>STREAM read_files()</code>）で Volume の CSV を増分取り込み。監査列 <code>_ingested_at</code> / <code>_source_file</code> を付与。<br>
-- MAGIC <b>Silver（6 streaming tables）</b>：列定義 + 型 + NOT NULL を明示し、<code>CONSTRAINT … EXPECT … ON VIOLATION</code> でデータ品質ルール、<code>PRIMARY KEY</code> / <code>FOREIGN KEY</code> でリレーションを宣言。<br>
-- MAGIC <b>Gold（3 materialized views、NB 02 由来分）</b>：Silver から集計マートを構築。差分更新は SDP が自動判定。<br><br>
-- MAGIC <b>NB 02 由来の Gold 3 件</b>：<code>gd_office_monthly_sales</code> / <code>gd_property_inventory</code> / <code>gd_market_linked_margin</code>。<br>
-- MAGIC <b>NB 05 由来の Gold 3 件</b>（Geo JOIN・AI enrich 必須）：<code>gd_property_hazard_summary</code> / <code>gd_contract_discount_score</code> / <code>gd_customer_rm_segment</code>。
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥉 Bronze 層</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">Volume の CSV を STREAM read_files() で増分取り込み</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/6: bz_offices</strong>（営業所マスタ）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_offices
COMMENT '営業所マスタ Bronze。30 拠点、戸建仲介 / マンション仲介 / 投資物件専門の 3 業態。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'offices.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/6: bz_properties</strong>（物件マスタ）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_properties
COMMENT '物件マスタ Bronze。3,000 件、戸建 / マンションの住宅地。reinfolib 取引価格情報を種に合成。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'properties.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/6: bz_customers</strong>（顧客マスタ）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_customers
COMMENT '顧客マスタ Bronze。2,500 名。年収レンジ / 家族構成 / ライフステージなどのプロファイル列を含む。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'customers.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 4/6: bz_market_index</strong>（不動産市況指標）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_market_index
COMMENT '不動産市況指標 Bronze。月 × エリア × 物件種別 の取引価格指数・地価指数・住宅ローン金利・建築費指数。land_price_index は NB 03 で L01 由来に上書き。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/',
  format          => 'csv',
  header          => 'true',
  pathGlobFilter  => 'market_index.csv'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 5/6: bz_inquiries</strong>（内見・問合せ履歴）｜<b>Autoloader 日次分割対応</b>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_inquiries
COMMENT '内見・問合せ履歴 Bronze。12,500 件、過去 24 ヶ月分。直近 5 日分は日次 CSV、それ以前は history.csv にまとめ。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/inquiries/',
  format          => 'csv',
  header          => 'true'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 6/6: bz_contracts</strong>（成約）｜<b>Autoloader 日次分割対応</b>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bz_contracts
COMMENT '成約 Bronze。5,000 件、過去 24 ヶ月分の取引履歴。直近 5 日分は日次 CSV、それ以前は history.csv。'
AS SELECT
  *,
  current_timestamp()        AS _ingested_at,
  _metadata.file_path        AS _source_file
FROM STREAM read_files(
  '${volume_path}/contracts/',
  format          => 'csv',
  header          => 'true'
);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #455A64; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥈 Silver 層</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">列定義 + 型 + NOT NULL + Expectations + PK/FK</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/6: sl_offices</strong>｜PK <code>office_id</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_offices (
  office_id     STRING NOT NULL,
  office_name   STRING,
  office_type   STRING,
  prefecture    STRING,
  area_code     STRING,
  city          STRING,
  lat           DOUBLE,
  lng           DOUBLE,
  _ingested_at  TIMESTAMP,
  _source_file  STRING,
  CONSTRAINT valid_office_id   EXPECT (office_id IS NOT NULL)                                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_office_type EXPECT (office_type IN ('戸建仲介','マンション仲介','投資物件専門')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_lat         EXPECT (lat BETWEEN 24 AND 46)                                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_lng         EXPECT (lng BETWEEN 122 AND 146)                                     ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_offices     PRIMARY KEY (office_id)
)
COMMENT '営業所マスタ Silver。30 拠点、業態・都道府県・緯度経度の妥当性チェック付き。'
AS SELECT
  CAST(office_id    AS STRING) AS office_id,
  CAST(office_name  AS STRING) AS office_name,
  CAST(office_type  AS STRING) AS office_type,
  CAST(prefecture   AS STRING) AS prefecture,
  CAST(area_code    AS STRING) AS area_code,
  CAST(city         AS STRING) AS city,
  CAST(lat          AS DOUBLE) AS lat,
  CAST(lng          AS DOUBLE) AS lng,
  _ingested_at,
  _source_file
FROM STREAM bz_offices;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/6: sl_properties</strong>｜PK <code>property_id</code>、FK <code>office_id</code> → <code>sl_offices</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_properties (
  property_id       STRING NOT NULL,
  property_type     STRING,
  prefecture        STRING,
  area_code         STRING,
  city              STRING,
  district          STRING,
  address           STRING,
  lat               DOUBLE,
  lng               DOUBLE,
  built_year        INT,
  floor_area_sqm    DOUBLE,
  land_area_sqm     DOUBLE,
  layout            STRING,
  nearest_station   STRING,
  walk_minutes      INT,
  energy_grade      INT,
  insulation_grade  INT,
  seismic_grade     INT,
  listing_price     BIGINT,
  assessment_price  BIGINT,
  listed_at         DATE,
  status            STRING,
  office_id         STRING NOT NULL,
  _ingested_at      TIMESTAMP,
  _source_file      STRING,
  CONSTRAINT valid_property_id     EXPECT (property_id IS NOT NULL)                                ON VIOLATION DROP ROW,
  CONSTRAINT valid_office_id_fk    EXPECT (office_id IS NOT NULL)                                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_property_type   EXPECT (property_type IN ('戸建','マンション'))                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_built_year      EXPECT (built_year BETWEEN 1950 AND 2030)                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_lat             EXPECT (lat BETWEEN 24 AND 46)                                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_lng             EXPECT (lng BETWEEN 122 AND 146)                                ON VIOLATION DROP ROW,
  CONSTRAINT valid_listing_price   EXPECT (listing_price > 0)                                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_status          EXPECT (status IN ('売出中','商談中','成約','取下げ'))           ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_properties      PRIMARY KEY (property_id),
  CONSTRAINT fk_sl_properties_office FOREIGN KEY (office_id) REFERENCES sl_offices(office_id)
)
COMMENT '物件マスタ Silver。3,000 件、住所・面積・築年・価格・等級（省エネ/断熱/耐震）・現況ステータス。'
AS SELECT
  CAST(property_id      AS STRING)  AS property_id,
  CAST(property_type    AS STRING)  AS property_type,
  CAST(prefecture       AS STRING)  AS prefecture,
  CAST(area_code        AS STRING)  AS area_code,
  CAST(city             AS STRING)  AS city,
  CAST(district         AS STRING)  AS district,
  CAST(address          AS STRING)  AS address,
  CAST(lat              AS DOUBLE)  AS lat,
  CAST(lng              AS DOUBLE)  AS lng,
  CAST(built_year       AS INT)     AS built_year,
  CAST(floor_area_sqm   AS DOUBLE)  AS floor_area_sqm,
  CAST(land_area_sqm    AS DOUBLE)  AS land_area_sqm,
  CAST(layout           AS STRING)  AS layout,
  CAST(nearest_station  AS STRING)  AS nearest_station,
  CAST(walk_minutes     AS INT)     AS walk_minutes,
  CAST(energy_grade     AS INT)     AS energy_grade,
  CAST(insulation_grade AS INT)     AS insulation_grade,
  CAST(seismic_grade    AS INT)     AS seismic_grade,
  CAST(listing_price    AS BIGINT)  AS listing_price,
  CAST(assessment_price AS BIGINT)  AS assessment_price,
  CAST(listed_at        AS DATE)    AS listed_at,
  CAST(status           AS STRING)  AS status,
  CAST(office_id        AS STRING)  AS office_id,
  _ingested_at,
  _source_file
FROM STREAM bz_properties;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/6: sl_customers</strong>｜PK <code>customer_id</code>、FK <code>registered_office_id</code> → <code>sl_offices</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_customers (
  customer_id            STRING NOT NULL,
  name                   STRING,
  age                    INT,
  gender                 STRING,
  phone                  STRING,
  residential_area       STRING,
  area_code              STRING,
  registered_office_id   STRING NOT NULL,
  first_contact_date     DATE,
  annual_income_band     STRING,
  household_composition  STRING,
  life_stage             STRING,
  desired_property_type  STRING,
  budget_max             BIGINT,
  _ingested_at           TIMESTAMP,
  _source_file           STRING,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)            ON VIOLATION DROP ROW,
  CONSTRAINT valid_office_id_fk EXPECT (registered_office_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_age         EXPECT (age BETWEEN 18 AND 100)             ON VIOLATION DROP ROW,
  CONSTRAINT valid_gender      EXPECT (gender IN ('男','女'))              ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_customers   PRIMARY KEY (customer_id),
  CONSTRAINT fk_sl_customers_office FOREIGN KEY (registered_office_id) REFERENCES sl_offices(office_id)
)
COMMENT '顧客マスタ Silver。2,500 名、年齢 / 性別 / 居住エリア / 年収レンジ / 家族構成 / ライフステージ。'
AS SELECT
  CAST(customer_id           AS STRING) AS customer_id,
  CAST(name                  AS STRING) AS name,
  CAST(age                   AS INT)    AS age,
  CAST(gender                AS STRING) AS gender,
  CAST(phone                 AS STRING) AS phone,
  CAST(residential_area      AS STRING) AS residential_area,
  CAST(area_code             AS STRING) AS area_code,
  CAST(registered_office_id  AS STRING) AS registered_office_id,
  CAST(first_contact_date    AS DATE)   AS first_contact_date,
  CAST(annual_income_band    AS STRING) AS annual_income_band,
  CAST(household_composition AS STRING) AS household_composition,
  CAST(life_stage            AS STRING) AS life_stage,
  CAST(desired_property_type AS STRING) AS desired_property_type,
  CAST(budget_max            AS BIGINT) AS budget_max,
  _ingested_at,
  _source_file
FROM STREAM bz_customers;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 4/6: sl_market_index</strong>｜複合 PK <code>(month, area_code, property_type)</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_market_index (
  month                    DATE   NOT NULL,
  area_code                STRING NOT NULL,
  area_name                STRING,
  property_type            STRING NOT NULL,
  tx_price_index           DOUBLE,
  land_price_index         DOUBLE,
  loan_rate_fixed_35y      DOUBLE,
  loan_rate_variable       DOUBLE,
  construction_cost_index  DOUBLE,
  _ingested_at             TIMESTAMP,
  _source_file             STRING,
  CONSTRAINT valid_month       EXPECT (month IS NOT NULL)                          ON VIOLATION DROP ROW,
  CONSTRAINT valid_area_code   EXPECT (area_code IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_property_type EXPECT (property_type IN ('マンション','戸建'))   ON VIOLATION DROP ROW,
  CONSTRAINT valid_tx_index    EXPECT (tx_price_index > 0)                         ON VIOLATION DROP ROW,
  CONSTRAINT valid_land_index  EXPECT (land_price_index > 0)                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_loan_fix    EXPECT (loan_rate_fixed_35y BETWEEN 0 AND 10)       ON VIOLATION DROP ROW,
  CONSTRAINT valid_loan_var    EXPECT (loan_rate_variable BETWEEN 0 AND 10)        ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_market_index PRIMARY KEY (month, area_code, property_type)
)
COMMENT '不動産市況指標 Silver。月 × エリア × 物件種別 の取引価格指数・地価指数・住宅ローン金利・建築費指数。'
AS SELECT
  CAST(month                   AS DATE)   AS month,
  CAST(area_code               AS STRING) AS area_code,
  CAST(area_name               AS STRING) AS area_name,
  CAST(property_type           AS STRING) AS property_type,
  CAST(tx_price_index          AS DOUBLE) AS tx_price_index,
  CAST(land_price_index        AS DOUBLE) AS land_price_index,
  CAST(loan_rate_fixed_35y     AS DOUBLE) AS loan_rate_fixed_35y,
  CAST(loan_rate_variable      AS DOUBLE) AS loan_rate_variable,
  CAST(construction_cost_index AS DOUBLE) AS construction_cost_index,
  _ingested_at,
  _source_file
FROM STREAM bz_market_index;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 5/6: sl_inquiries</strong>｜PK <code>inquiry_id</code>、FK 3 本（customer_id / office_id / property_id）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_inquiries (
  inquiry_id    STRING NOT NULL,
  customer_id   STRING NOT NULL,
  office_id     STRING NOT NULL,
  property_id   STRING NOT NULL,
  inquiry_date  DATE,
  visit_kind    STRING,
  funnel_stage  STRING,
  status        STRING,
  memo          STRING,
  _ingested_at  TIMESTAMP,
  _source_file  STRING,
  CONSTRAINT valid_inquiry_id    EXPECT (inquiry_id IS NOT NULL)                                     ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id_fk EXPECT (customer_id IS NOT NULL)                                    ON VIOLATION DROP ROW,
  CONSTRAINT valid_office_id_fk  EXPECT (office_id IS NOT NULL)                                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_property_id_fk EXPECT (property_id IS NOT NULL)                                    ON VIOLATION DROP ROW,
  CONSTRAINT valid_visit_kind    EXPECT (visit_kind IN ('来店','オンライン','内見','電話'))           ON VIOLATION DROP ROW,
  CONSTRAINT valid_funnel_stage EXPECT (funnel_stage IN ('反響','内見','申込','成約','失注'))         ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status      EXPECT (status IN ('オープン','クローズ'))                            ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_inquiries   PRIMARY KEY (inquiry_id),
  CONSTRAINT fk_sl_inquiries_customer FOREIGN KEY (customer_id) REFERENCES sl_customers(customer_id),
  CONSTRAINT fk_sl_inquiries_office   FOREIGN KEY (office_id)   REFERENCES sl_offices(office_id),
  CONSTRAINT fk_sl_inquiries_property FOREIGN KEY (property_id) REFERENCES sl_properties(property_id)
)
COMMENT '内見・問合せ履歴 Silver。12,500 件、過去 24 ヶ月分。funnel_stage 列は ON VIOLATION FAIL UPDATE で厳格に検証（ファネル分析の根幹）。'
AS SELECT
  CAST(inquiry_id   AS STRING) AS inquiry_id,
  CAST(customer_id  AS STRING) AS customer_id,
  CAST(office_id    AS STRING) AS office_id,
  CAST(property_id  AS STRING) AS property_id,
  CAST(inquiry_date AS DATE)   AS inquiry_date,
  CAST(visit_kind   AS STRING) AS visit_kind,
  CAST(funnel_stage AS STRING) AS funnel_stage,
  CAST(status       AS STRING) AS status,
  CAST(memo         AS STRING) AS memo,
  _ingested_at,
  _source_file
FROM STREAM bz_inquiries;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 6/6: sl_contracts</strong>｜PK <code>contract_id</code>、FK 4 本（inquiry_id / customer_id / office_id / property_id）
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sl_contracts (
  contract_id      STRING NOT NULL,
  inquiry_id       STRING NOT NULL,
  customer_id      STRING NOT NULL,
  office_id        STRING NOT NULL,
  property_id      STRING NOT NULL,
  contract_date    DATE,
  settled_price    BIGINT,
  listing_price    BIGINT,
  discount_amount  BIGINT,
  commission       BIGINT,
  payment_method   STRING,
  _ingested_at     TIMESTAMP,
  _source_file     STRING,
  CONSTRAINT valid_contract_id     EXPECT (contract_id IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_inquiry_id_fk   EXPECT (inquiry_id IS NOT NULL)                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id_fk  EXPECT (customer_id IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_office_id_fk    EXPECT (office_id IS NOT NULL)                        ON VIOLATION DROP ROW,
  CONSTRAINT valid_property_id_fk  EXPECT (property_id IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_settled_price   EXPECT (settled_price > 0)                            ON VIOLATION DROP ROW,
  CONSTRAINT valid_discount        EXPECT (discount_amount >= 0)                         ON VIOLATION DROP ROW,
  CONSTRAINT valid_commission      EXPECT (commission > 0)                               ON VIOLATION DROP ROW,
  CONSTRAINT settled_le_listing    EXPECT (settled_price <= listing_price * 1.05)        ON VIOLATION DROP ROW,
  CONSTRAINT pk_sl_contracts      PRIMARY KEY (contract_id),
  CONSTRAINT fk_sl_contracts_inquiry  FOREIGN KEY (inquiry_id)  REFERENCES sl_inquiries(inquiry_id),
  CONSTRAINT fk_sl_contracts_customer FOREIGN KEY (customer_id) REFERENCES sl_customers(customer_id),
  CONSTRAINT fk_sl_contracts_office   FOREIGN KEY (office_id)   REFERENCES sl_offices(office_id),
  CONSTRAINT fk_sl_contracts_property FOREIGN KEY (property_id) REFERENCES sl_properties(property_id)
)
COMMENT '成約 Silver。5,000 件、過去 24 ヶ月分。仲介手数料は慣例 3%+6万 ベース。'
AS SELECT
  CAST(contract_id    AS STRING) AS contract_id,
  CAST(inquiry_id     AS STRING) AS inquiry_id,
  CAST(customer_id    AS STRING) AS customer_id,
  CAST(office_id      AS STRING) AS office_id,
  CAST(property_id    AS STRING) AS property_id,
  CAST(contract_date  AS DATE)   AS contract_date,
  CAST(settled_price  AS BIGINT) AS settled_price,
  CAST(listing_price  AS BIGINT) AS listing_price,
  CAST(discount_amount AS BIGINT) AS discount_amount,
  CAST(commission     AS BIGINT) AS commission,
  CAST(payment_method AS STRING) AS payment_method,
  _ingested_at,
  _source_file
FROM STREAM bz_contracts;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="background: #B45309; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
-- MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥇 Gold 層（NB 02 由来 3 件）</h2>
-- MAGIC <p style="margin: 4px 0 0 0; color: #F5DEB3; font-size: 13px;">Silver 層からの集計マート。差分更新は SDP が自動判定。</p>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 1/3: gd_office_monthly_sales</strong>｜複合 PK <code>(office_id, sales_month)</code>、FK <code>office_id</code> → <code>sl_offices</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_office_monthly_sales (
  office_id           STRING NOT NULL,
  sales_month         DATE   NOT NULL,
  n_contracts         BIGINT,
  total_settled_price BIGINT,
  total_commission    BIGINT,
  avg_discount_amount DOUBLE,
  CONSTRAINT pk_gd_office_monthly_sales PRIMARY KEY (office_id, sales_month),
  CONSTRAINT fk_gd_office FOREIGN KEY (office_id) REFERENCES sl_offices(office_id)
)
COMMENT '営業所 × 月別 成約サマリ。成約数 / 合計成約価格 / 合計仲介手数料 / 平均値引額。'
AS SELECT
  c.office_id,
  CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE) AS sales_month,
  COUNT(*)                                  AS n_contracts,
  SUM(c.settled_price)                      AS total_settled_price,
  SUM(c.commission)                         AS total_commission,
  AVG(c.discount_amount)                    AS avg_discount_amount
FROM sl_contracts c
GROUP BY c.office_id, CAST(DATE_TRUNC('MONTH', c.contract_date) AS DATE);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 2/3: gd_property_inventory</strong>｜複合 PK <code>(property_type, status)</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_property_inventory (
  property_type        STRING NOT NULL,
  status               STRING NOT NULL,
  n_properties         BIGINT,
  avg_listing_price    DOUBLE,
  avg_assessment_price DOUBLE,
  avg_days_on_market   DOUBLE,
  CONSTRAINT pk_gd_property_inventory PRIMARY KEY (property_type, status)
)
COMMENT '物件種別 × ステータス別 在庫サマリ。件数 / 平均売出価格 / 平均査定価格 / 平均売出日数。'
AS SELECT
  p.property_type,
  p.status,
  COUNT(*)                                AS n_properties,
  AVG(p.listing_price)                    AS avg_listing_price,
  AVG(p.assessment_price)                 AS avg_assessment_price,
  AVG(DATEDIFF(CURRENT_DATE(), p.listed_at)) AS avg_days_on_market
FROM sl_properties p
GROUP BY p.property_type, p.status;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 10px 14px; border-radius: 4px; margin: 8px 0;">
-- MAGIC <strong>📍 3/3: gd_market_linked_margin</strong>｜複合 PK <code>(month, area_code, property_type)</code>
-- MAGIC </div>

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gd_market_linked_margin (
  month                    DATE   NOT NULL,
  area_code                STRING NOT NULL,
  area_name                STRING,
  property_type            STRING NOT NULL,
  tx_price_index           DOUBLE,
  land_price_index         DOUBLE,
  loan_rate_fixed_35y      DOUBLE,
  construction_cost_index  DOUBLE,
  n_contracts              BIGINT,
  avg_discount_amount      DOUBLE,
  avg_settled_price        DOUBLE,
  CONSTRAINT pk_gd_market_linked_margin PRIMARY KEY (month, area_code, property_type)
)
COMMENT '市況指標 × 成約値引額の月次比較。エリア × 物件種別ごとに、市況指数と平均値引額の連動性を可視化。'
AS SELECT
  m.month,
  m.area_code,
  m.area_name,
  m.property_type,
  m.tx_price_index,
  m.land_price_index,
  m.loan_rate_fixed_35y,
  m.construction_cost_index,
  COALESCE(c.n_contracts, 0)              AS n_contracts,
  COALESCE(c.avg_discount_amount, 0)      AS avg_discount_amount,
  COALESCE(c.avg_settled_price, 0)        AS avg_settled_price
FROM sl_market_index m
LEFT JOIN (
  SELECT
    CAST(DATE_TRUNC('MONTH', co.contract_date) AS DATE) AS month,
    p.area_code                            AS area_code,
    p.property_type                        AS property_type,
    COUNT(*)                               AS n_contracts,
    AVG(co.discount_amount)                AS avg_discount_amount,
    AVG(co.settled_price)                  AS avg_settled_price
  FROM sl_contracts co
  JOIN sl_properties p ON co.property_id = p.property_id
  GROUP BY CAST(DATE_TRUNC('MONTH', co.contract_date) AS DATE), p.area_code, p.property_type
) c
  ON c.month = m.month
 AND c.area_code = m.area_code
 AND c.property_type = m.property_type;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
-- MAGIC <strong>✅ SDP パイプライン定義 完了</strong><br>
-- MAGIC 次は <code>04_SDPパイプライン設定手順.py</code> の UI 操作に従って、このファイルをパイプラインのソースとして登録 → 実行してください。<br>
-- MAGIC 並行して <code>03_地理空間データパイプライン.py</code> で KSJ ファイル（Shapefile / GML）を Bronze 地理空間 10 / Silver 地理空間 8 に取り込みます。<br>
-- MAGIC NB 05 で Geo JOIN・AI Functions を適用し、Gold（NB 05 由来 3 件）と Silver enrich 3 件を生成します。
-- MAGIC </div>
