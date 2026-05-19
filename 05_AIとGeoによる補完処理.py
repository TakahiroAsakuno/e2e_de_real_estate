# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 05 | AI と Geo による補完処理
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜AI Functions + 物件 × Geo JOIN → Silver enrich 3 + Gold（NB 05 由来）3 + RAG 準備</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 25 min</span>
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
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 1. Whisper - 接客録音（MP3）を文字起こし</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,音声ファイル一覧
audio_dir = f"{VOLUME_PATH}/audio"
audio_files = [f.name for f in dbutils.fs.ls(audio_dir) if f.name.endswith(".mp3")]
print(f"接客録音 MP3: {len(audio_files)} 件")

# COMMAND ----------

# DBTITLE 1,MP3 をバイナリ読込 → Whisper エンドポイントで文字起こし
# recording_NN.mp3 → IQ{NN:06d} の規約で inquiry_id にマップ
WHISPER_AVAILABLE = False
if audio_files:
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE bz_audio_transcripts AS
            SELECT
              regexp_extract(path, 'recording_(\\\\d+).mp3', 1) AS recording_idx,
              CONCAT('IQ', LPAD(regexp_extract(path, 'recording_(\\\\d+).mp3', 1), 6, '0')) AS inquiry_id,
              path AS source_path,
              ai_query(
                '{WHISPER_ENDPOINT}',
                NAMED_STRUCT('audio', content)
              ) AS transcription,
              current_timestamp() AS _ingested_at
            FROM read_files('{audio_dir}', format => 'binaryFile', pathGlobFilter => '*.mp3')
        """)
        WHISPER_AVAILABLE = True
        print(f"✅ bz_audio_transcripts: {spark.table('bz_audio_transcripts').count():,} 件")
    except Exception as e:
        print(f"⚠ Whisper エンドポイント呼出失敗（未デプロイの可能性）: {e}")
        print("   接客録音の文字起こしはスキップ。Step 7 で transcription は NULL になります。")
else:
    print("音声ファイルなし。Whisper ステップをスキップ。")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 2. ai_classify - 商談メモのトピック分類</h2>
# MAGIC </div>

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE sl_inquiries_topics AS
SELECT
  inquiry_id,
  ai_classify(
    memo,
    ARRAY('住宅ローン相談', 'リフォーム相談', 'ハザード懸念', '駅近重視', '価格交渉', '一般問合せ')
  ) AS topic,
  current_timestamp() AS _classified_at
FROM sl_inquiries
""")
print(f"✅ sl_inquiries_topics: {spark.table('sl_inquiries_topics').count():,} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 3. ai_query - 顧客プロファイル enrich</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,既存プロファイル列が空の顧客のみ enrich（コスト削減）
spark.sql(f"""
CREATE OR REPLACE TABLE sl_customers_enriched AS
WITH customer_memos AS (
  -- 顧客ごとに最新 5 件のメモを連結
  SELECT
    c.customer_id,
    c.name,
    c.age,
    c.gender,
    c.phone,
    c.residential_area,
    c.area_code,
    c.registered_office_id,
    c.first_contact_date,
    c.annual_income_band,
    c.household_composition,
    c.life_stage,
    c.desired_property_type,
    c.budget_max,
    CONCAT_WS(' | ', COLLECT_LIST(i.memo_recent)) AS recent_memos
  FROM sl_customers c
  LEFT JOIN (
    SELECT
      customer_id,
      memo AS memo_recent,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY inquiry_date DESC) AS rn
    FROM sl_inquiries
    WHERE memo IS NOT NULL
  ) i ON c.customer_id = i.customer_id AND i.rn <= 5
  GROUP BY c.customer_id, c.name, c.age, c.gender, c.phone, c.residential_area, c.area_code,
           c.registered_office_id, c.first_contact_date, c.annual_income_band, c.household_composition,
           c.life_stage, c.desired_property_type, c.budget_max
),
enrich_target AS (
  -- 既存プロファイル列のいずれかが空のレコードのみ ai_query 対象に
  SELECT
    *,
    (annual_income_band IS NULL OR household_composition IS NULL OR life_stage IS NULL
     OR desired_property_type IS NULL OR budget_max IS NULL) AS needs_enrichment
  FROM customer_memos
)
SELECT
  customer_id, name, age, gender, phone, residential_area, area_code, registered_office_id,
  first_contact_date, annual_income_band, household_composition, life_stage, desired_property_type, budget_max,
  CASE WHEN needs_enrichment THEN
    ai_query(
      '{LLM_MODEL}',
      CONCAT(
        '次の不動産仲介の商談メモから、顧客プロファイルを JSON で出力。',
        'キー: inferred_income_band, inferred_household, inferred_life_stage, inferred_desire, inferred_budget_jpy。',
        '不明は null。商談メモ: ',
        COALESCE(recent_memos, '（メモなし）')
      ),
      responseFormat => '{{"type": "json_object"}}'
    )
  ELSE NULL END AS enriched_profile_json,
  needs_enrichment,
  current_timestamp() AS _enriched_at
FROM enrich_target
""")
print(f"✅ sl_customers_enriched: {spark.table('sl_customers_enriched').count():,} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4. ai_parse_document - 重説 + パンフ PDF を構造化</h2>
# MAGIC </div>

# COMMAND ----------

pdf_dir = f"{VOLUME_PATH}/pdf"
spark.sql(f"""
CREATE OR REPLACE TABLE bz_doc_parsed AS
SELECT
  regexp_extract(path, '([^/]+)\\\\.pdf$', 1) AS doc_id,
  CASE
    WHEN path LIKE '%/jyusetsu_%' THEN '重要事項説明書'
    WHEN path LIKE '%/pamphlet_%' THEN '物件パンフ'
    ELSE 'その他'
  END AS doc_type,
  path AS source_path,
  ai_parse_document(content) AS parsed,
  current_timestamp() AS _ingested_at
FROM read_files('{pdf_dir}', format => 'binaryFile', pathGlobFilter => '*.pdf')
""")
print(f"✅ bz_doc_parsed: {spark.table('bz_doc_parsed').count():,} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 5. ai_prep_search - チャンク化 → sl_doc_chunks</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ai_prep_search の VARIANT 入力 + variant_explode で展開
# 公式仕様: ai_prep_search(parsed VARIANT) RETURNS VARIANT
# 出力 VARIANT の document.contents 配列を variant_explode で展開し、各要素から
# chunk_to_embed / chunk_to_retrieve / chunk_id / chunk_position を取り出す
spark.sql("""
CREATE OR REPLACE TABLE sl_doc_chunks AS
WITH prep AS (
  SELECT
    doc_id,
    doc_type,
    source_path,
    ai_prep_search(parsed) AS prepared
  FROM bz_doc_parsed
)
SELECT
  CONCAT(p.doc_id, '_', CAST(c.value:chunk_id AS STRING)) AS chunk_id,
  p.doc_id,
  p.doc_type,
  p.source_path,
  CAST(c.value:chunk_position AS INT) AS chunk_index,
  CAST(c.value:chunk_to_embed AS STRING) AS chunk_to_embed,
  CAST(c.value:chunk_to_retrieve AS STRING) AS chunk_to_retrieve,
  current_timestamp() AS _ingested_at
FROM prep p,
     LATERAL variant_explode(p.prepared:document:contents) AS c
""")
print(f"✅ sl_doc_chunks: {spark.table('sl_doc_chunks').count():,} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 6. Geo JOIN - 物件 × ハザード / 用途地域 / 駅 / 地価 / メッシュ人口</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📐 Geo JOIN 標準フロー</strong><br>
# MAGIC <ol style="margin-top: 6px;">
# MAGIC   <li>物件の <code>h3_r9</code> から <code>h3_kring(k=1)</code> で隣接セル候補に拡張</li>
# MAGIC   <li>ポリゴン側の <code>h3_r9</code> と JOIN（fallback=false の行）</li>
# MAGIC   <li><code>st_envelope</code> の bbox プレフィルタで粗結合</li>
# MAGIC   <li><code>st_intersects(st_geomfromwkb(物件点), st_geomfromwkb(ポリゴン))</code> で最終判定</li>
# MAGIC   <li>fallback=true のポリゴンは bbox-only fallback 経路で別途 JOIN</li>
# MAGIC </ol>
# MAGIC <strong>📐 ハザード集約</strong>：浸水深ランク・警戒区分は文字列 MAX ではなく <b>severity rank</b> で集約。<br>
# MAGIC <strong>📐 重複対策</strong>：複数候補が返り得る JOIN は <code>QUALIFY ROW_NUMBER()</code> で property_id 単一行化。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,物件に H3 r9 / r8 / r6 + k-ring を付与した一時ビュー
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW prop_with_h3 AS
SELECT
  property_id,
  lat,
  lng,
  area_code,
  city,
  property_type,
  built_year,
  listing_price,
  assessment_price,
  status,
  office_id,
  h3_longlatash3(lng, lat, {H3_RES_GEO})  AS h3_r9,
  h3_longlatash3(lng, lat, {H3_RES_MESH}) AS h3_r8,
  h3_longlatash3(lng, lat, {H3_RES_ADMIN}) AS h3_r6,
  h3_kring(h3_longlatash3(lng, lat, {H3_RES_GEO}), 1)  AS h3_r9_kring1,
  h3_kring(h3_longlatash3(lng, lat, {H3_RES_GEO}), 10) AS h3_r9_kring10,
  st_point(lng, lat) AS prop_point
FROM sl_properties
""")

# COMMAND ----------

# DBTITLE 1,物件 × 用途地域（H3 + bbox + st_intersects + fallback、複数該当時は最初の 1 つ）
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_zoning AS
WITH h3_path AS (
  SELECT
    p.property_id,
    z.zoning_code,
    z.zoning_name
  FROM prop_with_h3 p
  JOIN sl_geo_zoning z
    ON ARRAY_CONTAINS(p.h3_r9_kring1, z.h3_r9)
   AND z.polyfill_fallback = false
  WHERE st_intersects(p.prop_point, st_envelope(st_geomfromwkb(z.geom_wkb_simplified)))
    AND st_intersects(p.prop_point, st_geomfromwkb(z.geom_wkb))
),
fallback_path AS (
  SELECT p.property_id, z.zoning_code, z.zoning_name
  FROM prop_with_h3 p
  JOIN (
    SELECT DISTINCT zoning_id, zoning_code, zoning_name, geom_wkb, geom_wkb_simplified
    FROM sl_geo_zoning WHERE polyfill_fallback = true
  ) z
    ON st_intersects(p.prop_point, st_envelope(st_geomfromwkb(z.geom_wkb_simplified)))
   AND st_intersects(p.prop_point, st_geomfromwkb(z.geom_wkb))
),
combined AS (
  SELECT property_id, zoning_code, zoning_name FROM h3_path
  UNION
  SELECT property_id, zoning_code, zoning_name FROM fallback_path
)
SELECT property_id, zoning_code, zoning_name
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY zoning_code) = 1
""")

# COMMAND ----------

# DBTITLE 1,物件 × 浸水想定（severity rank で集約：浸水深の深い方を採用）
# A31 浸水深ランクの severity rank（KSJ 仕様：1=0-0.5m, 2=0.5-3m, 3=3-5m, 4=5-10m, 5=10-20m, 6=20m〜）
# 文字列の場合は最大長で粗ソート
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_flood AS
WITH h3_path AS (
  SELECT p.property_id, f.flood_depth_class, f.duration_class
  FROM prop_with_h3 p
  JOIN sl_geo_flood f
    ON ARRAY_CONTAINS(p.h3_r9_kring1, f.h3_r9)
   AND f.polyfill_fallback = false
  WHERE st_intersects(p.prop_point, st_envelope(st_geomfromwkb(f.geom_wkb_simplified)))
    AND st_intersects(p.prop_point, st_geomfromwkb(f.geom_wkb))
),
fallback_path AS (
  SELECT p.property_id, f.flood_depth_class, f.duration_class
  FROM prop_with_h3 p
  JOIN (
    SELECT DISTINCT flood_id, flood_depth_class, duration_class, geom_wkb, geom_wkb_simplified
    FROM sl_geo_flood WHERE polyfill_fallback = true
  ) f
    ON st_intersects(p.prop_point, st_envelope(st_geomfromwkb(f.geom_wkb_simplified)))
   AND st_intersects(p.prop_point, st_geomfromwkb(f.geom_wkb))
),
combined AS (
  SELECT property_id, flood_depth_class, duration_class,
    -- severity rank（数値化可能なら数値、そうでなければ文字列長で代替）
    COALESCE(TRY_CAST(flood_depth_class AS INT), LENGTH(CAST(flood_depth_class AS STRING))) AS depth_rank
  FROM (SELECT * FROM h3_path UNION SELECT * FROM fallback_path)
)
SELECT property_id, flood_depth_class, duration_class
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY depth_rank DESC) = 1
""")

# COMMAND ----------

# DBTITLE 1,物件 × 土砂災害（severity rank：特別警戒区域 > 警戒区域）
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_landslide AS
WITH h3_path AS (
  SELECT p.property_id, l.hazard_type, l.hazard_grade
  FROM prop_with_h3 p
  JOIN sl_geo_landslide l
    ON ARRAY_CONTAINS(p.h3_r9_kring1, l.h3_r9)
   AND l.polyfill_fallback = false
  WHERE st_intersects(p.prop_point, st_envelope(st_geomfromwkb(l.geom_wkb_simplified)))
    AND st_intersects(p.prop_point, st_geomfromwkb(l.geom_wkb))
),
fallback_path AS (
  SELECT p.property_id, l.hazard_type, l.hazard_grade
  FROM prop_with_h3 p
  JOIN (
    SELECT DISTINCT landslide_id, hazard_type, hazard_grade, geom_wkb, geom_wkb_simplified
    FROM sl_geo_landslide WHERE polyfill_fallback = true
  ) l
    ON st_intersects(p.prop_point, st_envelope(st_geomfromwkb(l.geom_wkb_simplified)))
   AND st_intersects(p.prop_point, st_geomfromwkb(l.geom_wkb))
),
combined AS (
  SELECT property_id, hazard_type, hazard_grade,
    CASE
      WHEN hazard_grade = '特別警戒区域' THEN 2
      WHEN hazard_grade = '警戒区域'     THEN 1
      ELSE 0
    END AS grade_rank
  FROM (SELECT * FROM h3_path UNION SELECT * FROM fallback_path)
)
SELECT property_id, hazard_type, hazard_grade
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY grade_rank DESC) = 1
""")

# COMMAND ----------

# DBTITLE 1,物件 × 最寄駅距離（k=10 まで拡張、2km 圏内、最近傍 1 駅）
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_station AS
WITH candidates AS (
  SELECT
    p.property_id,
    s.station_name,
    s.line_name,
    st_distancesphere(p.prop_point, st_point(s.lng, s.lat)) AS distance_m
  FROM prop_with_h3 p
  JOIN sl_geo_stations s
    ON ARRAY_CONTAINS(p.h3_r9_kring10, s.h3_r9)
)
SELECT
  property_id,
  station_name AS nearest_station_calc,
  line_name AS nearest_line,
  distance_m AS nearest_station_distance_m
FROM candidates
WHERE distance_m <= 2000
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY distance_m) = 1
""")

# COMMAND ----------

# DBTITLE 1,物件 × 地価公示（adaptive k-ring 相当 + 加重平均 + NULL_REASON）
# 候補生成：k=10 で広めに取り、距離 ≤ 2000m でフィルタ
# 最近傍 1〜3 点を距離逆数で加重平均
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_landprice AS
WITH candidates AS (
  SELECT
    p.property_id,
    lp.source,
    lp.year,
    lp.price,
    st_distancesphere(p.prop_point, st_geomfromwkb(lp.geom_wkb)) AS distance_m
  FROM prop_with_h3 p
  JOIN sl_geo_landprice lp
    ON ARRAY_CONTAINS(p.h3_r9_kring10, lp.h3_r9)
  WHERE st_distancesphere(p.prop_point, st_geomfromwkb(lp.geom_wkb)) <= 2000
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY distance_m, year DESC) AS rn
  FROM candidates
),
top3 AS (
  SELECT
    property_id,
    -- 距離逆数で加重平均（距離 0 を避けるため +1）
    SUM(price * (1.0 / (distance_m + 1.0))) / SUM(1.0 / (distance_m + 1.0)) AS weighted_landprice,
    MIN(distance_m) AS nearest_landprice_distance_m,
    COUNT(*) AS n_landprice_points,
    MAX_BY(source, distance_m * -1)  AS nearest_landprice_source,  -- 最も近い source
    MAX_BY(year, distance_m * -1)    AS nearest_landprice_year     -- 最も近い year
  FROM ranked
  WHERE rn <= 3
  GROUP BY property_id
),
all_props AS (
  SELECT
    p.property_id,
    t.weighted_landprice AS nearest_landprice,
    t.nearest_landprice_distance_m AS landprice_distance_m,
    t.nearest_landprice_source AS landprice_source,
    t.nearest_landprice_year AS landprice_year,
    t.n_landprice_points,
    CASE
      WHEN t.weighted_landprice IS NULL THEN 'no_landprice_within_2km'
      ELSE NULL
    END AS landprice_null_reason
  FROM prop_with_h3 p
  LEFT JOIN top3 t ON p.property_id = t.property_id
)
SELECT * FROM all_props
""")

# COMMAND ----------

# DBTITLE 1,物件 × メッシュ人口（H3 r8 + bbox + st_intersects + fallback、最新年）
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_pop AS
WITH h3_path AS (
  SELECT p.property_id, pm.year, pm.pop_total
  FROM prop_with_h3 p
  JOIN sl_geo_pop_mesh pm
    ON p.h3_r8 = pm.h3_r8
   AND pm.polyfill_fallback = false
  WHERE st_intersects(p.prop_point, st_envelope(st_geomfromwkb(pm.geom_wkb_simplified)))
    AND st_intersects(p.prop_point, st_geomfromwkb(pm.geom_wkb))
),
fallback_path AS (
  SELECT p.property_id, pm.year, pm.pop_total
  FROM prop_with_h3 p
  JOIN (
    SELECT DISTINCT mesh_pk, year, pop_total, geom_wkb, geom_wkb_simplified
    FROM sl_geo_pop_mesh WHERE polyfill_fallback = true
  ) pm
    ON st_intersects(p.prop_point, st_envelope(st_geomfromwkb(pm.geom_wkb_simplified)))
   AND st_intersects(p.prop_point, st_geomfromwkb(pm.geom_wkb))
)
SELECT property_id, year AS mesh_pop_year, pop_total AS mesh_pop_total
FROM (SELECT * FROM h3_path UNION SELECT * FROM fallback_path)
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY year DESC) = 1
""")

# COMMAND ----------

# DBTITLE 1,物件 × 行政区域（admin_code 取得、最初の 1 つ）
spark.sql("""
CREATE OR REPLACE TEMP VIEW prop_admin AS
WITH joined AS (
  SELECT
    p.property_id,
    a.admin_code,
    a.admin_name,
    a.representative_lat,
    a.representative_lng
  FROM prop_with_h3 p
  JOIN sl_geo_admin a
    ON ARRAY_CONTAINS(a.h3_r6_array, p.h3_r6)
  WHERE st_intersects(p.prop_point, st_envelope(st_geomfromwkb(a.geom_wkb_simplified)))
    AND st_intersects(p.prop_point, st_geomfromwkb(a.geom_wkb))
)
SELECT property_id, admin_code, admin_name, representative_lat, representative_lng
FROM joined
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY admin_code) = 1
""")

# COMMAND ----------

# DBTITLE 1,統合 → sl_property_geo_enriched（マスキング事前計算列を含む）
spark.sql(f"""
CREATE OR REPLACE TABLE sl_property_geo_enriched AS
SELECT
  p.property_id,
  p.property_type,
  p.area_code,
  p.lat,
  p.lng,
  p.listing_price,
  p.assessment_price,
  p.status,
  p.office_id,
  -- 用途地域
  z.zoning_code,
  z.zoning_name,
  -- 浸水想定
  f.flood_depth_class,
  f.duration_class,
  -- 土砂災害
  ls.hazard_type  AS landslide_hazard_type,
  ls.hazard_grade AS landslide_hazard_grade,
  -- 最寄駅
  st_.nearest_station_calc,
  st_.nearest_line,
  st_.nearest_station_distance_m,
  -- 地価（加重平均 + NULL_REASON）
  lp.landprice_source,
  lp.landprice_year,
  lp.nearest_landprice,
  lp.landprice_distance_m,
  lp.n_landprice_points,
  lp.landprice_null_reason,
  CASE
    WHEN lp.nearest_landprice IS NULL OR lp.nearest_landprice = 0 THEN NULL
    ELSE CAST(p.assessment_price AS DOUBLE) / lp.nearest_landprice
  END AS price_vs_landprice_ratio,
  -- メッシュ人口（最新年）
  pop.mesh_pop_total,
  pop.mesh_pop_year,
  -- 行政区域
  ad.admin_code,
  ad.admin_name,
  -- マスキング: analyst（H3 r8 セル中心の lat/lng）。st_geomfromwkb で型変換
  st_y(st_geomfromwkb(h3_centeraswkb(h3_longlatash3(p.lng, p.lat, {H3_RES_MESH})))) AS analyst_mask_lat,
  st_x(st_geomfromwkb(h3_centeraswkb(h3_longlatash3(p.lng, p.lat, {H3_RES_MESH})))) AS analyst_mask_lng,
  -- マスキング: viewer（市区町村ポリゴン代表点）
  ad.representative_lat AS viewer_mask_lat,
  ad.representative_lng AS viewer_mask_lng,
  current_timestamp() AS _enriched_at
FROM sl_properties p
LEFT JOIN prop_zoning    z   ON p.property_id = z.property_id
LEFT JOIN prop_flood     f   ON p.property_id = f.property_id
LEFT JOIN prop_landslide ls  ON p.property_id = ls.property_id
LEFT JOIN prop_station   st_ ON p.property_id = st_.property_id
LEFT JOIN prop_landprice lp  ON p.property_id = lp.property_id
LEFT JOIN prop_pop       pop ON p.property_id = pop.property_id
LEFT JOIN prop_admin     ad  ON p.property_id = ad.property_id
""")
# property_id 粒度を検証
total = spark.table("sl_property_geo_enriched").count()
distinct = spark.sql("SELECT COUNT(DISTINCT property_id) FROM sl_property_geo_enriched").collect()[0][0]
assert total == distinct, f"property_id 粒度違反: total={total}, distinct={distinct}"
print(f"✅ sl_property_geo_enriched: {total:,} 件（property_id 単一行を保証）")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 7. sl_inquiries_enriched - Whisper + ai_classify 結合</h2>
# MAGIC </div>

# COMMAND ----------

# Whisper が成功した場合のみ transcription を JOIN
if WHISPER_AVAILABLE:
    # bz_audio_transcripts を inquiry_id 単一行に確定（regex 非一致や重複ファイル対策）
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW v_audio_uniq AS
        SELECT inquiry_id, transcription
        FROM bz_audio_transcripts
        WHERE recording_idx IS NOT NULL AND recording_idx <> ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY inquiry_id ORDER BY source_path) = 1
    """)
    spark.sql("""
        CREATE OR REPLACE TABLE sl_inquiries_enriched AS
        SELECT
          i.inquiry_id, i.customer_id, i.office_id, i.property_id,
          i.inquiry_date, i.visit_kind, i.funnel_stage, i.status, i.memo,
          t.topic,
          CAST(au.transcription AS STRING) AS transcription,
          current_timestamp() AS _enriched_at
        FROM sl_inquiries i
        LEFT JOIN sl_inquiries_topics t ON i.inquiry_id = t.inquiry_id
        LEFT JOIN v_audio_uniq au ON i.inquiry_id = au.inquiry_id
    """)
else:
    spark.sql("""
        CREATE OR REPLACE TABLE sl_inquiries_enriched AS
        SELECT
          i.inquiry_id, i.customer_id, i.office_id, i.property_id,
          i.inquiry_date, i.visit_kind, i.funnel_stage, i.status, i.memo,
          t.topic,
          CAST(NULL AS STRING) AS transcription,
          current_timestamp() AS _enriched_at
        FROM sl_inquiries i
        LEFT JOIN sl_inquiries_topics t ON i.inquiry_id = t.inquiry_id
    """)

# Whisper の有無に関わらず inquiry_id 単一行を保証する assert
n_e = spark.table("sl_inquiries_enriched").count()
n_i = spark.table("sl_inquiries").count()
assert n_e == n_i, f"sl_inquiries_enriched 行数異常: {n_e} != sl_inquiries {n_i}"
print(f"✅ sl_inquiries_enriched: {n_e:,} 件（inquiry_id 単一行を保証）")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #B45309; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 8. Gold（NB 05 由来）3 件</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,1/3: gd_property_hazard_summary（エリア × ハザード別の値引き率・売出期間）
spark.sql("""
CREATE OR REPLACE TABLE gd_property_hazard_summary AS
WITH prop_hazard AS (
  -- 物件単位（1 物件 1 行）でハザード区分を確定
  SELECT
    pge.property_id,
    pge.area_code,
    CASE
      WHEN pge.landslide_hazard_grade = '特別警戒区域' THEN 'レッドゾーン'
      WHEN pge.landslide_hazard_grade = '警戒区域'     THEN 'イエローゾーン'
      WHEN pge.flood_depth_class IS NOT NULL          THEN '浸水想定区域'
      ELSE 'ハザードなし'
    END AS hazard_type,
    p.listing_price,
    p.listed_at,
    p.status
  FROM sl_property_geo_enriched pge
  JOIN sl_properties p ON pge.property_id = p.property_id
),
contract_metrics AS (
  -- 物件単位で契約集計（1 物件複数契約のファンアウトを防ぐため、物件単位で平均化）
  SELECT
    property_id,
    AVG(discount_amount) AS avg_discount_amount_per_prop,
    AVG(CAST(discount_amount AS DOUBLE) / NULLIF(listing_price, 0)) AS avg_discount_rate_per_prop,
    AVG(DATEDIFF(contract_date, listed_at_first)) AS avg_days_to_settle_per_prop
  FROM (
    SELECT c.property_id, c.discount_amount, c.contract_date, p.listing_price,
           FIRST_VALUE(p.listed_at) OVER (PARTITION BY c.property_id ORDER BY c.contract_date) AS listed_at_first
    FROM sl_contracts c
    JOIN sl_properties p ON c.property_id = p.property_id
  )
  GROUP BY property_id
)
SELECT
  ph.area_code,
  ph.hazard_type,
  COUNT(DISTINCT ph.property_id) AS n_properties,
  AVG(cm.avg_discount_amount_per_prop) AS avg_discount_amount,
  AVG(cm.avg_discount_rate_per_prop)   AS avg_discount_rate,
  AVG(cm.avg_days_to_settle_per_prop)  AS avg_days_to_settle
FROM prop_hazard ph
LEFT JOIN contract_metrics cm ON ph.property_id = cm.property_id
GROUP BY ph.area_code, ph.hazard_type
""")
print(f"✅ gd_property_hazard_summary: {spark.table('gd_property_hazard_summary').count():,} 件")

# COMMAND ----------

# DBTITLE 1,2/3: gd_contract_discount_score（契約単位、1 契約 1 行）
spark.sql("""
CREATE OR REPLACE TABLE gd_contract_discount_score AS
WITH base AS (
  SELECT
    c.contract_id, c.inquiry_id, c.customer_id, c.office_id, c.property_id,
    c.contract_date, c.settled_price, c.listing_price, c.discount_amount,
    CAST(c.discount_amount AS DOUBLE) / NULLIF(c.listing_price, 0) AS discount_rate,
    pge.area_code, pge.property_type, pge.zoning_name,
    pge.flood_depth_class, pge.landslide_hazard_grade, pge.price_vs_landprice_ratio
  FROM sl_contracts c
  JOIN sl_property_geo_enriched pge ON c.property_id = pge.property_id
)
SELECT
  contract_id, inquiry_id, customer_id, office_id, property_id,
  contract_date, settled_price, listing_price, discount_amount, discount_rate,
  NTILE(5) OVER (ORDER BY discount_rate) AS discount_quintile,
  area_code, property_type, zoning_name,
  flood_depth_class, landslide_hazard_grade, price_vs_landprice_ratio
FROM base
""")
# 1 契約 1 行を検証
n = spark.table("gd_contract_discount_score").count()
nd = spark.sql("SELECT COUNT(DISTINCT contract_id) FROM gd_contract_discount_score").collect()[0][0]
assert n == nd, f"contract_id 粒度違反: total={n}, distinct={nd}"
print(f"✅ gd_contract_discount_score: {n:,} 件（contract_id 単一行を保証）")

# COMMAND ----------

# DBTITLE 1,3/3: gd_customer_rm_segment（RM 分析）
# 重要：inquiries と contracts を同時 JOIN すると monetary が問い合わせ件数倍に増幅するため、別 CTE で集約
spark.sql("""
CREATE OR REPLACE TABLE gd_customer_rm_segment AS
WITH inq_agg AS (
  SELECT customer_id, MAX(inquiry_date) AS last_contact_date
  FROM sl_inquiries
  GROUP BY customer_id
),
con_agg AS (
  SELECT
    customer_id,
    COUNT(DISTINCT contract_id) AS n_contracts,
    COALESCE(SUM(settled_price), 0) AS total_settled_price
  FROM sl_contracts
  GROUP BY customer_id
),
cust_agg AS (
  SELECT
    c.customer_id, c.life_stage, c.age, c.gender,
    i.last_contact_date,
    COALESCE(co.n_contracts, 0) AS n_contracts,
    COALESCE(co.total_settled_price, 0) AS total_settled_price
  FROM sl_customers c
  LEFT JOIN inq_agg i ON c.customer_id = i.customer_id
  LEFT JOIN con_agg co ON c.customer_id = co.customer_id
),
scored AS (
  SELECT *,
    -- last_contact_date が NULL（未接触顧客）の場合は最古扱いとして 9999 を入れる
    COALESCE(MONTHS_BETWEEN(CURRENT_DATE(), last_contact_date), 9999) AS months_since_last_contact_filled,
    MONTHS_BETWEEN(CURRENT_DATE(), last_contact_date) AS months_since_last_contact,
    -- recency_quintile：経過月が小さい（最近接触）ほど高位の quintile（=5）になるよう DESC
    -- 未接触（NULL→9999）は最古扱いで quintile=1 側に入る
    NTILE(5) OVER (ORDER BY COALESCE(MONTHS_BETWEEN(CURRENT_DATE(), last_contact_date), 9999) DESC) AS recency_quintile,
    NTILE(5) OVER (ORDER BY total_settled_price) AS monetary_quintile,
    last_contact_date IS NULL AS is_uncontacted
  FROM cust_agg
)
SELECT customer_id, life_stage, age, gender, last_contact_date, n_contracts, total_settled_price,
  months_since_last_contact, recency_quintile, monetary_quintile,
  CASE
    WHEN is_uncontacted THEN '未接触顧客'
    -- recency_quintile が高い = 最近接触、低い = 古い接触
    WHEN recency_quintile >= 4 AND monetary_quintile >= 4 THEN '優良顧客'             -- 最近接触 + 高額
    WHEN recency_quintile >= 4 AND monetary_quintile <= 2 THEN '新規育成候補'         -- 最近接触 + 低額（育成余地）
    WHEN recency_quintile <= 2 AND monetary_quintile >= 4 THEN '高額離反候補'         -- 古い接触 + 高額（離反リスク）
    WHEN recency_quintile <= 2 AND monetary_quintile <= 2 THEN '休眠顧客'             -- 古い接触 + 低額
    ELSE '一般顧客'
  END AS rm_segment
FROM scored
""")
print(f"✅ gd_customer_rm_segment: {spark.table('gd_customer_rm_segment').count():,} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ AI と Geo による補完処理 完了</strong><br>
# MAGIC Silver enrich 3 + Gold（NB 05 由来）3 + RAG 準備（bz_doc_parsed / sl_doc_chunks）が完了しました。<br>
# MAGIC 次は <code>06_テーブル設定.py</code> で UC ガバナンス（コメント / PK・FK / マスキング）を適用。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,作成テーブル件数サマリ
nb05_tables = [
    "sl_inquiries_enriched", "sl_customers_enriched", "sl_property_geo_enriched",
    "bz_doc_parsed", "sl_doc_chunks",
    "gd_property_hazard_summary", "gd_contract_discount_score", "gd_customer_rm_segment",
]
print("=== NB 05 作成テーブル サマリ ===")
for t in nb05_tables:
    try:
        n = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.{t}").count()
        print(f"  {t:32s}: {n:>10,} 行")
    except Exception as e:
        print(f"  {t:32s}: ⚠ {e}")
