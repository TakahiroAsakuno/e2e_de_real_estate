# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 06 | テーブル設定（UC ガバナンス）
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜コメント / PK・FK / PII + 多段位置情報マスキング</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 15 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC <ul style="margin-top: 8px;">
# MAGIC   <li><b>Step 1</b>：全テーブルに COMMENT を適用（Bronze 簡素 / Silver/Gold 詳細）</li>
# MAGIC   <li><b>Step 2</b>：05 で作成した Silver enrich + Gold（NB 05 由来）+ Silver RAG に PK/FK を ALTER TABLE で追加</li>
# MAGIC   <li><b>Step 3</b>：PII マスキング関数を定義（mask_name / mask_phone / mask_address_detail / mask_geo_admin_only / mask_geo_lat / mask_geo_lng）</li>
# MAGIC   <li><b>Step 4</b>：列マスクを各テーブルに適用</li>
# MAGIC   <li><b>Step 5</b>：結果確認（PK/FK 一覧、コメント、マスク列）</li>
# MAGIC </ul>
# MAGIC <strong>📌 権限グループ</strong>：00_config の <code>GROUP_ADMIN</code> / <code>GROUP_ANALYST</code> / <code>GROUP_VIEWER</code>（実値：<code>real_estate_admin</code> / <code>real_estate_analyst</code> / <code>real_estate_viewer</code>）が Account Console に作成済みである前提です。
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
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 1. テーブルコメントを一括適用</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,テーブルコメント辞書（NB 06 時点で存在する 41 件。MV 4 + Metric View 4 + VS Index 1 は NB 09/07/08 で作成時にコメント付与）
TABLE_COMMENTS = {
    # Bronze 構造化（6）
    "bz_offices":           "営業所マスタ Bronze。30 拠点、戸建仲介 / マンション仲介 / 投資物件専門の 3 業態。",
    "bz_properties":        "物件マスタ Bronze。3,000 件、戸建 / マンションの住宅地。reinfolib 取引価格情報を種に合成。",
    "bz_customers":         "顧客マスタ Bronze。2,500 名。年収レンジ / 家族構成 / ライフステージなどのプロファイル列を含む。",
    "bz_market_index":      "不動産市況指標 Bronze。月 × エリア × 物件種別 の取引価格指数 / 地価指数 / 住宅ローン金利 / 建築費指数。",
    "bz_inquiries":         "内見・問合せ履歴 Bronze。12,500 件、過去 24 ヶ月分。直近 5 日は日次 CSV、それ以前は history.csv。",
    "bz_contracts":         "成約 Bronze。5,000 件、過去 24 ヶ月分。直近 5 日は日次 CSV。",
    # Bronze 地理空間（10）
    "bz_geo_zoning":        "用途地域 Bronze（KSJ A29）。ポリゴン + 用途地域コード。",
    "bz_geo_flood":         "洪水浸水想定区域 Bronze（KSJ A31、想定最大規模）。ポリゴン + 浸水深ランク。",
    "bz_geo_landslide":     "土砂災害警戒区域 Bronze（KSJ A33）。ポリゴン + 警戒区分（特別警戒区域 / 警戒区域）。",
    "bz_geo_landprice_l01": "地価公示 Bronze（KSJ L01）。ポイント + 年次価格。",
    "bz_geo_landprice_l02": "都道府県地価調査 Bronze（KSJ L02）。ポイント + 基準地価格。",
    "bz_geo_stations":      "鉄道駅 Bronze（KSJ N02）。ポイント + 路線・運営会社属性。",
    "bz_geo_admin":         "行政区域 Bronze（KSJ N03）。admin_code で dissolve 済み。representative_lat/lng（ポリゴン内代表点）保持。",
    "bz_geo_pop_mesh":      "1km メッシュ別将来推計人口 Bronze（KSJ mesh1000）。ポリゴン + 推計年 + 人口。",
    "bz_geo_isj":           "大字・町丁目位置参照情報 Bronze（ISJ）。住所文字列 → 代表緯度経度。",
    "bz_geo_osm_poi":       "OpenStreetMap POI Bronze。コンビニ / スーパー / 学校等。",
    # Bronze RAG（1）
    "bz_doc_parsed":        "重要事項説明書 + 物件パンフ PDF Bronze。ai_parse_document で構造化（VARIANT）。",
    # Silver 構造化（6）
    "sl_offices":           "営業所マスタ Silver。30 拠点、業態 / 都道府県 / 緯度経度の妥当性チェック付き。",
    "sl_properties":        "物件マスタ Silver。3,000 件、住所 / 面積 / 築年 / 価格 / 等級（省エネ・断熱・耐震）/ 現況ステータス。",
    "sl_customers":         "顧客マスタ Silver。2,500 名。氏名・電話は PII マスキング対象。",
    "sl_market_index":      "不動産市況指標 Silver。月 × エリア × 物件種別 の主要指数。",
    "sl_inquiries":         "内見・問合せ履歴 Silver。12,500 件、過去 24 ヶ月分。funnel_stage は ON VIOLATION FAIL UPDATE で厳格検証。",
    "sl_contracts":         "成約 Silver。5,000 件、過去 24 ヶ月分。仲介手数料は慣例 3%+6万 ベース。",
    # Silver 地理空間（8、論理キーのみ）
    "sl_geo_zoning":        "用途地域 Silver。H3 r9 候補抽出列 + 原典 geom_wkb + 簡略化 geom_wkb_simplified + polyfill_fallback フラグ。",
    "sl_geo_flood":         "浸水想定 Silver（想定最大規模）。H3 r9 + 浸水深ランク + fallback フラグ。",
    "sl_geo_landslide":     "土砂災害警戒区域 Silver。H3 r9 + 警戒区分 + fallback フラグ。",
    "sl_geo_landprice":     "地価公示 L01 + 基準地 L02 統合 Silver（source 列で識別）。最近傍検索用 H3 r9 列付与。",
    "sl_geo_stations":      "鉄道駅 Silver。緯度経度 + 路線属性 + H3 r9 列。",
    "sl_geo_admin":         "行政区域 Silver。admin_code 単位（dissolve 済み）。H3 r6 配列 + representative_lat/lng 保持（viewer 権限マスキング用）。",
    "sl_geo_pop_mesh":      "メッシュ人口 Silver。H3 r8 + 推計年 + 人口総数 + fallback フラグ。",
    "sl_geo_osm_poi":       "OSM POI Silver。緯度経度 + カテゴリ + H3 r9 列。",
    # Silver RAG（1）
    "sl_doc_chunks":        "PDF チャンク Silver。ai_prep_search で生成。VS Index のソース。",
    # Silver enrich（3）
    "sl_inquiries_enriched":     "内見・問合せ enrich Silver。ai_classify トピック + Whisper 文字起こし結合。",
    "sl_customers_enriched":     "顧客 enrich Silver。ai_query で年収・家族構成・ライフステージを推論補完（needs_enrichment=true のみ）。",
    "sl_property_geo_enriched":  "物件 × Geo enrich Silver。用途地域 / ハザード / 最寄駅 / 地価公示 / メッシュ人口 / 行政区域 + マスキング事前計算列。",
    # Gold（6）
    "gd_office_monthly_sales":   "営業所 × 月別 成約サマリ Gold（NB 02 由来）。",
    "gd_property_inventory":     "物件種別 × ステータス別 在庫サマリ Gold（NB 02 由来）。",
    "gd_market_linked_margin":   "市況指標 × 成約値引額の月次比較 Gold（NB 02 由来）。",
    "gd_property_hazard_summary":"エリア × ハザード別の値引き率・売出期間 Gold（NB 05 由来）。",
    "gd_contract_discount_score":"契約単位の値引きスコア Gold（NB 05 由来、1 契約 1 行）。",
    "gd_customer_rm_segment":    "RM 分析セグメント Gold（NB 05 由来）。優良 / 新規育成 / 高額離反 / 休眠 / 未接触 / 一般。",
}

# COMMAND ----------

# DBTITLE 1,テーブルコメント一括適用
applied = 0
for tbl, comment in TABLE_COMMENTS.items():
    try:
        # シングルクオートをエスケープ
        safe_comment = comment.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {MY_CATALOG}.{MY_SCHEMA}.{tbl} IS '{safe_comment}'")
        applied += 1
    except Exception as e:
        print(f"⚠ {tbl}: {e}")
print(f"✅ テーブルコメント適用: {applied}/{len(TABLE_COMMENTS)} 件")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 2. PK / FK を 05 由来テーブルに追加</h2>
# MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">02 で SDP インライン宣言済みの分は除外。Silver 地理空間は論理キーのみのためスキップ</p>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,既存制約をチェックするヘルパー（再実行性のため）
def constraint_exists(table_name: str, constraint_name: str) -> bool:
    df = spark.sql(f"""
        SELECT 1 FROM {MY_CATALOG}.information_schema.table_constraints
        WHERE table_schema = '{MY_SCHEMA}'
          AND table_name = '{table_name}'
          AND constraint_name = '{constraint_name}'
        LIMIT 1
    """)
    return df.count() > 0


# COMMAND ----------

# DBTITLE 1,Silver enrich + Gold（NB 05 由来）+ Silver RAG の PK 追加（整合確認 + 再実行性）
PK_DEFS = [
    ("sl_doc_chunks",             "pk_sl_doc_chunks",             "chunk_id"),
    ("sl_inquiries_enriched",     "pk_sl_inquiries_enriched",     "inquiry_id"),
    ("sl_customers_enriched",     "pk_sl_customers_enriched",     "customer_id"),
    ("sl_property_geo_enriched",  "pk_sl_property_geo_enriched",  "property_id"),
    ("gd_property_hazard_summary","pk_gd_property_hazard_summary","area_code, hazard_type"),
    ("gd_contract_discount_score","pk_gd_contract_discount_score","contract_id"),
    ("gd_customer_rm_segment",    "pk_gd_customer_rm_segment",    "customer_id"),
]

pk_failures = []
for tbl, pk_name, cols in PK_DEFS:
    if constraint_exists(tbl, pk_name):
        print(f"  ⏭  {tbl} PK ({cols}): 既存制約のためスキップ")
        continue
    col_list = [x.strip() for x in cols.split(",")]
    # PK 整合確認：重複行数チェック
    dup = spark.sql(f"""
        SELECT COUNT(*) AS n FROM (
            SELECT {cols}, COUNT(*) AS c
            FROM {tbl}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["n"]
    if dup > 0:
        pk_failures.append(f"{tbl}: PK 候補 ({cols}) に {dup} 件の重複")
        print(f"  ❌ {tbl} PK ({cols}): 重複 {dup} 件、PK 制約をスキップ")
        continue
    try:
        for c in col_list:
            spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {c} SET NOT NULL")
        spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {pk_name} PRIMARY KEY ({cols})")
        print(f"  ✅ {tbl} PK ({cols})")
    except Exception as e:
        pk_failures.append(f"{tbl} PK: {e}")
        print(f"  ❌ {tbl} PK: {e}")

# COMMAND ----------

# DBTITLE 1,Silver enrich + Gold（NB 05 由来）の FK 追加
FK_DEFS = [
    # sl_inquiries_enriched
    ("sl_inquiries_enriched", "fk_sli_enr_customer", "customer_id", "sl_customers", "customer_id"),
    ("sl_inquiries_enriched", "fk_sli_enr_office",   "office_id",   "sl_offices",   "office_id"),
    ("sl_inquiries_enriched", "fk_sli_enr_property", "property_id", "sl_properties","property_id"),
    # sl_customers_enriched
    ("sl_customers_enriched", "fk_slc_enr_customer", "customer_id", "sl_customers", "customer_id"),
    # sl_property_geo_enriched
    ("sl_property_geo_enriched", "fk_slpge_property", "property_id", "sl_properties", "property_id"),
    # gd_contract_discount_score
    ("gd_contract_discount_score", "fk_gdds_contract", "contract_id", "sl_contracts", "contract_id"),
    ("gd_contract_discount_score", "fk_gdds_office",   "office_id",   "sl_offices",   "office_id"),
    ("gd_contract_discount_score", "fk_gdds_property", "property_id", "sl_properties","property_id"),
    ("gd_contract_discount_score", "fk_gdds_customer", "customer_id", "sl_customers", "customer_id"),
    # gd_customer_rm_segment
    ("gd_customer_rm_segment", "fk_gdcrm_customer", "customer_id", "sl_customers", "customer_id"),
]

fk_failures = []
for tbl, fk_name, col, ref_tbl, ref_col in FK_DEFS:
    if constraint_exists(tbl, fk_name):
        print(f"  ⏭  {tbl}.{col} → {ref_tbl}.{ref_col}: 既存制約のためスキップ")
        continue
    # FK 整合確認：orphan 行数チェック
    orphan = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {tbl} t
        LEFT JOIN {ref_tbl} r ON t.{col} = r.{ref_col}
        WHERE t.{col} IS NOT NULL AND r.{ref_col} IS NULL
    """).collect()[0]["n"]
    if orphan > 0:
        fk_failures.append(f"{tbl}.{col} → {ref_tbl}.{ref_col}: orphan {orphan} 件")
        print(f"  ❌ {tbl}.{col} → {ref_tbl}.{ref_col}: orphan {orphan} 件、FK 制約をスキップ")
        continue
    try:
        spark.sql(f"""
            ALTER TABLE {tbl}
            ADD CONSTRAINT {fk_name}
            FOREIGN KEY ({col}) REFERENCES {ref_tbl}({ref_col})
        """)
        print(f"  ✅ {tbl}.{col} → {ref_tbl}.{ref_col}")
    except Exception as e:
        fk_failures.append(f"{tbl} FK {fk_name}: {e}")
        print(f"  ❌ {tbl} FK {fk_name}: {e}")

# 制約適用失敗のサマリ（最後の Step 5 で assert する）
constraint_failures = pk_failures + fk_failures
if constraint_failures:
    print(f"\n⚠ 制約適用失敗 {len(constraint_failures)} 件:")
    for msg in constraint_failures:
        print(f"   - {msg}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 3. PII / 多段位置情報マスキング関数を定義</h2>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>📐 設計方針</strong>（README セクション 10 参照）<br>
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><code>sl_property_geo_enriched</code> を <b>業務クエリの一次参照先</b>とする</li>
# MAGIC   <li>同テーブルに事前計算列（<code>analyst_mask_lat/lng</code> / <code>viewer_mask_lat/lng</code>）を持ち、マスク UDF は<b>列選択のみ</b>（JOIN レス）</li>
# MAGIC   <li><code>sl_properties.lat/lng</code>（素データ）には <code>mask_geo_admin_only</code> を適用（admin 以外 NULL）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,mask_name（氏名、admin 以外は伏字）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_name(name STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}') THEN name
    WHEN name IS NULL THEN NULL
    ELSE CONCAT(SUBSTR(name, 1, 1), '***')
  END
""")
print("✅ mask_name")

# COMMAND ----------

# DBTITLE 1,mask_phone（電話、末尾 4 桁以外を伏字）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}') THEN phone
    WHEN phone IS NULL THEN NULL
    ELSE CONCAT('***-****-', SUBSTR(phone, -4))
  END
""")
print("✅ mask_phone")

# COMMAND ----------

# DBTITLE 1,mask_address_detail（番地以下を伏字、市区町村・町丁目までは見せる）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_address_detail(address STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}') THEN address
    WHEN address IS NULL THEN NULL
    -- N丁目までを残して以降を伏字
    ELSE REGEXP_REPLACE(address, '([0-9０-９]+丁目).*$', '$1***')
  END
""")
print("✅ mask_address_detail")

# COMMAND ----------

# DBTITLE 1,mask_geo_admin_only（緯度経度、admin のみ exact、それ以外 NULL）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_geo_admin_only(coord DOUBLE)
RETURNS DOUBLE
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}') THEN coord
    ELSE NULL
  END
""")
print("✅ mask_geo_admin_only")

# COMMAND ----------

# DBTITLE 1,mask_geo_lat / mask_geo_lng（権限 3 段階の列選択、enriched テーブル用）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_geo_lat(exact_lat DOUBLE, analyst_lat DOUBLE, viewer_lat DOUBLE)
RETURNS DOUBLE
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}')   THEN exact_lat
    WHEN is_account_group_member('{GROUP_ANALYST}') THEN analyst_lat
    WHEN is_account_group_member('{GROUP_VIEWER}')  THEN viewer_lat
    ELSE NULL
  END
""")
print("✅ mask_geo_lat")

spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_geo_lng(exact_lng DOUBLE, analyst_lng DOUBLE, viewer_lng DOUBLE)
RETURNS DOUBLE
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}')   THEN exact_lng
    WHEN is_account_group_member('{GROUP_ANALYST}') THEN analyst_lng
    WHEN is_account_group_member('{GROUP_VIEWER}')  THEN viewer_lng
    ELSE NULL
  END
""")
print("✅ mask_geo_lng")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 4. 列マスクを各テーブルに適用</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,sl_customers の PII マスキング
spark.sql("ALTER TABLE sl_customers ALTER COLUMN name SET MASK mask_name")
spark.sql("ALTER TABLE sl_customers ALTER COLUMN phone SET MASK mask_phone")
print("✅ sl_customers.name / phone")

# COMMAND ----------

# DBTITLE 1,sl_properties の住所・座標マスキング（admin のみ exact、それ以外 NULL）
spark.sql("ALTER TABLE sl_properties ALTER COLUMN address SET MASK mask_address_detail")
spark.sql("ALTER TABLE sl_properties ALTER COLUMN lat SET MASK mask_geo_admin_only")
spark.sql("ALTER TABLE sl_properties ALTER COLUMN lng SET MASK mask_geo_admin_only")
print("✅ sl_properties.address / lat / lng")

# COMMAND ----------

# DBTITLE 1,sl_property_geo_enriched の緯度経度に 3 段階マスク（事前計算列を引数渡し）
spark.sql("""
    ALTER TABLE sl_property_geo_enriched ALTER COLUMN lat
    SET MASK mask_geo_lat USING COLUMNS (analyst_mask_lat, viewer_mask_lat)
""")
spark.sql("""
    ALTER TABLE sl_property_geo_enriched ALTER COLUMN lng
    SET MASK mask_geo_lng USING COLUMNS (analyst_mask_lng, viewer_mask_lng)
""")
print("✅ sl_property_geo_enriched.lat / lng（3 段階マスク）")

# 補助列（analyst_mask_lat/lng, viewer_mask_lat/lng）にも権限別マスクを適用
# viewer は viewer_mask_* のみ参照可、analyst は analyst_mask_* + viewer_mask_* を参照可
# 補助列を直接 SELECT して viewer が高精度な analyst_mask_* を取得できないように

# mask_analyst_helper: admin / analyst のみ exact、viewer は NULL（高精度の漏洩防止）
spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_analyst_helper(coord DOUBLE)
RETURNS DOUBLE
RETURN
  CASE
    WHEN is_account_group_member('{GROUP_ADMIN}')   THEN coord
    WHEN is_account_group_member('{GROUP_ANALYST}') THEN coord
    ELSE NULL
  END
""")

spark.sql("ALTER TABLE sl_property_geo_enriched ALTER COLUMN analyst_mask_lat SET MASK mask_analyst_helper")
spark.sql("ALTER TABLE sl_property_geo_enriched ALTER COLUMN analyst_mask_lng SET MASK mask_analyst_helper")
print("✅ sl_property_geo_enriched.analyst_mask_lat / lng（admin/analyst のみ exact、viewer NULL）")
# viewer_mask_lat/lng は市区町村レベルなので、viewer 含む全グループに公開可（明示的マスクなし）

# H3 r9 セル ID も直接公開すると緯度経度を逆引きできるため、analyst 以上に制限
# （sl_property_geo_enriched に H3 r9 列がある場合のみ、ない場合はスキップ）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">Step 5. 結果確認</h2>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,information_schema から PK / FK 一覧を確認
display(spark.sql(f"""
    SELECT
      table_name,
      constraint_name,
      constraint_type
    FROM {MY_CATALOG}.information_schema.table_constraints
    WHERE table_schema = '{MY_SCHEMA}'
      AND constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
    ORDER BY table_name, constraint_type DESC, constraint_name
"""))

# COMMAND ----------

# DBTITLE 1,コメント設定済みテーブル数を確認
display(spark.sql(f"""
    SELECT
      table_name,
      LEFT(comment, 80) AS comment_excerpt
    FROM {MY_CATALOG}.information_schema.tables
    WHERE table_schema = '{MY_SCHEMA}'
    ORDER BY table_name
"""))

# COMMAND ----------

# DBTITLE 1,マスク適用列の確認
display(spark.sql(f"""
    SELECT
      table_name,
      column_name,
      mask_name
    FROM {MY_CATALOG}.information_schema.column_masks
    WHERE schema_name = '{MY_SCHEMA}'
      AND catalog_name = '{MY_CATALOG}'
    ORDER BY table_name, column_name
"""))

# COMMAND ----------

# DBTITLE 1,制約適用失敗チェック（失敗があれば NB を fail させる）
if constraint_failures:
    raise RuntimeError(
        f"UC 制約適用に {len(constraint_failures)} 件の失敗があります:\n"
        + "\n".join(f"  - {m}" for m in constraint_failures)
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ UC ガバナンス完了</strong><br>
# MAGIC コメント / PK・FK / 多段位置情報マスキングが適用されました。<br>
# MAGIC 次は <code>07_UC_Metrics_Views.py</code> で Metric Views（不動産 KPI セマンティクス層）を定義します。
# MAGIC </div>
