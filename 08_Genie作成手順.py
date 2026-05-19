# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 08 | Genie Space 作成手順
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜Genie Space を作成し、Metric Views + Vector Search Index を Source に接続</p>
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
# MAGIC <ul style="margin-top: 8px;">
# MAGIC   <li>不動産仲介 E2E 分析用の Genie Space を作成</li>
# MAGIC   <li>Source として Metric Views 4 件 + Silver 主要テーブルを接続</li>
# MAGIC   <li>General Instructions（業務知識）を登録</li>
# MAGIC   <li>サンプル質問（通常 + エージェントモード）を登録</li>
# MAGIC   <li>Vector Search Index <code>idx_property_docs</code> を作成（RAG エージェント用、NB 09 のマルチエージェントから参照）</li>
# MAGIC </ul>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1. Vector Search Endpoint + Index を作成（idx_property_docs）
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 1. Vector Search Endpoint + Index 作成</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 1-A.</b> 左サイドバー <b>Compute</b> → <b>「Vector Search Endpoints」</b> タブ<br>
# MAGIC <b>Step 1-B.</b> <b>「Create」</b> → エンドポイント名 <code>real_estate_vs_endpoint</code> → Type: <code>STANDARD</code> → Create<br>
# MAGIC <b>Step 1-C.</b> エンドポイントが <b>READY</b> になるまで待機（数分）<br>
# MAGIC <b>Step 1-D.</b> 左サイドバー <b>Catalog</b> → <code>komae_demo_v4.real_estate_e2e_demo.sl_doc_chunks</code> を選択<br>
# MAGIC <b>Step 1-E.</b> 右上の <b>「Create」</b> → <b>「Vector Search Index」</b><br>
# MAGIC <b>Step 1-F.</b> 以下を入力：<br>
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><b>Name</b>: <code>idx_property_docs</code></li>
# MAGIC   <li><b>Primary key</b>: <code>chunk_id</code></li>
# MAGIC   <li><b>Embedding source column</b>: <code>chunk_to_embed</code></li>
# MAGIC   <li><b>Embedding model</b>: <code>databricks-gte-large-ja</code>（日本語対応の埋め込みモデル）</li>
# MAGIC   <li><b>Sync mode</b>: <code>Triggered</code></li>
# MAGIC   <li><b>Endpoint</b>: <code>real_estate_vs_endpoint</code></li>
# MAGIC </ul>
# MAGIC <b>Step 1-G.</b> Create → インデックス作成完了まで待機（5〜10 分）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2. Genie Space を作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 2. Genie Space 作成</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 2-A.</b> 左サイドバー <b>Genie</b> → 右上の <b>「New」ボタン</b><br>
# MAGIC <b>Step 2-B.</b> Space 名: <code>不動産仲介 E2E 分析</code><br>
# MAGIC <b>Step 2-C.</b> Source（テーブル / Metric View）を選択：
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li><code>metric_sales_summary</code></li>
# MAGIC   <li><code>metric_property</code></li>
# MAGIC   <li><code>metric_customer</code></li>
# MAGIC   <li><code>metric_funnel</code></li>
# MAGIC   <li><code>sl_properties</code>（マスク済み）</li>
# MAGIC   <li><code>sl_property_geo_enriched</code></li>
# MAGIC   <li><code>sl_contracts</code></li>
# MAGIC   <li><code>sl_inquiries_enriched</code></li>
# MAGIC   <li><code>sl_customers</code>（マスク済み）</li>
# MAGIC   <li><code>gd_property_hazard_summary</code></li>
# MAGIC </ul>
# MAGIC <b>Step 2-D.</b> SQL Warehouse を選択（サーバーレス推奨）<br>
# MAGIC <b>Step 2-E.</b> Create
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3. General Instructions を登録
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 3. General Instructions（業務知識）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC Genie Space の <b>「Instructions」タブ</b> に以下を貼り付け。Genie のクエリ精度が大きく向上します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC ## ビジネスコンテキスト
# MAGIC これは首都圏（東京・神奈川・千葉・埼玉）+ 大阪・福岡・愛知の 7 都府県で住宅売買仲介を行う事業の分析空間です。
# MAGIC データは過去 24 ヶ月分の履歴を含みます。
# MAGIC
# MAGIC ## 主要テーブル
# MAGIC - sl_properties: 物件マスタ（3,000 件）。lat/lng は admin 権限のみ参照可
# MAGIC - sl_property_geo_enriched: 物件 × ハザード / 用途地域 / 駅 / 地価 を Geo JOIN 済み。業務クエリの一次参照先
# MAGIC - sl_contracts: 成約履歴（5,000 件、過去 24 ヶ月）
# MAGIC - sl_inquiries: 内見・問合せ（12,500 件）。funnel_stage は反響/内見/申込/成約/失注
# MAGIC - gd_property_hazard_summary: エリア×ハザード別の値引き率・売出期間
# MAGIC - metric_sales_summary / metric_property / metric_customer / metric_funnel: KPI 統一定義
# MAGIC
# MAGIC ## ハザード区分の意味
# MAGIC - レッドゾーン: 土砂災害特別警戒区域（最も危険）
# MAGIC - イエローゾーン: 土砂災害警戒区域
# MAGIC - 浸水想定区域: KSJ A31（想定最大規模の浸水想定）
# MAGIC - ハザードなし: 上記いずれにも該当しない
# MAGIC
# MAGIC ## 駅徒歩区分
# MAGIC - 5 分以内 / 10 分以内 / 15 分以内 / 15 分超
# MAGIC
# MAGIC ## エリアコード（area_code）
# MAGIC - 11: 埼玉県、12: 千葉県、13: 東京都、14: 神奈川県、23: 愛知県、27: 大阪府、40: 福岡県
# MAGIC
# MAGIC ## RM セグメント
# MAGIC - 優良顧客: 最近接触 + 高額
# MAGIC - 新規育成候補: 最近接触 + 低額
# MAGIC - 高額離反候補: 古い接触 + 高額
# MAGIC - 休眠顧客: 古い接触 + 低額
# MAGIC - 未接触顧客: last_contact_date が NULL
# MAGIC
# MAGIC ## クエリ時の注意
# MAGIC - 月次集計は contract_month / inquiry_month を使う（DATE_TRUNC 済みの DATE 型）
# MAGIC - 値引き率を求めるときは discount_amount / listing_price
# MAGIC - 仲介手数料率は commission / settled_price
# MAGIC - 地価との乖離は sl_property_geo_enriched.price_vs_landprice_ratio を使う
# MAGIC - メジャーは MEASURE(metric_name) で呼ぶ
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4. サンプル質問を登録
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 4. サンプル質問（通常 + エージェントモード）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>通常質問（README セクション「Genie Space サンプル質問」参照）</b>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC - 先月、最も成約数が多かった営業所は？
# MAGIC - マンションの平均売出期間は？
# MAGIC - 30 代ファミリー層に売れている物件種別 TOP 3 は？
# MAGIC - 浸水想定 1m 以上の物件と非該当物件で、平均値引き率を比較して
# MAGIC - 土砂災害特別警戒区域（レッドゾーン）の物件を営業所別に集計して、売出期間が長い TOP 5 を見せて
# MAGIC - 駅徒歩 5 分以内 vs 15 分超 で、成約価格の差を市区町村別に比較して
# MAGIC - 地価公示（L01）との乖離率が大きい物件 TOP 10 を見せて
# MAGIC - 1km メッシュの将来推計人口と直近 1 年の成約数の相関を見せて
# MAGIC - funnel_stage 別の月次転換率を営業所別に比較して

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>エージェントモード質問</b>（Playground マルチエージェントで RAG + Genie 連携）
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC - 物件 P-12345 の重要事項説明書に書かれているハザード情報と、現状の浸水想定区域の関係は？（RAG + Genie 両参照）
# MAGIC - 江東区で築 10 年以内マンションの典型的な特約事項と、直近の成約事例の値引き傾向を教えて（RAG → Genie）
# MAGIC - 先月最も成約した物件種別について、パンフ記載の省エネ等級 / 耐震等級の標準仕様を教えて（Genie → RAG）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 5. マルチエージェント（Playground）を作成
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">Step 5. マルチエージェント（Playground）</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <b>Step 5-A.</b> 左サイドバー <b>Playground</b> → 右上の <b>「Multi-agent」</b><br>
# MAGIC <b>Step 5-B.</b> エージェント 1：<b>RAG エージェント</b><br>
# MAGIC <ul style="margin-top: 4px;">
# MAGIC   <li>Tool: Vector Search Index <code>idx_property_docs</code></li>
# MAGIC   <li>説明：重要事項説明書 + 物件パンフから関連情報を検索</li>
# MAGIC </ul>
# MAGIC <b>Step 5-C.</b> エージェント 2：<b>Genie エージェント</b><br>
# MAGIC <ul style="margin-top: 4px;">
# MAGIC   <li>Tool: Genie Space <code>不動産仲介 E2E 分析</code></li>
# MAGIC   <li>説明：成約 / 物件 / 顧客 / ファネル / Geo データを自然言語クエリ</li>
# MAGIC </ul>
# MAGIC <b>Step 5-D.</b> オーケストレーション LLM：<code>databricks-claude-opus-4-7</code><br>
# MAGIC <b>Step 5-E.</b> Step 4 のエージェントモード質問でテスト
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ Genie Space + マルチエージェント 作成完了</strong><br>
# MAGIC 次は <code>09_ダッシュボード作成手順.py</code> で AI/BI Dashboard を Genie Code（自然言語プロンプト）で自動生成し、H3 地図ヒートマップを追加します。
# MAGIC </div>
