# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 11 | Genie Code インタラクティブ分析
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜経営判断シナリオを Genie で深掘り</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 15 min</span>
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
# MAGIC 経営判断シナリオを 3 本立てで Genie Space に問いかけ、ドリルダウン → 仮説検証 → アクション提言の流れを体験します。<br>
# MAGIC 各シナリオで使う Genie プロンプトを以下に列挙。実際の問い合わせは Genie Space <code>不動産仲介 E2E 分析</code> から行います。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## シナリオ 1：ハザード起因の値引き分析
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">シナリオ 1：浸水想定が値引きにどう影響するか</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 想定ペルソナ：査定 / 仕入担当
# MAGIC
# MAGIC **背景**：浸水想定区域に該当する物件の査定価格をどの程度下げるべきか、データドリブンに判断したい。
# MAGIC
# MAGIC ### Genie プロンプト（順次入力）
# MAGIC
# MAGIC 1. **全体把握**
# MAGIC    ```
# MAGIC    ハザード区分（レッドゾーン / イエローゾーン / 浸水想定区域 / ハザードなし）別の物件数と平均値引き率を比較して
# MAGIC    ```
# MAGIC
# MAGIC 2. **エリア別ドリルダウン**
# MAGIC    ```
# MAGIC    浸水想定区域に該当する物件の値引き率を、都道府県（area_code）別に比較。値引き率が高い順に TOP 7 都府県を表示
# MAGIC    ```
# MAGIC
# MAGIC 3. **時系列の傾向**
# MAGIC    ```
# MAGIC    過去 24 ヶ月の月次で、ハザードなし vs 浸水想定区域 の平均値引き率の推移を折れ線グラフで
# MAGIC    ```
# MAGIC
# MAGIC 4. **仮説検証：築年との交絡**
# MAGIC    ```
# MAGIC    築年区分（10年以内 / 10-20年 / 20年超）× ハザード区分 のクロス集計で平均値引き率を見せて
# MAGIC    ```
# MAGIC
# MAGIC 5. **アクション提言**
# MAGIC    ```
# MAGIC    浸水想定区域で築 20 年超の物件の典型的な値引き率と売出期間を踏まえ、査定価格を listing_price からどの程度下げるべきか提案して
# MAGIC    ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## シナリオ 2：駅徒歩 × 価格弾力性
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">シナリオ 2：駅徒歩分が成約価格にどう効くか</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 想定ペルソナ：エリアマネージャー
# MAGIC
# MAGIC **背景**：駅徒歩 5 分以内のプレミアム物件と、15 分超の郊外物件で価格弾力性がどう異なるか把握したい。
# MAGIC
# MAGIC ### Genie プロンプト
# MAGIC
# MAGIC 1. **全体傾向**
# MAGIC    ```
# MAGIC    駅徒歩区分（5分以内 / 10分以内 / 15分以内 / 15分超）別の物件種別ごとの平均売出価格を比較
# MAGIC    ```
# MAGIC
# MAGIC 2. **物件種別の差**
# MAGIC    ```
# MAGIC    マンション と 戸建 で、駅徒歩分による平均成約価格の差をエリア別に比較。一番駅近プレミアムが効くエリア TOP 5 は？
# MAGIC    ```
# MAGIC
# MAGIC 3. **売出期間との関係**
# MAGIC    ```
# MAGIC    駅徒歩区分別の平均売出期間（avg_days_on_market）を比較。15 分超の物件は売出から何日で成約しているか
# MAGIC    ```
# MAGIC
# MAGIC 4. **エリア × 駅徒歩のヒートマップ**
# MAGIC    ```
# MAGIC    都道府県別 × 駅徒歩区分 のセル別平均成約価格をヒートマップで
# MAGIC    ```
# MAGIC
# MAGIC 5. **アクション提言**
# MAGIC    ```
# MAGIC    駅徒歩 15 分超の物件で売出期間が 90 日超のものを抽出。値引き提案でクロージング率を上げる戦術を提案して
# MAGIC    ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## シナリオ 3：営業所配置最適化
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">シナリオ 3：商圏人口と成約数の相関で出店戦略を考える</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 想定ペルソナ：経営企画 / 役員
# MAGIC
# MAGIC **背景**：30 営業所の生産性に差がある。商圏人口の縮小エリアから撤退すべきか、人口集中エリアに新規出店すべきか。
# MAGIC
# MAGIC ### Genie プロンプト
# MAGIC
# MAGIC 1. **営業所別の成約数 + 商圏人口**
# MAGIC    ```
# MAGIC    各営業所の直近 6 ヶ月の成約数と、営業所近隣 1km の将来推計人口（mesh_pop_total）の関係を散布図で
# MAGIC    ```
# MAGIC
# MAGIC 2. **業績悪化営業所の特定**
# MAGIC    ```
# MAGIC    成約数が前年同期比で 30% 以上減少した営業所を特定。各営業所の所在地（市区町村）と商圏人口減少率を併せて表示
# MAGIC    ```
# MAGIC
# MAGIC 3. **ファネル転換率の差**
# MAGIC    ```
# MAGIC    業績悪化営業所と業績好調営業所で、内見→申込 / 申込→成約 の転換率にどの程度差があるか比較
# MAGIC    ```
# MAGIC
# MAGIC 4. **撤退候補 vs 新規出店候補**
# MAGIC    ```
# MAGIC    商圏人口が縮小している市区町村にある営業所と、商圏人口が増加していて営業所が無い市区町村を一覧で
# MAGIC    ```
# MAGIC
# MAGIC 5. **アクション提言**
# MAGIC    ```
# MAGIC    上記を踏まえ、撤退候補営業所 2 件と新規出店候補エリア 3 件を提案。投資対効果も含めて
# MAGIC    ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## マルチエージェント連携シナリオ（RAG + Genie）
# MAGIC <div style="background: #B45309; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h3 style="margin: 0; color: #FFFFFF;">RAG（重説 / パンフ）+ Genie（成約 / Geo）の連携</h3>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 営業現場シナリオ：物件提案の現場準備
# MAGIC
# MAGIC 1. ```
# MAGIC    物件 P-12345 の重要事項説明書に書かれているハザード情報と、現状の浸水想定区域の関係は？
# MAGIC    ```
# MAGIC    （RAG エージェントが重説検索 + Genie エージェントが浸水想定区域データ参照）
# MAGIC
# MAGIC 2. ```
# MAGIC    江東区で築 10 年以内マンションの典型的な特約事項と、直近の成約事例の値引き傾向を教えて
# MAGIC    ```
# MAGIC    （RAG が重説の特約事項を取得 → Genie が成約データを集計）
# MAGIC
# MAGIC 3. ```
# MAGIC    先月最も成約した物件種別について、パンフ記載の省エネ等級 / 耐震等級の標準仕様を教えて
# MAGIC    ```
# MAGIC    （Genie が成約 TOP の物件種別を特定 → RAG がパンフから仕様を抽出）

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ E2E デモ完了</strong><br>
# MAGIC データ取り込み → 加工（SDP + 地理空間）→ ガバナンス（UC + マスキング）→ 分析（Metric Views + Dashboard + Genie）→ AI 活用（RAG + マルチエージェント）の全フローを体験しました。<br>
# MAGIC <br>
# MAGIC <strong>📊 達成したアウトプット</strong>：
# MAGIC <ul style="margin-top: 6px;">
# MAGIC   <li>50 オブジェクトの Lakehouse（Bronze 17 / Silver 19 / Gold 6 / MV 4 / Metric 4 / VS Index 1）</li>
# MAGIC   <li>地理空間処理（H3 + st_* + Shapefile/GML）の主役級デモ</li>
# MAGIC   <li>多段位置情報マスキング（admin / analyst / viewer）の UC ガバナンス</li>
# MAGIC   <li>重説 + パンフ PDF の RAG + Genie マルチエージェント</li>
# MAGIC   <li>H3 ヒートマップ + ファネル + ハザード値引きの AI/BI Dashboard</li>
# MAGIC </ul>
# MAGIC </div>
