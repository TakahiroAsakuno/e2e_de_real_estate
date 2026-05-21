# 不動産仲介 Databricks E2E デモ

## 概要

首都圏（東京・神奈川・千葉・埼玉）＋大阪・福岡・愛知の住宅売買仲介を題材に、Databricks Data Intelligence Platform 上で **データ取り込み → 加工 → ガバナンス → 分析 → AI 活用** までを一気通貫で体験するデモ教材。
構造化データのパイプライン（SDP + Autoloader + Expectations）に加え、**地理空間処理（H3 / `st_*` / Shapefile / GML 取り込み）を主役級に押し出し**、国交省の不動産取引価格情報（reinfolib API）・国土数値情報（KSJ）・位置参照情報（ISJ）・OpenStreetMap を組み合わせて、**ハザード・用途地域・地価・駅距離が物件価値にどう影響するかを実データで分析する**構成です。
Unity Catalog によるガバナンス（PII + 多段位置情報マスキング）、UC Metrics Views によるセマンティクス層、重要事項説明書 / 物件パンフを題材とした RAG（`ai_parse_document` + `ai_prep_search` + Vector Search）、AI/BI Dashboard・Genie・Databricks One によるアウトプット体験までを通して、プラットフォームの機能を確認できる構成です。

> 注記：本デモは「傾向分析」を目的としており、サンプルデータ規模（物件 3,000 件 / 7 都府県）に起因して市区町村別・ハザード別の細かい統計値はばらつきます。デモ目的の説明であり、因果推定の根拠ではありません。

## 対象 / 形式

| 項目 | 内容 |
|---|---|
| 対象 | データ基盤・分析基盤の評価担当者 |
| 形式 | デモ（手を動かすハンズオンは別企画） |
| 所要時間 | 約 2.5 時間 |
| 環境 | Databricks サーバレスワークスペース |

## 想定利用者と活用ツール

住宅売買仲介事業の各ロールが、どのツールでどんな課題を解くかを想定したシナリオ構成です。

| 想定利用者 | 主な関心事 | メインツール | サブツール |
|---|---|---|---|
| 経営企画 / 役員 | 月次成約・営業所ランキング・エリア別業績 | AI/BI Dashboard | Genie |
| エリアマネージャー / 営業所運営 | 内見ファネル・業績悪化営業所検知 | AI/BI Dashboard（営業所フィルター） | Genie |
| 査定 / 仕入担当 | **地価公示 vs 査定価格**・滞留物件・**ハザード影響値下げ余地** | Genie | AI/BI Dashboard |
| 営業（現場） | 商談中物件の重要事項説明書 RAG・ハザード説明補助・顧客提案 | RAG / マルチエージェント（Databricks One 経由） | — |
| マーケ / CRM 担当 | 顧客セグメント・休眠掘り起こし・LTV | Genie | AI/BI Dashboard |

## デモ構成（10 ステップ）

「**デモステップ番号**」と「**NB 番号**」は別物です。手順 NB がないステップ（導入・マルチエージェント・Databricks One）は対応 NB 列が空欄になります。Databricks One は UI 設定のみで専用 NB を持ちません。

| デモステップ | ステップ名 | 形式 | 紹介機能 | 時間 | 対応 NB |
|---|---|---|---|---|---|
| 1 | 導入 | スライド | プラットフォーム概要・本日の流れ | 5 分 | — |
| 2 | 構造化データパイプライン | UI + NB | SDP + Autoloader + Expectations | 25 分 | NB 02 / NB 04 |
| 3 | **地理空間データパイプライン** | NB | **KSJ ファイル（Shapefile / GML） / GML 取り込み / ポリゴン処理 / H3 化** | 20 分 | NB 03 |
| 4 | NB 補完処理（AI + Geo JOIN） | NB（SQL + Python） | AI Functions + **物件 × ハザード / 用途地域 / 地価 / 駅 の Geo JOIN** | 25 分 | NB 05 |
| 5 | オーケストレーション | UI | Jobs（SDP + 地理空間 + NB の混在 DAG） | 5 分 | NB 10 |
| 6 | UC ガバナンス | UI（一部 NB） | コメント・PK/FK・リネージ・タグ・PII マスキング・**多段位置情報マスキング** | 15 分 | NB 06 |
| 7 | UC Metrics Views | NB + UI | 不動産 KPI のセマンティクス層（YAML 定義） | 10 分 | NB 07 |
| 8 | RAG 構築 | NB + UI | **重要事項説明書 + 物件パンフ** PDF を `ai_parse_document` + `ai_prep_search` + Vector Search Index | 15 分 | NB 05（PDF 構造化） / UI |
| 9 | マルチエージェント | UI（Playground） | RAG エージェント + Genie エージェント（成約 + Geo） | 10 分 | — |
| 10 | AI/BI Dashboard + Genie + Databricks One | UI | Materialized View + **H3 地図ヒートマップ** + 業務担当者向けポータル（Databricks One は UI 設定のみ） | 20 分 | NB 08（Genie）/ NB 09（Dashboard）/ — (Databricks One) |

合計 150 分

## 技術スタック

| カテゴリ | 技術 |
|---|---|
| コンピュート | Serverless（SQL Warehouse / Notebook Serverless / SDP Serverless）<br>全 NB が Serverless で動作。NB 03 は GDAL 非依存の Pure Python スタック（**pyshp + shapely + pyproj**）で実装。 |
| 言語 | SQL, Python (PySpark) |
| 取り込み | Autoloader（`cloud_files`）+ Volume |
| パイプライン | Spark Declarative Pipelines（SDP）+ Expectations |
| 加工 | SDP（宣言的）+ Notebook（SQL + Python 混在） |
| テーブルフォーマット | Delta Lake |
| **地理空間処理** | **H3 関数（`h3_pointash3` / `h3_polyfillash3` / `h3_kring` / `h3_centeraswkb`）/ `st_*` 関数（`st_contains` / `st_intersects` / `st_distancesphere` / `st_pointonsurface` / `st_envelope`）/ Shapefile / GML / GeoJSON 取り込み** |
| ガバナンス | Unity Catalog（カタログ / スキーマ / Volume / コメント / PK・FK / リネージ / インサイト / タグ / カラムマスキング・行フィルタ） |
| セマンティクス | UC Metrics Views（YAML 定義） |
| 高速化 | Materialized View + 差分更新 |
| AI Functions | `ai_classify`, `ai_query`, `ai_parse_document`, `ai_prep_search` |
| RAG | Vector Search Index |
| エージェント | Playground マルチエージェント |
| 可視化 | AI/BI Dashboard, Genie Space, Genie Code |
| エンドユーザー | Databricks One |
| ワークフロー | Jobs（SDP + 地理空間 NB + AI NB 混在 DAG） |

## メダリオンアーキテクチャ

```
[Volume: raw_data/transactions]
       │  reinfolib 取引価格情報（実データ）+ 合成 内見・成約 CSV
       ▼
[Autoloader (cloud_files)]
       │  新着ファイルのみ増分取り込み
       ▼
[Bronze 構造化: bz_*]   ← Raw 形式保持・監査列付与
       │  SDP + Expectations（型不正 / NULL / 重複の品質チェック）
       ▼
[Silver 構造化: sl_*]   ← クレンジング・JOIN・正規化済み
       │
       │       ┌──────────────────────────────────────┐
       │       │ [Volume: raw_data/geo]                │
       │       │   KSJ ファイル（Shapefile / GML） / OSM GeoJSON         │
       │       ▼                                       │
       │  [Bronze 地理空間: bz_geo_*]                  │
       │       │ ジオメトリ展開・座標変換（→EPSG:4326）│
       │       ▼                                       │
       │  [Silver 地理空間: sl_geo_*]                  │
       │     （H3 r9 候補抽出列 + ジオメトリ保持）     │
       │       │                                       │
       └───────┴──► [Geo JOIN：H3 r9 + h3_kring(k=1) │
                    で候補拡張 → bbox プレフィルタ →  │
                    `st_contains` / `st_intersects`   │
                    で最終判定]                       │
                                                      │
                          │                           │
                          ▼                           │
[Silver enrich: sl_*_enriched] ← AI Functions / Geo JOIN
[Gold: gd_*]     ← ビジネスマート（NB 02 で 3 件 + NB 05 で 3 件）
       │                                              │
       ├─►[MV: mv_*]            ← Dashboard 専用 Materialized View
       │       mv_dashboard_kpi（経営 KPI）
       │       mv_h3_price_heatmap（H3 r8 価格ヒートマップ）
       │       mv_hazard_discount（ハザード別値引き率）
       │       mv_sales_funnel（内見ファネル）
       │
       └─►[Metrics Views: metric_*]  ← Genie / Dashboard / SQL から共通呼出


[Volume: raw_data/pdf]
       │  重要事項説明書 / 物件パンフ PDF
       ▼
[ai_parse_document（PDF 構造化）]
       ▼
[Bronze: bz_doc_parsed]
       │  ai_prep_search（チャンク + メタデータ自動生成）
       ▼
[Silver: sl_doc_chunks]
       │  Vector Search Index
       ▼
[VS Index: idx_property_docs]
       │
       └─►[Playground マルチエージェント]
            ├─ RAG エージェント（重説・パンフ参照）
            └─ Genie エージェント（成約・在庫 + Geo 参照）
```

## サンプルデータ

### 構造化データ（不動産仲介）

| データ | 件数 | 主要カラム |
|---|---|---|
| 営業所マスタ（offices） | 30 | 営業所 ID, 営業所名, 業態（戸建仲介 / マンション仲介 / 投資物件専門）, 都道府県, 市区町村, 緯度経度 |
| 物件マスタ（properties） | 3,000 | 物件 ID, 物件種別（戸建 / マンション）, 都道府県, 市区町村, 町丁目, **緯度経度**, 築年, 専有面積, 土地面積, 間取り, 最寄駅, 駅徒歩分, **省エネ等級**, **断熱等性能等級**, **耐震等級**, 売出価格, 査定価格, 入庫日, **status（現況：売出中 / 商談中 / 成約 / 取下げ）** |
| 顧客マスタ（customers） | 2,500 | 顧客 ID, 氏名, 年齢, 性別, 電話番号, 居住エリア, 登録営業所, 初回接点日, 年収レンジ, 家族構成, ライフステージ, 希望物件種別, 予算上限 |
| 内見・問合せ履歴（inquiries） | 12,500 | 商談 ID, 顧客, 営業所, 物件, 商談日, 来店区分（来店 / オンライン / 内見 / 電話）, **funnel_stage（反響 / 内見 / 申込 / 成約 / 失注）**, **status（商談状態：オープン / クローズ）**, 商談メモ |
| 成約（contracts） | 5,000 | 契約 ID, 商談, 顧客, 営業所, 物件, 契約日, 成約価格, 売出価格, 値引額, 仲介手数料, 決済方法 |
| 不動産市況指標（market_index） | 〜数千 | 月, エリア（都道府県・市区町村）, 物件種別, **取引価格指数**, **地価指数（L01/L02 由来）**, **住宅ローン金利（35年固定/変動）**, **建築費指数** |

PII マスキング対象：氏名・電話番号・**住所（番地以下伏字）**・**詳細緯度経度（権限別の多段マスキング）**

#### 時系列範囲とテーブルの関係
- `properties.status` は **現況マスタ**（現時点の在庫ステータス）。3,000 件 = 現況の管理物件数
- `contracts` は **過去 24 ヶ月分の成約履歴**（5,000 件）。物件は売出 → 成約 → 新規売出 を繰り返すため、`properties` の何倍にもなり得る
- `inquiries` は **過去 24 ヶ月分の商談履歴**（12,500 件）。`funnel_stage` で「反響→内見→申込→成約→失注」のファネル分析が可能

#### データ生成方針
`properties` の住所・面積・築年・売出価格の一部は **reinfolib（不動産情報ライブラリ）API の実取引データを種に**して生成します。`market_index` も同 API + KSJ L01/L02 から取得。
Autoloader デモ用にトランザクション系（inquiries / contracts）は **5 日分の日次 CSV に分割** して Volume に配置し、新着ファイルが増分取り込みされる挙動を確認できるようにしています。

`customers` の年収レンジ・家族構成・ライフステージ・希望物件種別・予算上限の一部は、`inquiries.商談メモ` から `ai_query` で抽出する構成にしており、AI Functions を用いたプロファイル enrich の例として確認できます。

### 地理空間データ（このデモの主役）

| データ | 出典 | 形式 | 主用途 |
|---|---|---|---|
| 用途地域 | KSJ A29 | Shapefile / GML（ポリゴン） | `st_contains` で物件 × 用途地域 JOIN |
| **洪水浸水想定区域（想定最大規模）** | KSJ A31 | Shapefile / GML（ポリゴン + 浸水深ランク属性） | **物件 × 浸水深ランク JOIN → 値引き率分析**。本デモは「想定最大規模の浸水深」を主指標、計画規模・浸水継続時間は補助参考 |
| 土砂災害警戒区域 | KSJ A33 | Shapefile / GML（ポリゴン） | 物件 × 警戒区域フラグ（特別警戒区域＝レッドゾーンも区別） |
| **地価公示** | KSJ L01 | Shapefile / GML（ポイント + 年次価格） | **査定価格 vs 地価公示の乖離分析**、エリア地価指数の基礎 |
| **都道府県地価調査** | KSJ L02 | Shapefile / GML（ポイント + 基準地価格） | L01 の補完（基準地）。L01/L02 を統合した地価インデックスを構築 |
| 鉄道駅 | KSJ N02 | Shapefile / GML（ポイント + 路線属性） | `st_distancesphere` で最寄駅・徒歩分計算 |
| 行政区域 | KSJ N03 | Shapefile / GML（ポリゴン） | 市区町村別集計（`st_contains` で精密判定）。**`representative_lat / representative_lng` を `st_pointonsurface()` で算出**（必ずポリゴン内に落ちる代表点。`st_centroid()` だと凹形・島しょ・マルチポリゴンでポリゴン外に出る可能性があるため不採用）。viewer 権限の位置マスキング代表点に使用 |
| 1km メッシュ別将来推計人口 | KSJ mesh1000 | Shapefile / GML（メッシュポリゴン + 推計属性） | 商圏人口需要分析（H3 r8 で集計） |
| 大字・町丁目位置参照情報 | ISJ | CSV | 住所文字列 → 代表緯度経度のジオコーディング前処理 |
| OpenStreetMap POI | OSM Overpass JSON | GeoJSON | 周辺施設（コンビニ・スーパー・学校）。KSJ との差分検出デモにも使用 |

### Geo JOIN の標準フロー（H3 候補漏れ対策）

`h3_polyfillash3` は **セル重心包含** ベースのため、細いポリゴン（河川沿いの浸水想定や急傾斜地）はセル展開で空配列になる可能性があります。物件側を `h3_kring` で拡張するだけでは、ポリゴン側が 0 セルなら復元できません。本デモでは以下の **標準フロー（4 段 + fallback）** を **すべての Geo JOIN の標準** とします：

```
[標準フロー]
  1. ポリゴン側を h3_polyfillash3 で r9 セル展開（候補抽出列）
  2. 物件側の h3_r9 に対して h3_kring(k=1) で隣接セルまで候補拡張
  3. bbox プレフィルタ（st_envelope）で粗結合
  4. 最終判定は st_contains / st_intersects（ポリゴン照合）

[fallback：ポリゴン側のセル展開が 0〜数件と異常に少ない場合]
  - 該当ポリゴンに対しては bbox-only fallback（st_envelope のみで粗結合）→ st_intersects
  - polyfill 件数を NB 03 で記録し、閾値以下のレコードに fallback フラグを立てる
```

地価公示の最近傍検索は **adaptive k-ring** を使います：

```
[地価最近傍検索]
  1. 物件の h3_r9 から開始
  2. k=0（同一セル）で候補が 0 件なら k=1 に拡張、k=2 に拡張... と段階的に k を増やす
  3. 候補が 1〜3 点取れた時点で打ち切り、距離順 ORDER BY で最近傍を確定
  4. k の上限は 10（H3 r9 で約 2km 圏目安）。**探索上限を約 2km に統一**（デモでは 2km 圏で十分。より広域要件が必要なら r7 / r8 の H3 列を別途付与する）
  5. 上限到達 / 約 2km 圏内に候補なしの場合は NULL 扱い + NULL_REASON 列で理由を記録（"no_landprice_within_2km" 等）
```

> H3 v4.x 公式の平均辺長は r9 約 201m / 面積 約 0.1 km²、k=N リングが概ね N × 200m 圏に対応します。広域要件（5km 等）が必要な場合は、地価テーブルに r7（約 1.4km 辺）/ r8（約 531m 辺）の H3 列を別途付与し、より粗い解像度で adaptive k-ring を回してください。

### 非構造化データ（RAG 用）

| データ | 件数 | 内容 |
|---|---|---|
| 重要事項説明書 PDF | 5 | 物件種別ごとの架空サンプル（重要事項・取引条件・心理的瑕疵・ハザード記載含む） |
| 物件パンフレット PDF | 5 | 物件詳細・周辺施設・図面・**省エネ等級 / 断熱等性能等級 / 一次エネルギー消費量等級 / 耐震等級** 記載（架空） |
| 接客録音 MP3 | 5〜10 件 | 内見時の架空対話。Whisper デモ用 |

PDF・MP3 はすべて **架空データを Claude で生成**して同梱します（実物の重要事項説明書は個人情報を含むため使用不可）。

## カタログ・スキーマ規約

| 項目 | 値 |
|---|---|
| カタログ | `komae_demo_v4`（事前作成済み前提） |
| スキーマ | `real_estate_e2e_demo`（このデモ専用） |
| Volume | `raw_data`（CSV / Shapefile / PDF / MP3 共通配置） |
| Volume パス | `/Volumes/komae_demo_v4/real_estate_e2e_demo/raw_data` |

テーブル命名規則：

| プレフィックス | 層 | 例 |
|---|---|---|
| `bz_*` | Bronze（Raw 取り込み・構造化） | `bz_properties`, `bz_contracts` |
| `bz_geo_*` | Bronze（地理空間） | `bz_geo_zoning`, `bz_geo_flood`, `bz_geo_landprice_l01` |
| `bz_doc_*` | Bronze（PDF 構造化） | `bz_doc_parsed` |
| `sl_*` | Silver（クレンジング・JOIN） | `sl_properties`, `sl_doc_chunks` |
| `sl_geo_*` | Silver（地理空間・H3 化済み） | `sl_geo_zoning`, `sl_geo_flood`, `sl_geo_landprice` |
| `sl_*_enriched` | Silver（AI Functions / Geo JOIN で enrich 済み） | `sl_inquiries_enriched`, `sl_property_geo_enriched` |
| `gd_*` | Gold（ビジネスマート テーブル / Delta） | `gd_office_monthly_sales`, `gd_contract_discount_score` |
| `mv_*` | Materialized View（Dashboard 専用、差分更新） | `mv_dashboard_kpi`, `mv_h3_price_heatmap`, `mv_hazard_discount`, `mv_sales_funnel` |
| `metric_*` | UC Metrics View（セマンティクス層） | `metric_sales_summary`, `metric_property`, `metric_funnel` |
| `idx_*` | Vector Search Index | `idx_property_docs` |

## ファイル構成

```
sample/e2e_de_real_estate/
├── 00_config.py                              共通: 変数定義・スキーマ・Volume 作成
├── 01_データ準備.py                          事前実行: reinfolib 取得 + 合成 CSV + KSJ ファイル（Shapefile / GML）（L01/L02 含む）+ ISJ + OSM + PDF + MP3 を Volume 配置
├── 02_SDPパイプライン定義.sql                SDP コード本体: Bronze/Silver/Gold（NB 02 由来 3 件）+ Expectations
├── 03_地理空間データパイプライン.py          KSJ ファイル（Shapefile / GML） / GML 取り込み・座標変換・H3 化（地価 L01/L02 含む）・sl_geo_admin の representative_lat/lng 算出
├── 04_SDPパイプライン設定手順.py             UI 操作手順: パイプライン作成・Trigger・実行・スキーマ設定
├── 05_AIとGeoによる補完処理.py               音声 → Whisper / ai_classify / ai_query / ai_parse_document + 物件 × ハザード / 用途地域 / 地価 / 駅 の Geo JOIN + 派生 Gold（NB 05 由来 3 件）
├── 06_テーブル設定.py                        コメント / PK・FK / カラムマスキング（PII + 多段位置情報）/ タグ
├── 07_UC_Metrics_Views.py                    Metrics Views 定義（不動産 KPI セマンティクス、funnel 含む）
├── 08_Genie作成手順.py                       Genie Space 作成 + General Instructions + サンプル質問
├── 09_ダッシュボード作成手順.py              AI/BI Dashboard を Genie Code で自動生成 + H3 地図ウィジェット + Genie 連携 + MV 4 件
├── 10_Jobsワークフロー作成手順.py            Jobs（SDP + 地理空間 NB + AI NB 混在 DAG）+ スケジュール + 通知
├── 11_Genie Codeインタラクティブ分析.py      経営判断シナリオ（ハザード起因値引き分析 / 駅徒歩×価格弾力性 / 営業所配置最適化）
└── README.md
```

## データ管理方針（再現性・キャッシュ）

外部データ（reinfolib API / KSJ ZIP / ISJ ZIP / OSM Overpass）は、**ライセンス・サイズ・API キーの観点で Git 管理しない方針**です。代わりに以下の仕組みで再現性と再実行効率を担保します。

| データ | Git 管理 | 入手 | キャッシュ |
|---|---|---|---|
| **reinfolib API**（取得済み JSON） | ✅ | リポジトリ同梱（取得済み JSON）/ 更新時のみ API キー | `data/external/reinfolib/` → Volume `raw_data/reinfolib_cache/` |
| **KSJ ZIP**（A29/A31/A33/L01/L02/N02/N03/mesh1000） | × | 都度 ZIP DL | Volume `raw_data/geo/<dataset>/` |
| **ISJ ZIP** | × | 都度 ZIP DL | Volume `raw_data/geo/isj/` |
| **OSM Overpass** | × | 都度 API DL | Volume `raw_data/geo/osm/` |
| **PDF（重説 + パンフ）** | ○（通常 Git） | 自前生成 | `data/pdf/` |
| **MP3（接客録音）** | ○（通常 Git） | 自前生成 | `data/audio/` |

### キャッシュ動作

`01_データ準備.py` は以下のロジックで動作：

1. 各セクション冒頭で **Volume の対象パスに既存ファイルがあるか確認**（`should_fetch()` ヘルパー）
2. 既存ファイルがあれば **API/DL を skip**（ログ：`Volume キャッシュ済み（FORCE_REFRESH=False のため skip）`）
3. `FORCE_REFRESH = True` に変更すれば全データを強制再 DL

### ライセンス補足

- **KSJ（国土数値情報）**：[CC-BY 4.0 互換](https://nlftp.mlit.go.jp/ksj/other/agreement_01.html)。再配布は可能だが、本プロジェクトではサイズ・運用面から Git 管理しない
- **reinfolib API**：API キーがユーザー単位で発行されるため、各自で取得・キャッシュする方針
- **ISJ / OSM**：それぞれの公式規約に従う。本プロジェクトでは Git 管理しない

詳細は [data/README.md](./data/README.md) を参照。

## 実行順序

1. **00_config.py** を最初に実行（スキーマ・Volume 作成）
2. **01_データ準備.py** を実行（reinfolib 取得 / KSJ ファイル（Shapefile / GML） DL（L01/L02 含む）/ ISJ DL / OSM DL / PDF 生成 / MP3 生成 → Volume 配置）
3. **02_SDPパイプライン定義.sql** は SDP として登録するための定義ファイル。実体の実行は `04_SDPパイプライン設定手順.py` の UI 操作で行う
4. **03_地理空間データパイプライン.py** で Shapefile / GML 取り込み・座標変換・H3 化
5. **05_AIとGeoによる補完処理.py** で音声・テキストの AI enrich + 物件 × Geo JOIN
6. **06** で UC ガバナンス、**07** で Metrics Views、**08** で Genie、**09** でダッシュボード、**10** で Jobs ワークフロー、**11** で Genie Code を使った経営判断シナリオ深掘り体験
7. `08`〜`11` の UI 操作系は手順 NB に従って Databricks UI で操作する。Databricks One は UI 設定のみで専用 NB なし

## 各ステップで確認できるポイント

### 2. 構造化データパイプライン（SDP + Autoloader + Expectations）
- `cloud_files()` による新着ファイルのみの増分取り込み
- `@dlt.expect_or_drop` / `@dlt.expect_or_fail` によるデータ品質ルール
  - 例：成約価格 ≤ 売出価格 / 築年 ≥ 1950 / 緯度経度の範囲チェック / 物件 ID の重複検知 / `funnel_stage` が列挙値に含まれる
- Expectation 違反レコード件数のメトリクス自動収集
- パイプライン UI でのデータ系統（Lineage）自動描画

### 3. 地理空間データパイプライン（このデモの差別化軸）
- KSJ ファイル（Shapefile / GML） を `GeoPandas` + `applyInPandas` で分散読み込み
- 投影座標系（JGD2011 平面直角座標 / 緯度経度）から WGS84（EPSG:4326）への座標変換
- ポリゴンの簡略化（`st_simplify`）でデータサイズ削減
- **H3 化（候補抽出用）**：ポリゴンを `h3_polyfillash3` で H3 セル列に展開
  - 注意：`h3_polyfillash3` はセル**重心包含**のため、細いポリゴン（特に河川沿いの浸水想定や急傾斜地）はセルから漏れる場合があります
  - **本デモの原則：H3 r9 候補抽出 → `h3_kring(k=1)` で隣接セル拡張 → `st_envelope` bbox プレフィルタ → `st_contains` / `st_intersects` で最終判定 + polyfill 空/少数セル時の bbox-only fallback**（地価最近傍は adaptive k-ring + 距離順、詳細は「Geo JOIN の標準フロー」節）
- 行政区域は r6（広域集計）+ ジオメトリ保持で精密判定併用、メッシュ人口は r8、用途地域・ハザード・地価・物件 JOIN は r9 を採用
- **`sl_geo_admin` に `representative_lat / representative_lng` を追加**：`st_pointonsurface()` で行政区域ポリゴン内に必ず落ちる代表点を算出し、viewer 権限の位置マスキング代表点として使用（`st_centroid()` だと凹形・島しょ・マルチポリゴンでポリゴン外に出るリスクがあるため不採用。厳密な市区町村役所座標が必要な場合は別データ取得を案内）

### 4. NB 補完処理 + AI Functions + Geo JOIN
- SDP では書きにくい処理を Notebook で SQL + Python ハイブリッドで実装
- `ai_query` を用いて `inquiries.商談メモ` から顧客プロファイル（年収レンジ・家族構成・ライフステージ等）を抽出し、`customers` を enrich
- `ai_classify` による商談メモのトピック分類（「住宅ローン相談」「リフォーム相談」「ハザード懸念」など）
- **Geo JOIN（H3 r9 + k-ring 拡張 + bbox プレフィルタ + `st_*` 精密判定）**
  - **物件 × 用途地域**（用途地域コード付与）
  - **物件 × 浸水想定区域**（想定最大規模の浸水深ランク付与）
  - **物件 × 土砂災害警戒区域 / 特別警戒区域フラグ**
  - **物件 × 地価公示 L01 / L02**（k-ring + 距離順で最近傍 1〜3 点を取得、加重平均で推定地価を付与、査定価格との乖離スコア化）
  - **物件 × 最寄駅距離**（`st_distancesphere`）
  - **物件 × メッシュ人口**（H3 r8 セルで JOIN）
- **H3 セル単位の価格集計**（駅徒歩 5 分以内の H3 r9 セル別平均成約価格）

### 6. UC ガバナンス
| 機能 | 確認ポイント |
|---|---|
| テーブル / カラムコメント | 辞書ベースで一括設定、カタログ UI 上の自然言語検索 |
| PK / FK 制約 | 制約から ER 図が自動生成 |
| リネージ | `bz_*` → `sl_*` → `gd_*` → `mv_*` の系統がクリックで追える（地理空間系統も含む） |
| インサイト | ① このテーブルを参照する Dashboard / NB の自動表示 ② 誰がどんなクエリを実行したか ③ ヘビーユーザー TOP |
| カラムマスキング（PII） | 氏名・電話・住所（番地以下）の動的マスキング |
| **多段位置情報マスキング** | **権限グループ別に 3 段階で精度制御**：管理者は exact、分析担当は H3 r8 セル中心、一般閲覧は `sl_geo_admin.representative_lat/lng`（市区町村ポリゴン内代表点、`st_pointonsurface()` 由来）。**`sl_property_geo_enriched` に事前計算列**（`analyst_mask_lat/lng` / `viewer_mask_lat/lng`）を持たせ、マスク関数 UDF は権限判定で**列を選択するだけ**の軽量実装に寄せる（JOIN レス）。住所詳細は `sl_properties.address` に `mask_address_detail`、補助列 `analyst_mask_*` は `mask_analyst_helper`（admin/analyst のみ exact、viewer NULL）で保護 |

### 7. UC Metrics Views
- 不動産向け KPI を YAML で定義：
  - `metric_sales_summary`（成約数・成約価格・仲介手数料・値引き率、ディメンション：営業所 / 月 / 物件種別 / ハザード区分）
  - `metric_property`（売出期間・在庫件数・平均売出価格、ディメンション：物件種別 / 用途地域 / 駅徒歩区分）
  - `metric_customer`（顧客数・平均成約価格・RM セグメント分布、ディメンション：RM セグメント / 年代 / 性別）
  - **`metric_funnel`（反響→内見→申込→成約のファネル件数・転換率、ディメンション：営業所 / 月 / 来店区分 / 物件種別）**。元データは `inquiries.funnel_stage`
- Genie / Dashboard / SQL から **同じ定義** で参照
- FILTER 句・Composable measure の組み込み

### 8. RAG 構築（重要事項説明書 + 物件パンフ）
- PDF を SQL 関数 `ai_parse_document` で構造化（VARIANT 型）
- `ai_prep_search` でチャンク（`chunk_id` / `chunk_to_embed`）+ メタデータ自動生成
- ドキュメント種別（重説 / パンフ）のメタデータ付与でフィルタ可能な RAG に
- Vector Search Index は UI から Source Table・PK・Embedding カラムを選択して作成

### 9. マルチエージェント
- Playground 上で RAG エージェント（重説・パンフ参照）+ Genie エージェント（成約 + Geo 参照）を連携
- 1 つの問い合わせから両エージェントが連携して回答する流れを確認
  - 例：「この物件の重説に書かれているハザード情報と、現状の浸水想定区域の関係は？」

### 10. AI/BI Dashboard + Genie + Databricks One
- Gold 層から Dashboard 専用の Materialized View（`mv_*`）を作成
- `REFRESH MATERIALIZED VIEW` の差分更新挙動
- **H3 ヒートマップウィジェット**：H3 r8 セルごとの平均成約価格を地図上に表示
- ダッシュボードから Genie への自然言語クエリ連携
- Databricks One で営業担当者向けの簡易ポータルを構成（UI 設定のみ・専用 NB なし）

### 5. Jobs（SDP + 地理空間 + AI 混在 DAG）
```
[Task1: SDP パイプライン実行]  ──┐
                                  ├──►[Task3: NB 補完処理（AI + Geo JOIN）]
[Task2: 地理空間データ取り込み] ──┘            │
                                                ▼
                                       [Task4: NB Metrics Views 更新]
                                                ▼
                                       [Task5: NB MV 差分更新]
```

## 入力プロンプト集（再現用）

各機能で使う入力プロンプトの一覧です。詳細・追加例は対応する NB（07 / 08 / 09 / 11）に記載しています。

### Genie Space サンプル質問（デモステップ 9 / NB 08）
通常質問：
- 「先月、最も成約数が多かった営業所は？」
- 「マンションの平均売出期間は？」
- 「30 代ファミリー層に売れている物件種別 TOP 3 は？」

エージェントモード：
- 「**浸水想定 1m 以上**の物件と非該当物件で、平均値引き率を比較して」
- 「**土砂災害特別警戒区域（レッドゾーン）**の物件を営業所別に集計して、売出期間が長い TOP 5 を見せて」
- 「**駅徒歩 5 分以内** vs **15 分超** で、成約価格の差を市区町村別に比較して」
- 「**地価公示（L01）との乖離率**が大きい物件 TOP 10 を見せて」
- 「**1km メッシュの将来推計人口**と直近 1 年の成約数の相関を見せて」
- 「**funnel_stage 別**の月次転換率を営業所別に比較して」

### マルチエージェント デモ質問（デモステップ 9）
- 「物件 P-12345 の重要事項説明書に書かれているハザード情報と、現状の浸水想定区域の関係は？」（RAG + Genie 両参照）
- 「江東区で築 10 年以内マンションの典型的な特約事項と、直近の成約事例の値引き傾向を教えて」（RAG → Genie）
- 「先月最も成約した物件種別について、パンフ記載の省エネ等級 / 耐震等級の標準仕様を教えて」（Genie → RAG）

### RAG 検索クエリ例（デモステップ 8 / NB 05 + UI）
- 「重要事項説明書の心理的瑕疵に関する記載」
- 「マンション管理規約のペット飼育条項」
- 「戸建ての境界確定状況の記載」

### AI/BI Dashboard プロンプト（デモステップ 10 / NB 09・Genie Code）
ダッシュボード作成用の自然言語プロンプト：
- 「営業所別 × 月別の成約数と仲介手数料率をヒートマップで」
- 「物件種別の成約構成比をドーナツチャートで」
- 「築年 × 成約価格の散布図、**浸水想定区域該当の有無**で色分け」
- 「**H3 r8 セルごとの平均成約価格を地図ヒートマップで**」
- 「**駅徒歩分 × 価格弾力性**を物件種別別に折れ線で」
- 「**地価公示乖離率（査定価格 / 近傍地価公示）の分布**をエリア別ヒストグラムで」
- 「**funnel_stage 別の月次ファネル**をサンキー図で」

詳細プロンプトとウィジェット仕様は `09_ダッシュボード作成手順.py` を参照。

## 技術スタック詳細（リソース管理）

PoC・引き継ぎ・運用時のリファレンスとして、本デモで作成・利用するすべての Databricks リソースを一覧化したセクションです。
Pipeline ID / Job ID / Dashboard ID / Genie Space ID は実行後に埋めてください。

### 1. Unity Catalog リソース

| 項目 | 値 |
|---|---|
| カタログ | `komae_demo_v4`（事前作成済み） |
| スキーマ | `komae_demo_v4.real_estate_e2e_demo` |
| Volume | `komae_demo_v4.real_estate_e2e_demo.raw_data` |
| Volume パス | `/Volumes/komae_demo_v4/real_estate_e2e_demo/raw_data` |

### 2. テーブル / ビュー一覧（合計 50 オブジェクト）

#### Bronze 構造化（6 テーブル / SDP Streaming Table）
| テーブル | ソース | 説明 |
|---|---|---|
| `bz_offices` | `raw_data/offices.csv` | 営業所マスタ Bronze（30 件） |
| `bz_properties` | `raw_data/properties.csv` | 物件マスタ Bronze（3,000 件） |
| `bz_customers` | `raw_data/customers.csv` | 顧客マスタ Bronze（2,500 件） |
| `bz_market_index` | `raw_data/market_index.csv` | 不動産市況指標 Bronze（取引価格指数 + 地価指数 + 住宅ローン金利 + 建築費指数） |
| `bz_inquiries` | `raw_data/inquiries/*.csv`（5 日分） | 内見・問合せ履歴 Bronze（12,500 件） |
| `bz_contracts` | `raw_data/contracts/*.csv`（5 日分） | 成約 Bronze（5,000 件） |

#### Bronze 地理空間（10 テーブル / Delta）
| テーブル | ソース | 説明 |
|---|---|---|
| `bz_geo_zoning` | KSJ A29 Shapefile | 用途地域ポリゴン |
| `bz_geo_flood` | KSJ A31 Shapefile | 洪水浸水想定区域ポリゴン（想定最大規模の浸水深ランク属性付） |
| `bz_geo_landslide` | KSJ A33 Shapefile | 土砂災害警戒区域 / 特別警戒区域ポリゴン |
| `bz_geo_landprice_l01` | KSJ L01 Shapefile | **地価公示ポイント（年次価格）** |
| `bz_geo_landprice_l02` | KSJ L02 Shapefile | **都道府県地価調査ポイント（基準地）** |
| `bz_geo_stations` | KSJ N02 Shapefile | 鉄道駅ポイント（路線・運営会社属性付） |
| `bz_geo_admin` | KSJ N03 Shapefile | 行政区域ポリゴン |
| `bz_geo_pop_mesh` | KSJ mesh1000 Shapefile | 1km メッシュ別将来推計人口 |
| `bz_geo_isj` | ISJ CSV | 大字・町丁目位置参照情報 |
| `bz_geo_osm_poi` | OSM Overpass GeoJSON | OSM POI（コンビニ・スーパー・学校など） |

> Bronze では地価系（L01/L02）を別テーブルとして取り込み、Silver で `sl_geo_landprice` に統合します。`bz_geo_isj`（位置参照情報）は物件のジオコーディング処理にのみ使うため Silver 独立テーブルを作りません。

#### Bronze RAG（1 テーブル / Delta）
| テーブル | ソース | 説明 |
|---|---|---|
| `bz_doc_parsed` | `raw_data/pdf/*.pdf` | 重説 + パンフを `ai_parse_document` で構造化（VARIANT） |

#### Silver 構造化（6 テーブル / SDP Streaming Table）
| テーブル | PK | FK | 説明 |
|---|---|---|---|
| `sl_offices` | `office_id` | — | 営業所マスタ |
| `sl_properties` | `property_id` | `office_id` → `sl_offices` | 物件マスタ（Expectations: 築年 / 緯度経度範囲 / 価格） |
| `sl_customers` | `customer_id` | `registered_office_id` → `sl_offices` | 顧客マスタ |
| `sl_market_index` | `(month, area_code, property_type)` | — | 不動産市況指標 |
| `sl_inquiries` | `inquiry_id` | `customer_id` / `office_id` / `property_id` | 内見・問合せ履歴（`funnel_stage` 列を含む） |
| `sl_contracts` | `contract_id` | `inquiry_id` / `customer_id` / `office_id` / `property_id` | 成約 |

#### Silver 地理空間（8 テーブル / Delta、H3 r9 候補抽出列 + ジオメトリ保持）

> 各テーブルの「論理キー」は UC 制約として張りません（fallback 行 `h3_r9 = NULL` と PK NOT NULL 要件が両立しないため）。同じキーは「論理キー」表にも記載済み。

| テーブル | 論理キー | 説明 |
|---|---|---|
| `sl_geo_zoning` | `(zoning_id, h3_r9)` | 用途地域。H3 r9 で候補抽出列を展開、ジオメトリ列保持で精密判定 |
| `sl_geo_flood` | `(flood_id, h3_r9, flood_depth_class)` | 浸水想定（想定最大規模）。H3 r9 + 浸水深ランク |
| `sl_geo_landslide` | `(landslide_id, h3_r9, hazard_type)` | 土砂災害警戒区域。`hazard_type` ∈ {警戒区域, 特別警戒区域} |
| `sl_geo_landprice` | `(landprice_id, year, source)` | **地価公示 L01 + 基準地 L02 統合（`source` で識別）**。最近傍検索用に H3 r9 列付与 |
| `sl_geo_stations` | `station_id` | 鉄道駅（緯度経度 + 路線属性 + H3 r9 列） |
| `sl_geo_admin` | `admin_code` | 行政区域。Bronze 後に admin_code で dissolve 済み。H3 r6 列 + `representative_lat/lng`（`representative_point()` 由来、必ずポリゴン内）を保持 |
| `sl_geo_pop_mesh` | `(mesh_pk, h3_r8, year)` | メッシュ人口。H3 r8 + 推計年 |
| `sl_geo_osm_poi` | `poi_id` | OSM POI（カテゴリ別、H3 r9 列付与） |

#### Silver RAG（1 テーブル / Delta）
| テーブル | PK | 説明 |
|---|---|---|
| `sl_doc_chunks` | `chunk_id` | `ai_prep_search` で生成したチャンク + メタデータ。VS Index のソース |

#### Silver enrich（3 テーブル / Delta、`sl_*_enriched`）
| テーブル | PK | FK | 説明 |
|---|---|---|---|
| `sl_inquiries_enriched` | `inquiry_id` | `customer_id` / `office_id` / `property_id` | 商談メモ AI 分類 + Whisper 文字起こし |
| `sl_customers_enriched` | `customer_id` | `customer_id` → `sl_customers` | `ai_query` で年収・家族構成を enrich |
| `sl_property_geo_enriched` | `property_id` | `property_id` → `sl_properties` | **物件 × 用途地域 / ハザード（浸水・土砂）/ 最寄駅 / メッシュ人口 / 地価公示乖離 を Geo JOIN**。さらにマスキング用の事前計算列 `admin_code` / `analyst_mask_lat` / `analyst_mask_lng`（H3 r8 セル中心）/ `viewer_mask_lat` / `viewer_mask_lng`（`sl_geo_admin.representative_lat/lng` 由来）を保持 |

#### Gold（6 テーブル / Delta、`gd_*`）
| テーブル | PK | FK | 由来 | 説明 |
|---|---|---|---|---|
| `gd_office_monthly_sales` | `(office_id, sales_month)` | `office_id` → `sl_offices` | NB 02 | 営業所 × 月別 成約数・仲介手数料 |
| `gd_property_inventory` | `(property_type, status)` | — | NB 02 | 物件種別 × ステータス別 在庫サマリ |
| `gd_market_linked_margin` | `(month, area_code, property_type)` | — | NB 02 | 市況指標 × 値引額の月次比較 |
| `gd_property_hazard_summary` | `(area_code, hazard_type)` | — | **NB 05** | エリア × ハザード別 値引き率・売出期間（ハザード付与は Geo JOIN 後のため NB 05 由来） |
| `gd_contract_discount_score` | `contract_id` | `contract_id` / `office_id` / `property_id` / `customer_id` | NB 05 | **契約単位の値引きスコア**（粒度：1 契約 1 行）。月次×営業所×物件種別×ハザードの集計値は MV `mv_hazard_discount` 側で行う |
| `gd_customer_rm_segment` | `customer_id` | `customer_id` → `sl_customers` | NB 05 | RM 分析（最終接点経過月 × 累計取引額） |

#### Materialized View（4 / `mv_*`、Dashboard 専用・差分更新）
| MV | 用途 |
|---|---|
| `mv_dashboard_kpi` | 経営 KPI（月次成約数・成約価格・仲介手数料率） |
| `mv_h3_price_heatmap` | H3 r8 セルごとの平均成約価格（地図ヒートマップ用） |
| `mv_hazard_discount` | ハザード区分別 × 月の値引き率 |
| `mv_sales_funnel` | 反響 → 内見 → 申込 → 成約のファネル件数・転換率（`inquiries.funnel_stage` 由来） |

#### Metric Views（4 / UC Metric Views）
| Metric View | 主要 Measure | 主要 Dimension |
|---|---|---|
| `metric_sales_summary` | 成約数 / 成約価格 / 仲介手数料 / 値引き率 | 営業所 / 月 / 物件種別 / ハザード区分 |
| `metric_property` | 売出期間 / 在庫件数 / 平均売出価格 | 物件種別 / 用途地域 / 駅徒歩区分 |
| `metric_customer` | 顧客数 / 平均成約価格 / RM セグメント分布 | RM セグメント / 年代 / 性別 |
| `metric_funnel` | 反響数 / 内見数 / 申込数 / 成約数 / 転換率 | 営業所 / 月 / 来店区分 / 物件種別 |

#### Vector Search Index（1）
| Index | Source Table | PK | Embedding 列 |
|---|---|---|---|
| `idx_property_docs` | `sl_doc_chunks` | `chunk_id` | `chunk_to_embed` |

#### オブジェクト数集計
| 区分 | 数 |
|---|---|
| Bronze 構造化 | 6 |
| Bronze 地理空間 | 10 |
| Bronze RAG | 1 |
| Silver 構造化 | 6 |
| Silver 地理空間 | 8 |
| Silver RAG | 1 |
| Silver enrich | 3 |
| Gold（`gd_*`、NB 02 由来 3 + NB 05 由来 3） | 6 |
| Materialized View（`mv_*`） | 4 |
| Metric Views | 4 |
| VS Index | 1 |
| **合計** | **50** |

### 3. UC 制約として張る PK / FK

| テーブル群 | 対象テーブル | PK 数 | FK 数 | 設定 NB |
|---|---|---|---|---|
| Silver SDP（構造化） | `sl_offices` / `sl_properties` / `sl_customers` / `sl_market_index` / `sl_inquiries` / `sl_contracts` | 6 | 9 | NB 02（インライン宣言） |
| Silver RAG | `sl_doc_chunks` | 1 | — | NB 06（`ALTER TABLE ADD CONSTRAINT`） |
| Silver enrich | `sl_inquiries_enriched` / `sl_customers_enriched` / `sl_property_geo_enriched` | 3 | 5 | NB 06（`ALTER TABLE ADD CONSTRAINT`） |
| Gold（NB 02 由来） | `gd_office_monthly_sales` / `gd_property_inventory` / `gd_market_linked_margin` | 3 | 1 | NB 02（インライン宣言） |
| Gold（NB 05 由来） | `gd_property_hazard_summary` / `gd_contract_discount_score` / `gd_customer_rm_segment` | 3 | 5 | NB 06（`ALTER TABLE ADD CONSTRAINT`） |
| **合計** | | **16** | **20** | |

> Silver 地理空間（`sl_geo_*` 8 件）は UC 制約 PK を**論理キーのみ**とし、`ALTER TABLE ADD CONSTRAINT` は付与しません。理由：`EXPLODE_OUTER(h3_polyfillash3())` を使うテーブルでは fallback 行の H3 展開列（`h3_r9` / `h3_r8` 等）が NULL になり得るため、Databricks の PK 列 NOT NULL 要件と両立しません（Point 系の `h3_longlatash3` テーブルや `sl_geo_admin` も整合性のため同方針に揃えています）。論理キーは下の「論理キー」表に記載。

### 4. 論理キー（UC 制約は張らないが文書化目的の主キー）

| テーブル | 論理キー | 種別 |
|---|---|---|
| `sl_geo_zoning` | `(zoning_id, h3_r9)` | Silver 地理空間 |
| `sl_geo_flood` | `(flood_id, h3_r9, flood_depth_class)` | Silver 地理空間 |
| `sl_geo_landslide` | `(landslide_id, h3_r9, hazard_type)` | Silver 地理空間 |
| `sl_geo_landprice` | `(landprice_id, year, source)` | Silver 地理空間 |
| `sl_geo_stations` | `station_id` | Silver 地理空間 |
| `sl_geo_admin` | `admin_code` | Silver 地理空間 |
| `sl_geo_pop_mesh` | `(mesh_pk, h3_r8, year)` | Silver 地理空間 |
| `sl_geo_osm_poi` | `poi_id` | Silver 地理空間 |
| `mv_dashboard_kpi` | `(month, office_id)` | Materialized View |
| `mv_h3_price_heatmap` | `(h3_r8, month, property_type)` | Materialized View |
| `mv_hazard_discount` | `(month, hazard_type, area_code)` | Materialized View |
| `mv_sales_funnel` | `(month, office_id, funnel_stage)` | Materialized View |
| `idx_property_docs` | `chunk_id` | Vector Search Index |

### 5. AI Functions / モデル

| 利用機能 | 関数 | モデル | 使用ノートブック |
|---|---|---|---|
| 商談メモのトピック分類 | `ai_classify` | `databricks-claude-opus-4-7` | NB 05 |
| 顧客プロファイル enrich | `ai_query` | `databricks-claude-opus-4-7` | NB 05 |
| 重説 / パンフ PDF 構造化 | `ai_parse_document` | （内部モデル） | NB 05 |
| RAG 用チャンク生成 | `ai_prep_search` | （内部モデル） | NB 05（チャンク化を 05 末尾で実施。VS Index 作成は UI） |

### 6. Model Serving エンドポイント

| エンドポイント | 用途 | デプロイ元 |
|---|---|---|
| `komae_whisper_large_v3` | 接客録音 MP3 の文字起こし | UC 上の Whisper Large v3 モデル |

### 7. 外部データソース（事前取得が必要）

| ソース | 取得方法 | 認証 | 01 NB の対応セクション |
|---|---|---|---|
| **reinfolib API**（不動産取引価格情報・地価公示 API） | REST API（四半期更新） | API キー（無料登録） | 01-A |
| **KSJ ファイル（Shapefile / GML）**（A29 / A31 / A33 / **L01** / **L02** / N02 / N03 / mesh1000） | ZIP ダウンロード | 不要（CC-BY 4.0） | 01-B |
| **ISJ CSV**（大字・町丁目位置参照情報） | ZIP ダウンロード | 不要 | 01-C |
| **OSM Overpass API**（POI） | REST API（バウンディングボックス指定） | 不要（レート制限有） | 01-D |

### 8. Genie Space / AI/BI Dashboard

| リソース | 名前 | ID | 作成 NB |
|---|---|---|---|
| Genie Space | 不動産仲介 E2E 分析 | `_______________` | NB 08 |
| Dashboard | 不動産仲介 E2E ダッシュボード（H3 地図ウィジェット含む） | `_______________` | NB 09 |

### 9. SDP Pipeline / Jobs

| リソース | 名前 | ID | 作成 NB |
|---|---|---|---|
| SDP パイプライン | E2E_real_estate_pipeline | `_______________` | NB 04 |
| Jobs ワークフロー | 不動産 E2E ETL | `_______________` | NB 10 |

### 10. PII / 多段位置情報マスキング関数（NB 06 で定義）

> **権限グループ名の表記**：本セクションで「`admin` / `analyst` / `viewer`」と書かれている箇所は、すべて 00_config.py で定義した `GROUP_ADMIN` / `GROUP_ANALYST` / `GROUP_VIEWER`（実値：`real_estate_admin` / `real_estate_analyst` / `real_estate_viewer`）を指します。NB 06 で `is_account_group_member()` を実装する際は必ず 00_config.py の定数を参照してください。

設計方針：

- **`sl_property_geo_enriched` を業務クエリの一次参照先**とする。同テーブルに事前計算列（`admin_code` / `analyst_mask_lat/lng` / `viewer_mask_lat/lng`）を持ち、UC のカラムマスクは **同一テーブル内の他列を引数として渡す軽量 UDF** で実装（JOIN レス）
- **`sl_properties.lat / lng`（素データ）** には別途、`admin` 限定マスク（admin 以外 NULL）を適用。3 段マスクは enriched テーブル側で完結させる
- これにより、`sl_properties` 経由でクエリしても素の lat/lng は admin 以外には漏れず、業務クエリは `sl_property_geo_enriched` の 3 段マスク済み列を使う運用に統一できる

| 関数名 | 適用列 | 動作 |
|---|---|---|
| `mask_name` | `sl_customers.name` | `is_account_group_member(GROUP_ADMIN)`（`real_estate_admin` グループ）不一致時に伏字 |
| `mask_phone` | `sl_customers.phone` | 末尾 4 桁以外を伏字 |
| `mask_address_detail` | `sl_properties.address` | 番地以下を伏字（市区町村・町丁目までは見せる） |
| `mask_geo_admin_only(coord)` | `sl_properties.lat`, `sl_properties.lng` | `admin` のみ exact、それ以外 NULL（素データテーブルからの直接参照を抑止） |
| `mask_geo_lat(exact_lat, analyst_lat, viewer_lat)` | `sl_property_geo_enriched.lat`（事前計算列 `analyst_mask_lat` / `viewer_mask_lat` を同テーブル内の他列として引数渡し） | **権限 3 段階の列選択**：`admin` → `exact_lat`、`analyst` → `analyst_lat`、`viewer` → `viewer_lat` |
| `mask_geo_lng(exact_lng, analyst_lng, viewer_lng)` | `sl_property_geo_enriched.lng`（同上） | 上記と同じ判定で経度の列を選択。**事前計算で一貫性を担保**しているため `(lat, lng)` JOIN 不要 |

派生列のマスク方針：(a) 住所詳細は `sl_properties.address` に `mask_address_detail`（番地以下を伏字）、(b) 補助列 `analyst_mask_lat/lng` は `mask_analyst_helper`（viewer は NULL）、(c) `viewer_mask_lat/lng` は市区町村レベルなので全グループに公開可。H3 r9 列等の高精度 ID 列を直接公開しないテーブル設計とする。詳細実装は NB 06 を参照。

### 11. Volume 配置物

| パス | 内容 | 件数・サイズ目安 |
|---|---|---|
| `/Volumes/.../raw_data/offices.csv` | 営業所マスタ CSV | 30 |
| `/Volumes/.../raw_data/properties.csv` | 物件マスタ CSV | 3,000 |
| `/Volumes/.../raw_data/customers.csv` | 顧客マスタ CSV | 2,500 |
| `/Volumes/.../raw_data/market_index.csv` | 不動産市況指標 CSV | reinfolib + KSJ L01/L02 より |
| `/Volumes/.../raw_data/inquiries/*.csv` | 内見・問合せ履歴 日次分割 | 5 日分 |
| `/Volumes/.../raw_data/contracts/*.csv` | 成約 日次分割 | 5 日分 |
| `/Volumes/.../raw_data/geo/A29/*.{shp,gml,xml}` | 用途地域 Shapefile | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/A31/*.{shp,gml,xml}` | 洪水浸水想定区域 Shapefile（想定最大規模） | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/A33/*.{shp,gml,xml}` | 土砂災害警戒区域 / 特別警戒区域 Shapefile | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/L01/*.{shp,gml,xml}` | **地価公示 Shapefile** | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/L02/*.{shp,gml,xml}` | **都道府県地価調査 Shapefile** | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/N02/*.{shp,gml,xml}` | 鉄道駅 Shapefile | 全国（フィルタで限定） |
| `/Volumes/.../raw_data/geo/N03/*.{shp,gml,xml}` | 行政区域 Shapefile | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/mesh1000/*.{shp,gml,xml}` | 1km メッシュ別将来推計人口 | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/isj/*.csv` | 大字・町丁目位置参照情報 | 7 都道府県分 |
| `/Volumes/.../raw_data/geo/osm/*.geojson` | OSM POI | エリア絞り込み |
| `/Volumes/.../raw_data/pdf/jyusetsu_*.pdf` | 重要事項説明書 PDF（架空） | 5 |
| `/Volumes/.../raw_data/pdf/pamphlet_*.pdf` | 物件パンフ PDF（架空） | 5 |
| `/Volumes/.../raw_data/audio/*.mp3` | 接客録音 MP3（架空） | 5〜10 |

### 12. ノートブック → リソース マッピング

| NB | 対応デモステップ | 作成・更新するリソース |
|---|---|---|
| `00_config.py` | — | スキーマ / Volume |
| `01_データ準備.py` | — | Volume 配置物（reinfolib 取得 / KSJ ファイル（Shapefile / GML）（L01/L02 含む）/ ISJ / OSM / PDF / MP3） |
| `02_SDPパイプライン定義.sql` | 2 | Bronze 構造化 6 / Silver 構造化 6 / Gold（NB 02 由来）3（定義のみ。実行は NB 04） |
| `03_地理空間データパイプライン.py` | 3 | Bronze 地理空間 10 / Silver 地理空間 8（H3 化 + ジオメトリ保持 + `sl_geo_admin.representative_lat/lng`） |
| `04_SDPパイプライン設定手順.py` | 2 | SDP パイプライン本体（UI 操作） |
| `05_AIとGeoによる補完処理.py` | 4 / 8（PDF 構造化のみ） | Silver enrich 3 / Gold（NB 05 由来）3（`gd_property_hazard_summary` 含む）/ `bz_doc_parsed` / `sl_doc_chunks` + ai_classify/ai_query/ai_parse_document/Whisper |
| `06_テーブル設定.py` | 6 | 全テーブルのコメント / Silver enrich + RAG + Gold（NB 05 由来）の PK/FK / PII + 多段位置情報マスク |
| `07_UC_Metrics_Views.py` | 7 | Metric Views 4 件（`metric_funnel` 含む） |
| `08_Genie作成手順.py` | 9 | Genie Space（UI 操作） |
| `09_ダッシュボード作成手順.py` | 10 | AI/BI Dashboard + Genie Code（UI 操作）+ MV 4 件 |
| `10_Jobsワークフロー作成手順.py` | 5 | Jobs（UI 操作） |
| `11_Genie Codeインタラクティブ分析.py` | 10 | Genie への質問プロンプト（NB 内に出力） |

Databricks One は UI 設定のみで専用 NB を持ちません（デモステップ 10 の最終演出）。
