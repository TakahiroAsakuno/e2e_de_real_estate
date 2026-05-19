# データ管理方針

このディレクトリは、不動産仲介 E2E デモで使用するデータの管理場所です。
データの種類によって Git 管理方針が異なります。

## ディレクトリ構成

```
data/
├── pdf/                  Git 管理（自前生成 PDF：重要事項説明書 + 物件パンフ、各 5 件）
├── audio/                Git 管理（自前生成 MP3：接客録音、5〜10 件）
└── external/
    ├── reinfolib/        Git 管理 ✅（PDL 1.0、出典明示で再配布可）
    ├── ksj/              Git 管理外（サイズが大きいため）
    ├── isj/              Git 管理外
    └── osm/              Git 管理外
```

## API キーが必要 / 不要な場面

| 利用シーン | API キー | 備考 |
|---|---|---|
| **デモを実行するだけ** | **不要** | `data/external/reinfolib/` の JSON を Volume にコピーして使う |
| reinfolib データを最新版に更新 | 必要 | `FORCE_REFRESH=True` で再取得 → 結果を git commit |
| KSJ / ISJ / OSM の最新データ取得 | 不要（公開 ZIP） | ただしネットワーク必要 |

## ファイル種別ごとの方針

| データ種別 | 入手方法 | Git 管理 | ライセンス | 備考 |
|---|---|---|---|---|
| **PDF（重説 + パンフ）** | 自前生成（Claude） | あり | 自由 | リポジトリで配布 |
| **MP3（接客録音）** | 自前生成 | あり | 自由 | リポジトリで配布 |
| **KSJ ZIP** | 公式 ZIP DL | **なし** | [CC-BY 4.0 互換](https://nlftp.mlit.go.jp/ksj/other/agreement_01.html) | 都度 DL、Volume キャッシュ |
| **ISJ CSV** | 公式 ZIP DL | **なし** | 公式規約準拠 | 都度 DL、Volume キャッシュ |
| **reinfolib API** | REST API（要 API キー）→ 取得済み JSON を Git 管理 | **あり** | [PDL 1.0](https://www.reinfolib.mlit.go.jp/help/termsOfUse/)（出典明示で再配布可） | 取得済みデータをリポジトリ同梱、利用者は API キー不要 |
| **OSM POI** | Overpass API | **なし** | ODbL | 都度 DL、Volume キャッシュ |

## ライセンス上の判断

外部データを **Git 管理しない理由**：

1. **reinfolib**：API キーが利用者単位で発行され、ユーザー責任で取得する建付け
2. **KSJ**：CC-BY 4.0 互換で再配布は可能だが、ZIP サイズが 7 都府県分で数百 MB〜数 GB になりリポジトリが肥大化
3. **ISJ / OSM**：各々のライセンスに従えば再配布可だが、KSJ 同様にサイズが大きい

→ 外部データは **初回 DL してローカル `data/external/` と Databricks Volume に保存し、以降は再利用** する戦略をとります。

## キャッシュの仕組み

`sample/e2e_de_real_estate/01_データ準備.py` は以下のロジックで動作：

1. 各セクション（reinfolib / KSJ / ISJ / OSM）の冒頭で Volume の対象パスに既存ファイルがあるか確認
2. 既存ファイルがあれば **DL をスキップ**
3. **`FORCE_REFRESH = True`** に変更すれば全データを強制再 DL

## 出典明示（再配布時）

将来 KSJ データを再配布する場合は、以下の出典明示が必要：

> 出典：『国土数値情報（用途地域 A29 等）』（国土交通省）（https://nlftp.mlit.go.jp/ksj/）（取得年月日）

データを加工して再配布する場合は：

> 『国土数値情報（用途地域 A29 等）』（国土交通省）（URL）を加工して作成
