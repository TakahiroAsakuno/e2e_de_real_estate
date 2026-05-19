# OpenStreetMap POI - 取得済みデータ

このディレクトリには、Overpass API で取得した OpenStreetMap の POI（コンビニ・スーパー・学校）を
GeoJSON FeatureCollection 形式で **gzip 圧縮**して格納します。

> Databricks / GitHub からのクローン高速化のため `.geojson.gz` でコミットしています
> （元データ 約48MB → 約3MB、圧縮率 6%）。
> `01_データ準備.py` の `sync_repo_osm_to_volume()` が Volume コピー時に自動展開します。

## ファイル命名規則

```
{region}_{tag}.geojson.gz
```

例：`kanto_shop_convenience.geojson.gz`（首都圏のコンビニ）

新規取得スクリプトで `.geojson` のまま追加した場合も同関数が読み込めますが、
**コミット時は `gzip -9` 圧縮**してください（例：`gzip -9 *.geojson`）。

| region | 対象範囲 | bbox（南西緯, 南西経, 北東緯, 北東経） |
|---|---|---|
| kanto | 首都圏（東京・神奈川・千葉・埼玉） | (34.95, 138.95, 36.30, 140.90) |
| kansai | 大阪府 | (34.30, 135.20, 35.10, 136.10) |
| chubu | 愛知県 | (34.55, 136.60, 35.60, 137.85) |
| kyushu | 福岡県 | (33.10, 130.00, 34.10, 131.10) |

| tag | 内容 |
|---|---|
| shop_convenience | コンビニ |
| shop_supermarket | スーパー |
| amenity_school | 学校 |

合計 4 region × 3 tag = **12 ファイル**

## GeoJSON 構造

Overpass API レスポンスを GeoJSON FeatureCollection に変換した形式：

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [139.7795549, 35.7823732] },
      "properties": {
        "name": "セブン-イレブン",
        "brand": "7-ELEVEN",
        "shop": "convenience",
        "osm_type": "node",
        "osm_id": 261000759,
        "tag": "shop=convenience"
      }
    },
    ...
  ]
}
```

## 出典

このデータは [OpenStreetMap](https://www.openstreetmap.org/) のデータを
[Overpass API](https://overpass-api.de/) 経由で取得したものです。

> 出典：© OpenStreetMap contributors

## ライセンス

[Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/) に基づき、
出典明示を条件に**再配布・加工・商用利用が可能**です。

加工して再配布する場合は、加工した旨を明記する必要があります。

## データ取得方法

データを**追加・更新する場合**：

```bash
# 全 21 件を取得（5 秒間隔、Overpass レート制限回避）
python3 scripts/fetch_osm.py --sleep 5

# 特定 region / tag のみ
python3 scripts/fetch_osm.py --region osaka --tag shop=convenience
```

**デモを実行するだけの利用者は API キー不要**です（Overpass API はキー不要）。
このディレクトリの GeoJSON が `01_データ準備.py` で自動的に Volume にコピーされます。
