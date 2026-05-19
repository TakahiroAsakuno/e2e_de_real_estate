# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 03 | 地理空間データパイプライン
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ｜KSJ Shapefile / GML → Bronze 地理空間 10 → Silver 地理空間 8（H3 化）</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 20 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC 01 で Volume 配置した KSJ ファイル（Shapefile / GML）/ ISJ / OSM GeoJSON を読み込み、Bronze 地理空間 10 / Silver 地理空間 8 のテーブルを作成します。<br>
# MAGIC <ul style="margin-top: 8px;">
# MAGIC   <li><b>Bronze 地理空間 10</b>：KSJ A29/A31/A33/L01/L02/N02/N03/mesh1000 + ISJ + OSM POI</li>
# MAGIC   <li><b>Silver 地理空間 8</b>：H3 r9 候補抽出列 + ジオメトリ（WKB 原典 + 簡略化）保持。L01/L02 は <code>sl_geo_landprice</code> に統合</li>
# MAGIC   <li><b>polyfill は原典 WKB を使用</b>：簡略化は表示・軽量 JOIN 用に併存、候補抽出での false negative を避ける</li>
# MAGIC   <li><b><code>sl_geo_admin</code></b>：Bronze 後に <code>dissolve(by="admin_code")</code> で 1 行化し、<code>representative_point()</code> で代表点を算出</li>
# MAGIC </ul>
# MAGIC <strong>⚠ PK 制約の方針</strong>：Silver 地理空間の PK（<code>(zoning_id, h3_r9)</code> 等）は<b>論理キー扱い</b>とし、<code>ALTER TABLE ADD CONSTRAINT</code> は付けません。理由：(a) <code>EXPLODE_OUTER</code> による h3_r9 NULL 行と PK 列 NOT NULL 要件が両立しない、(b) Databricks の PK は情報的制約で非強制のため、ガバナンス価値は README の論理キー一覧で代替。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ライブラリインストール
# MAGIC %pip install --quiet geopandas==0.14.4 shapely==2.0.4 pyproj==3.6.1 fiona==1.9.6

# COMMAND ----------

# DBTITLE 1,Python 再起動
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリインポート
import os
import glob as _glob
import hashlib
from typing import List, Optional, Tuple, Dict

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
import pyproj

from pyspark.sql import functions as F

print(f"geopandas : {gpd.__version__}")
print(f"pyproj    : {pyproj.__version__}")

# COMMAND ----------

# DBTITLE 1,KSJ 列名マッピング（公開仕様に基づく推測値、要動作確認）
# 各 KSJ データセットの「使う列」を Silver 契約列名にマップ。
# 実データで列名が異なる場合は、ここを修正するだけで全体に反映される。
# TODO(列名): 実データを読み込んだ後、KSJ 公式仕様書で列名を確認して調整
KSJ_COLUMN_MAP = {
    "A29": {  # 用途地域
        "zoning_code": ["A29_004", "用途地域コード"],
        "zoning_name": ["A29_005", "用途地域名"],
    },
    "A31": {  # 洪水浸水想定（想定最大規模）
        "flood_depth_class": ["A31_001", "A31a_101", "浸水深ランクコード"],
        "duration_class":    ["A31_002", "A31a_102", "浸水継続時間ランク"],
    },
    "A33": {  # 土砂災害警戒区域
        "hazard_type": ["A33_001", "現象区分", "ハザード種別"],
        "hazard_grade": ["A33_002", "警戒区分"],
    },
    "L01": {  # 地価公示
        "year":  ["L01_005", "公示年", "L01_LandPriceYear"],
        "price": ["L01_006", "公示価格"],
    },
    "L02": {  # 都道府県地価調査
        "year":  ["L02_005", "基準年", "L02_StandardYear"],
        "price": ["L02_006", "基準地価格"],
    },
    "N02": {  # 鉄道駅
        "station_name": ["N02_005", "駅名"],
        "line_name":    ["N02_003", "路線名"],
        "operator":     ["N02_004", "運営会社"],
    },
    "N03": {  # 行政区域
        "admin_code": ["N03_007", "行政コード"],
        "admin_name": ["N03_004", "市区町村名", "N03_003"],
    },
    "mesh1000": {  # 1km メッシュ別将来推計人口
        "mesh_id":  ["MESH_ID", "MESH1KM_ID"],
        "year":     ["YEAR", "推計年"],
        "pop_total": ["POP_TOTAL", "POPT_TOTAL"],
    },
}


def find_first_existing_column(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def normalize_columns(gdf: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
    if dataset not in KSJ_COLUMN_MAP:
        return gdf
    mapping = KSJ_COLUMN_MAP[dataset]
    rename_map: Dict[str, str] = {}
    null_cols: List[str] = []
    for target_col, candidates in mapping.items():
        found = find_first_existing_column(list(gdf.columns), candidates)
        if found:
            rename_map[found] = target_col
        else:
            null_cols.append(target_col)
    gdf = gdf.rename(columns=rename_map)
    for col in null_cols:
        gdf[col] = None
    return gdf


print("✅ KSJ_COLUMN_MAP / normalize_columns 定義完了")

# COMMAND ----------

# DBTITLE 1,共通ヘルパー（読み込み・座標変換・行 ID 生成・WKB 化）
def read_geo_files(geo_dir: str, exts: Tuple[str, ...] = (".shp", ".gml", ".geojson", ".xml")) -> gpd.GeoDataFrame:
    """ファイルパスを sorted で決定的順序にし、各 GeoDataFrame に _source_path（相対パス）と _row_in_file（ファイル内連番）を付与。
    これにより stable_row_id が読み込み順・フィルタ後の行順に依存しなくなる。"""
    paths: List[str] = []
    for root, dirs, files in os.walk(geo_dir):
        dirs.sort()
        for f in sorted(files):
            if f.lower().endswith(exts):
                paths.append(os.path.join(root, f))
    paths = sorted(paths)
    if not paths:
        raise FileNotFoundError(f"地理空間ファイルが見つかりません: {geo_dir}（拡張子 {exts}）")

    frames: List[gpd.GeoDataFrame] = []
    for p in paths:
        try:
            g = gpd.read_file(p)
            if g.empty:
                continue
            rel_path = os.path.relpath(p, geo_dir)
            g["_source_path"] = rel_path
            g["_source_file"] = os.path.basename(p)
            g["_row_in_file"] = range(len(g))   # ファイル内の元の行番号（フィルタ後も保持）
            frames.append(g)
        except Exception as e:
            print(f"⚠ 読み込み失敗: {p}: {e}")
    if not frames:
        raise RuntimeError(f"読み込み可能な地理空間ファイルが見つかりません: {geo_dir}")
    base_crs = frames[0].crs
    aligned = [g.to_crs(base_crs) if g.crs and g.crs != base_crs else g for g in frames]
    return gpd.GeoDataFrame(pd.concat(aligned, ignore_index=True), crs=base_crs)


def to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        print("⚠ CRS 未設定。JGD2011 緯度経度 (EPSG:6668) を仮定")
        gdf = gdf.set_crs(epsg=6668)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def validate_in_japan_bbox_strict(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """bounds の minx/maxx/miny/maxy が全て日本 bbox 内のジオメトリのみ残す（座標系誤認のセーフティ）"""
    JP_BBOX = (122.0, 24.0, 146.0, 46.0)
    minx_lim, miny_lim, maxx_lim, maxy_lim = JP_BBOX
    if gdf.empty:
        return gdf
    bounds = gdf.geometry.bounds
    mask = (
        (bounds["minx"] >= minx_lim) & (bounds["maxx"] <= maxx_lim)
        & (bounds["miny"] >= miny_lim) & (bounds["maxy"] <= maxy_lim)
    )
    n_before = len(gdf)
    gdf = gdf[mask].reset_index(drop=True)
    n_after = len(gdf)
    if n_after < n_before:
        print(f"⚠ 日本 bbox 外（または bbox を跨ぐ） {n_before - n_after} 件を除外")
    return gdf


def stable_row_id(row_in_file: int, geom: Optional[BaseGeometry], source_path: Optional[str]) -> int:
    """行ごとに一意な決定的 ID。
    入力：(_row_in_file, geometry, _source_path)。これらは read_geo_files で決定的に付与されるため、
    読み込み順・フィルタ後の行順・基底ディレクトリの違いに影響されない。"""
    h = hashlib.md5()
    h.update(str(row_in_file).encode("utf-8"))
    h.update((source_path or "").encode("utf-8"))
    if geom is not None:
        wkb_bytes = geom.wkb
        h.update(wkb_bytes[:1024])
        if len(wkb_bytes) > 1024:
            h.update(wkb_bytes[-256:])
    return int.from_bytes(h.digest()[:8], byteorder="big", signed=True)


def gdf_to_spark_with_row_ids(
    gdf: gpd.GeoDataFrame,
    *,
    add_simplified: bool = True,
    simplify_tolerance_deg: float = 0.00005,
) -> "DataFrame":
    """GeoDataFrame → Spark DataFrame。各行に決定的な stable row_id を付与。原典 WKB + 簡略化 WKB を別列で保持。
    _row_in_file（ファイル内連番）と _source_path（相対パス）が読み込み時点で付与されている前提（read_geo_files）。"""
    pdf = gdf.reset_index(drop=True).copy()
    if "_row_in_file" not in pdf.columns:
        # 後付け（dissolve 後など）の場合のフォールバック：ソート済みの key 列 + 連番
        pdf["_row_in_file"] = range(len(pdf))
    if "_source_path" not in pdf.columns:
        pdf["_source_path"] = pdf.get("_source_file", "")
    pdf["row_id"] = [
        stable_row_id(rif, g, sp)
        for rif, g, sp in zip(pdf["_row_in_file"], pdf["geometry"], pdf["_source_path"])
    ]
    pdf["geom_wkb"] = pdf["geometry"].apply(lambda g: g.wkb if g is not None else None)
    if add_simplified:
        pdf["geom_wkb_simplified"] = pdf["geometry"].apply(
            lambda g: g.simplify(simplify_tolerance_deg, preserve_topology=True).wkb if g is not None else None
        )
    pdf = pdf.drop(columns=["geometry"])
    pdf["_ingested_at"] = pd.Timestamp.utcnow()
    return spark.createDataFrame(pdf)


def dissolve_by_key(gdf: gpd.GeoDataFrame, key: str) -> gpd.GeoDataFrame:
    """key 列でポリゴンを結合（1 key = 1 行）。N03 の島しょ・分割ポリゴン対応。
    dissolve 後は _row_in_file を sorted(key) ベースで再付与し、決定性を保つ。"""
    if key not in gdf.columns:
        return gdf
    gdf = gdf[gdf[key].notna()].copy()
    dissolved = gdf.dissolve(by=key, aggfunc="first").reset_index()
    dissolved = dissolved.sort_values(by=key).reset_index(drop=True)
    dissolved["_row_in_file"] = range(len(dissolved))
    dissolved["_source_path"] = f"dissolved_by_{key}"
    dissolved["_source_file"] = f"dissolved_by_{key}"
    return dissolved


print("✅ ヘルパー関数 定義完了")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #1B3139; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥉 Bronze 地理空間 10 テーブル</h2>
# MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">geopandas で読み込み、行 ID と WKB バイナリで保持して Spark Delta に書き込み</p>
# MAGIC </div>

# COMMAND ----------

spark.sql(f"USE CATALOG {MY_CATALOG}")
spark.sql(f"USE SCHEMA {MY_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,1/10: bz_geo_zoning（用途地域 KSJ A29）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/A29")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "A29")
df = gdf_to_spark_with_row_ids(gdf)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_zoning")
print(f"✅ bz_geo_zoning: {spark.table('bz_geo_zoning').count():,} 行")

# COMMAND ----------

# DBTITLE 1,2/10: bz_geo_flood（洪水浸水想定 KSJ A31）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/A31")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "A31")
df = gdf_to_spark_with_row_ids(gdf)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_flood")
print(f"✅ bz_geo_flood: {spark.table('bz_geo_flood').count():,} 行")

# COMMAND ----------

# DBTITLE 1,3/10: bz_geo_landslide（土砂災害警戒区域 KSJ A33）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/A33")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "A33")
df = gdf_to_spark_with_row_ids(gdf)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_landslide")
print(f"✅ bz_geo_landslide: {spark.table('bz_geo_landslide').count():,} 行")

# COMMAND ----------

# DBTITLE 1,4/10: bz_geo_landprice_l01（地価公示 KSJ L01）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/L01")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "L01")
df = gdf_to_spark_with_row_ids(gdf, add_simplified=False)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_landprice_l01")
print(f"✅ bz_geo_landprice_l01: {spark.table('bz_geo_landprice_l01').count():,} 行")

# COMMAND ----------

# DBTITLE 1,5/10: bz_geo_landprice_l02（都道府県地価調査 KSJ L02）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/L02")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "L02")
df = gdf_to_spark_with_row_ids(gdf, add_simplified=False)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_landprice_l02")
print(f"✅ bz_geo_landprice_l02: {spark.table('bz_geo_landprice_l02').count():,} 行")

# COMMAND ----------

# DBTITLE 1,6/10: bz_geo_stations（鉄道駅 KSJ N02、Point のみ抽出）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/N02")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)

# N02 は線形（LineString）と点（Point）の混在 Shapefile が複数含まれる。駅レイヤは Point のみを採用
gdf = gdf[gdf["geometry"].apply(lambda g: g is not None and g.geom_type == "Point")].reset_index(drop=True)
print(f"Point ジオメトリのみ抽出: {len(gdf)} 件")

# 7 都府県 bbox で絞り込み
TARGET_BBOX = (122.0, 33.0, 141.0, 36.5)  # minx, miny, maxx, maxy
minx, miny, maxx, maxy = TARGET_BBOX
gdf = gdf.cx[minx:maxx, miny:maxy].reset_index(drop=True)

gdf = normalize_columns(gdf, "N02")
df = gdf_to_spark_with_row_ids(gdf, add_simplified=False)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_stations")
print(f"✅ bz_geo_stations: {spark.table('bz_geo_stations').count():,} 行")

# COMMAND ----------

# DBTITLE 1,7/10: bz_geo_admin（行政区域 KSJ N03、admin_code で dissolve 済み）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/N03")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
gdf = normalize_columns(gdf, "N03")

# admin_code ごとに dissolve（島しょ・分割ポリゴンを 1 行化）
gdf = dissolve_by_key(gdf, key="admin_code")
print(f"admin_code で dissolve 後: {len(gdf)} 行")

# 代表点を Pandas 側で算出（representative_point は必ずポリゴン内に落ちる、st_pointonsurface 相当）
gdf["representative_lng"] = gdf["geometry"].apply(lambda g: g.representative_point().x if g is not None else None)
gdf["representative_lat"] = gdf["geometry"].apply(lambda g: g.representative_point().y if g is not None else None)

df = gdf_to_spark_with_row_ids(gdf)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_admin")
print(f"✅ bz_geo_admin: {spark.table('bz_geo_admin').count():,} 行")

# COMMAND ----------

# DBTITLE 1,8/10: bz_geo_pop_mesh（1km メッシュ別将来推計人口 KSJ mesh1000）
gdf = read_geo_files(f"{VOLUME_PATH}/geo/mesh1000")
gdf = to_wgs84(gdf)
gdf = validate_in_japan_bbox_strict(gdf)
# 7 都府県 bbox で絞り込み
gdf = gdf.cx[minx:maxx, miny:maxy].reset_index(drop=True)
gdf = normalize_columns(gdf, "mesh1000")
df = gdf_to_spark_with_row_ids(gdf)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_pop_mesh")
print(f"✅ bz_geo_pop_mesh: {spark.table('bz_geo_pop_mesh').count():,} 行")

# COMMAND ----------

# DBTITLE 1,9/10: bz_geo_isj（大字・町丁目位置参照情報 CSV）
isj_files = sorted(_glob.glob(f"{VOLUME_PATH}/geo/isj/*.csv"))
if not isj_files:
    raise FileNotFoundError(f"ISJ CSV が見つかりません: {VOLUME_PATH}/geo/isj/")

isj_dfs = []
for p in isj_files:
    try:
        isj_dfs.append(pd.read_csv(p, encoding="cp932"))
    except UnicodeDecodeError:
        isj_dfs.append(pd.read_csv(p, encoding="utf-8"))
isj_df = pd.concat(isj_dfs, ignore_index=True)
isj_df["_ingested_at"] = pd.Timestamp.utcnow()

spark_isj = spark.createDataFrame(isj_df)
spark_isj.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_isj")
print(f"✅ bz_geo_isj: {spark.table('bz_geo_isj').count():,} 行")

# COMMAND ----------

# DBTITLE 1,10/10: bz_geo_osm_poi（OpenStreetMap POI GeoJSON）
osm_files = sorted(_glob.glob(f"{VOLUME_PATH}/geo/osm/*.geojson"))
if not osm_files:
    raise FileNotFoundError(f"OSM GeoJSON が見つかりません: {VOLUME_PATH}/geo/osm/")

osm_root = f"{VOLUME_PATH}/geo/osm"
osm_gdfs = []
for p in osm_files:
    try:
        g = gpd.read_file(p)
        if not g.empty:
            g["_source_path"] = os.path.relpath(p, osm_root)
            g["_source_file"] = os.path.basename(p)
            g["_row_in_file"] = range(len(g))
            osm_gdfs.append(g)
    except Exception as e:
        print(f"⚠ OSM 読み込み失敗: {p}: {e}")

if not osm_gdfs:
    raise RuntimeError(
        f"OSM GeoJSON が全件読み込み失敗しました: {VOLUME_PATH}/geo/osm/\n"
        f"01 の Overpass 取得を再実行してください。"
    )

osm_gdf = gpd.GeoDataFrame(pd.concat(osm_gdfs, ignore_index=True), crs="EPSG:4326")
osm_gdf = validate_in_japan_bbox_strict(osm_gdf)
osm_gdf = osm_gdf[osm_gdf["geometry"].apply(lambda g: g is not None and g.geom_type == "Point")].reset_index(drop=True)
df = gdf_to_spark_with_row_ids(osm_gdf, add_simplified=False)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bz_geo_osm_poi")
print(f"✅ bz_geo_osm_poi: {spark.table('bz_geo_osm_poi').count():,} 行")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="background: #455A64; color: #FFFFFF; padding: 14px 20px; border-radius: 6px; margin: 20px 0 10px 0;">
# MAGIC <h2 style="margin: 0; color: #FFFFFF; font-size: 20px;">🥈 Silver 地理空間 8 テーブル（H3 r9 候補抽出列 + ジオメトリ保持）</h2>
# MAGIC <p style="margin: 4px 0 0 0; color: #B0BEC5; font-size: 13px;">polyfill は原典 geom_wkb で実施。簡略化 geom_wkb_simplified は表示・軽量 JOIN 用に併存</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #F57C00; background: #FFF3E0; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>💡 H3 候補抽出の fallback 戦略</strong><br>
# MAGIC ポリゴンを <code>h3_polyfillash3()</code> で H3 r9 セル ID 配列に展開。<br>
# MAGIC 配列が空または少数（< 2）になった細長いポリゴンには <code>polyfill_fallback=true</code> フラグを立て、NB 05 の Geo JOIN で bbox-only 経路に回すための目印にします。<br>
# MAGIC <strong>論理 PK</strong>：Silver 地理空間の主キー（<code>(zoning_id, h3_r9)</code> 等）は README 上の論理キーであり、UC 制約は付けません（h3_r9 が NULL になる fallback 行と PK NOT NULL 要件が両立しないため）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,1/8: sl_geo_zoning（用途地域、H3 r9 展開 + fallback フラグ）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_zoning AS
WITH cells AS (
  SELECT
    row_id AS zoning_id,
    zoning_code,
    zoning_name,
    geom_wkb,
    geom_wkb_simplified,
    h3_polyfillash3(geom_wkb, 9) AS h3_cells,
    _source_file,
    _ingested_at
  FROM bz_geo_zoning
)
SELECT
  zoning_id,
  zoning_code,
  zoning_name,
  geom_wkb,
  geom_wkb_simplified,
  COALESCE(SIZE(h3_cells), 0) < 2 AS polyfill_fallback,
  EXPLODE_OUTER(h3_cells) AS h3_r9,
  _source_file,
  _ingested_at
FROM cells
""")
print(f"✅ sl_geo_zoning: {spark.table('sl_geo_zoning').count():,} 行")

# COMMAND ----------

# DBTITLE 1,2/8: sl_geo_flood（洪水浸水想定、H3 r9 展開 + fallback フラグ）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_flood AS
WITH cells AS (
  SELECT
    row_id AS flood_id,
    flood_depth_class,
    duration_class,
    geom_wkb,
    geom_wkb_simplified,
    h3_polyfillash3(geom_wkb, 9) AS h3_cells,
    _source_file,
    _ingested_at
  FROM bz_geo_flood
)
SELECT
  flood_id,
  flood_depth_class,
  duration_class,
  geom_wkb,
  geom_wkb_simplified,
  COALESCE(SIZE(h3_cells), 0) < 2 AS polyfill_fallback,
  EXPLODE_OUTER(h3_cells) AS h3_r9,
  _source_file,
  _ingested_at
FROM cells
""")
print(f"✅ sl_geo_flood: {spark.table('sl_geo_flood').count():,} 行")

# COMMAND ----------

# DBTITLE 1,3/8: sl_geo_landslide（土砂災害警戒区域、H3 r9 展開 + fallback フラグ）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_landslide AS
WITH cells AS (
  SELECT
    row_id AS landslide_id,
    hazard_type,
    hazard_grade,
    geom_wkb,
    geom_wkb_simplified,
    h3_polyfillash3(geom_wkb, 9) AS h3_cells,
    _source_file,
    _ingested_at
  FROM bz_geo_landslide
)
SELECT
  landslide_id,
  hazard_type,
  hazard_grade,
  geom_wkb,
  geom_wkb_simplified,
  COALESCE(SIZE(h3_cells), 0) < 2 AS polyfill_fallback,
  EXPLODE_OUTER(h3_cells) AS h3_r9,
  _source_file,
  _ingested_at
FROM cells
""")
print(f"✅ sl_geo_landslide: {spark.table('sl_geo_landslide').count():,} 行")

# COMMAND ----------

# DBTITLE 1,4/8: sl_geo_landprice（L01 + L02 統合、Point から H3 r9 を付与）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_landprice AS
WITH unified AS (
  SELECT
    row_id AS landprice_id,
    'L01' AS source,
    CAST(year  AS INT)    AS year,
    CAST(price AS BIGINT) AS price,
    geom_wkb,
    _source_file,
    _ingested_at
  FROM bz_geo_landprice_l01
  UNION ALL
  SELECT
    row_id AS landprice_id,
    'L02' AS source,
    CAST(year  AS INT)    AS year,
    CAST(price AS BIGINT) AS price,
    geom_wkb,
    _source_file,
    _ingested_at
  FROM bz_geo_landprice_l02
)
SELECT
  landprice_id,
  source,
  year,
  price,
  geom_wkb,
  h3_longlatash3(
    st_x(st_geomfromwkb(geom_wkb)),
    st_y(st_geomfromwkb(geom_wkb)),
    9
  ) AS h3_r9,
  _source_file,
  _ingested_at
FROM unified
""")
print(f"✅ sl_geo_landprice: {spark.table('sl_geo_landprice').count():,} 行")

# COMMAND ----------

# DBTITLE 1,5/8: sl_geo_stations（鉄道駅、Point から H3 r9 を付与）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_stations AS
SELECT
  row_id AS station_id,
  station_name,
  line_name,
  operator,
  geom_wkb,
  st_x(st_geomfromwkb(geom_wkb)) AS lng,
  st_y(st_geomfromwkb(geom_wkb)) AS lat,
  h3_longlatash3(
    st_x(st_geomfromwkb(geom_wkb)),
    st_y(st_geomfromwkb(geom_wkb)),
    9
  ) AS h3_r9,
  _source_file,
  _ingested_at
FROM bz_geo_stations
""")
print(f"✅ sl_geo_stations: {spark.table('sl_geo_stations').count():,} 行")

# COMMAND ----------

# DBTITLE 1,6/8: sl_geo_admin（行政区域、Bronze で dissolve 済み、H3 r6 配列 + representative_lat/lng）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_admin AS
SELECT
  admin_code,
  admin_name,
  row_id AS admin_internal_id,
  geom_wkb,
  geom_wkb_simplified,
  h3_polyfillash3(geom_wkb, 6) AS h3_r6_array,
  representative_lat,
  representative_lng,
  _source_file,
  _ingested_at
FROM bz_geo_admin
WHERE admin_code IS NOT NULL
""")
print(f"✅ sl_geo_admin: {spark.table('sl_geo_admin').count():,} 行")

# COMMAND ----------

# DBTITLE 1,7/8: sl_geo_pop_mesh（メッシュ人口、H3 r8 展開 + year + simplified 保持）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_pop_mesh AS
WITH cells AS (
  SELECT
    row_id AS mesh_pk,
    mesh_id,
    CAST(year AS INT) AS year,
    CAST(pop_total AS DOUBLE) AS pop_total,
    geom_wkb,
    geom_wkb_simplified,
    h3_polyfillash3(geom_wkb, 8) AS h3_cells,
    _source_file,
    _ingested_at
  FROM bz_geo_pop_mesh
)
SELECT
  mesh_pk,
  mesh_id,
  year,
  pop_total,
  geom_wkb,
  geom_wkb_simplified,
  COALESCE(SIZE(h3_cells), 0) < 2 AS polyfill_fallback,
  EXPLODE_OUTER(h3_cells) AS h3_r8,
  _source_file,
  _ingested_at
FROM cells
""")
print(f"✅ sl_geo_pop_mesh: {spark.table('sl_geo_pop_mesh').count():,} 行")

# COMMAND ----------

# DBTITLE 1,8/8: sl_geo_osm_poi（OSM POI、Point から H3 r9 を付与）
spark.sql("""
CREATE OR REPLACE TABLE sl_geo_osm_poi AS
SELECT
  row_id AS poi_id,
  geom_wkb,
  st_x(st_geomfromwkb(geom_wkb)) AS lng,
  st_y(st_geomfromwkb(geom_wkb)) AS lat,
  h3_longlatash3(
    st_x(st_geomfromwkb(geom_wkb)),
    st_y(st_geomfromwkb(geom_wkb)),
    9
  ) AS h3_r9,
  _source_file,
  _ingested_at
FROM bz_geo_osm_poi
""")
print(f"✅ sl_geo_osm_poi: {spark.table('sl_geo_osm_poi').count():,} 行")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ 地理空間データパイプライン 完了</strong><br>
# MAGIC Bronze 地理空間 10 / Silver 地理空間 8 のテーブルが作成されました。<br>
# MAGIC NB 05 で Silver 地理空間と <code>sl_properties</code> を Geo JOIN し、<code>sl_property_geo_enriched</code> を生成します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Silver 地理空間 テーブル件数サマリ
silver_tables = [
    "sl_geo_zoning", "sl_geo_flood", "sl_geo_landslide", "sl_geo_landprice",
    "sl_geo_stations", "sl_geo_admin", "sl_geo_pop_mesh", "sl_geo_osm_poi",
]
print("=== Silver 地理空間 サマリ ===")
for t in silver_tables:
    n = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.{t}").count()
    print(f"  {t:25s}: {n:>10,} 行")

# fallback 件数の確認
print("\n=== H3 polyfill fallback 件数（ポリゴン側で 0〜1 セルしか得られなかったレコード）===")
for t in ["sl_geo_zoning", "sl_geo_flood", "sl_geo_landslide", "sl_geo_pop_mesh"]:
    n_fb = spark.sql(f"SELECT COUNT(*) FROM {MY_CATALOG}.{MY_SCHEMA}.{t} WHERE polyfill_fallback = true").collect()[0][0]
    print(f"  {t:25s}: {n_fb:>10,} 件（NB 05 で bbox-only fallback 経路に回す）")
