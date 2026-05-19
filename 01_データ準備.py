# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 01 | データ準備
# MAGIC <div style="background: linear-gradient(135deg, #1B3139 0%, #2D4A54 100%); padding: 20px 30px; border-radius: 10px; margin-bottom: 15px;">
# MAGIC   <div style="display: flex; align-items: center;">
# MAGIC     <div>
# MAGIC       <p style="color: #B0BEC5; margin: 5px 0 0 0;">不動産仲介 E2E デモ</p>
# MAGIC     </div>
# MAGIC     <div style="margin-left: auto;">
# MAGIC       <span style="background: rgba(255,255,255,0.15); color: #FFFFFF; padding: 4px 12px; border-radius: 20px; font-size: 13px;">⏱ 30 min</span>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>🎯 このノートブックのゴール</strong><br>
# MAGIC E2E デモで使用するすべてのデータを Volume <code>raw_data</code> に配置します。<br>
# MAGIC <ul style="margin-top: 8px;">
# MAGIC   <li><b>01-A</b>: reinfolib API（不動産取引価格情報）取得 ※未登録時は SAFE_MODE で合成のみ</li>
# MAGIC   <li><b>01-B</b>: 国土数値情報（KSJ） ZIP ダウンロード（A29/A31/A33/L01/L02/N02/N03/mesh1000）</li>
# MAGIC   <li><b>01-C</b>: 位置参照情報（ISJ） ダウンロード（大字・町丁目）</li>
# MAGIC   <li><b>01-D</b>: OpenStreetMap（OSM） POI 取得（Overpass API → GeoJSON 変換）</li>
# MAGIC   <li><b>01-E</b>: 合成データ生成（営業所 30 / 物件 3,000 / 顧客 2,500 / 内見 12,500 / 成約 5,000 / 市況指標）</li>
# MAGIC   <li><b>01-F</b>: PDF（重要事項説明書 5 + 物件パンフ 5）の Volume 配置確認 ※事前手動配置</li>
# MAGIC   <li><b>01-G</b>: MP3（接客録音 5〜10 件）の Volume 配置確認 ※事前手動配置</li>
# MAGIC </ul>
# MAGIC <strong>⚠ URL に関する注意</strong>: KSJ / ISJ の URL は年次・バージョンで頻繁に変わります。本 NB 内の URL は本 README 作成時点の推測値で、実行前に <a href="https://nlftp.mlit.go.jp/ksj/index.html">KSJ 公式</a> / <a href="https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi">ISJ 公式</a> で最新版を確認してください（コード中の <code>TODO(URL)</code> 印を参照）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ライブラリインストール
# MAGIC %pip install --quiet geopandas==0.14.4 shapely==2.0.4 pyproj==3.6.1 requests==2.32.3 fiona==1.9.6 h3==4.1.0 faker==25.8.0 tqdm==4.66.4

# COMMAND ----------

# DBTITLE 1,Python 再起動
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,共通設定の読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,ライブラリインポート・シード固定
import os
import io
import json
import zipfile
import shutil
import random
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import Point
import pyproj
from tqdm import tqdm
from faker import Faker

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("ja_JP")
Faker.seed(SEED)

# キャッシュ制御：Volume に既存ファイルがあれば外部 API/DL を skip。True で強制再 DL
FORCE_REFRESH = False


def has_cached_files(volume_path: str, min_files: int = 1, extensions: Optional[Tuple[str, ...]] = None) -> bool:
    """Volume 内の対象パスに min_files 以上のファイル（指定拡張子）があれば True"""
    try:
        files = [f for f in dbutils.fs.ls(volume_path) if f.isFile()]
        if extensions:
            files = [f for f in files if any(f.name.endswith(e) for e in extensions)]
        return len(files) >= min_files
    except Exception:
        return False


def should_fetch(volume_path: str, min_files: int = 1, extensions: Optional[Tuple[str, ...]] = None) -> bool:
    """FORCE_REFRESH=True または キャッシュ無しなら True（取得が必要）"""
    if FORCE_REFRESH:
        return True
    return not has_cached_files(volume_path, min_files, extensions)


print(f"geopandas    : {gpd.__version__}")
print(f"pyproj       : {pyproj.__version__}")
print(f"FORCE_REFRESH: {FORCE_REFRESH}（True で外部 DL を強制再実行）")

# COMMAND ----------

# DBTITLE 1,ヘルパー関数（西暦変換 / safe_int / ZIP 検証）
def parse_japanese_year(s) -> Optional[int]:
    """「昭和60年」「平成5年」「令和2年」「2022年」等を西暦に変換"""
    if not isinstance(s, str):
        return None
    s = s.strip()
    try:
        if s.startswith("昭和"):
            return 1925 + int(s[2:].replace("年", ""))
        if s.startswith("平成"):
            return 1988 + int(s[2:].replace("年", ""))
        if s.startswith("令和"):
            return 2018 + int(s[2:].replace("年", ""))
        if s.endswith("年"):
            return int(s[:-1])
        return int(s)
    except Exception:
        return None


def safe_int(v) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def is_valid_zip(path: str) -> bool:
    """ZIP ファイル（HTML が 200 で返るケースも含めて）の整合性チェック"""
    try:
        with zipfile.ZipFile(path) as zf:
            return zf.testzip() is None and len(zf.namelist()) > 0
    except Exception:
        return False

# COMMAND ----------

# DBTITLE 1,Volume 配下のディレクトリ構成
SUBDIRS = [
    "",                  # ルート（CSV 配置）
    "inquiries",
    "contracts",
    "geo",
    "geo/A29",           # 用途地域
    "geo/A31",           # 洪水浸水想定
    "geo/A33",           # 土砂災害警戒区域
    "geo/L01",           # 地価公示
    "geo/L02",           # 都道府県地価調査
    "geo/N02",           # 鉄道駅
    "geo/N03",           # 行政区域
    "geo/mesh1000",      # 1km メッシュ別将来推計人口
    "geo/isj",           # 大字・町丁目位置参照情報
    "geo/osm",           # OSM POI（GeoJSON）
    "pdf",
    "audio",
]

for sub in SUBDIRS:
    path = f"{VOLUME_PATH}/{sub}" if sub else VOLUME_PATH
    dbutils.fs.mkdirs(path)

print(f"✅ Volume 配下のディレクトリ {len(SUBDIRS)} 個を確保")
print(f"   ベース: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,既存ファイルのクリア（再実行用）
def clear_volume():
    """Volume 配下の生成データを削除（PDF / MP3 は除外）"""
    for sub in SUBDIRS:
        if sub in ("pdf", "audio"):
            continue
        path = f"{VOLUME_PATH}/{sub}" if sub else VOLUME_PATH
        try:
            for f in dbutils.fs.ls(path):
                if f.isFile():
                    dbutils.fs.rm(f.path)
        except Exception:
            pass
    print("✅ 再実行用に既存ファイルをクリア（pdf/ と audio/ は保持）")

# 再実行時のみコメントアウトを外して実行
# clear_volume()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🌐 01-A. reinfolib API（不動産取引価格情報）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 国土交通省「不動産情報ライブラリ」公開 API から取引価格情報を取得。<br>
# MAGIC API キーが <b>Secret 未登録時は SAFE_MODE</b> に切替わり、<b>reinfolib のみ完全合成</b>で物件 3,000 件を保証します。<br>
# MAGIC KSJ / ISJ / OSM はネット取得が必要で、SAFE_MODE 中でも 01-B〜01-D で取得失敗時はエラー停止します（必須データのため）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,reinfolib API キー取得（リポジトリにキャッシュがあれば API キー無しでも進める）
SAFE_MODE = False
REINFOLIB_API_KEY: Optional[str] = None
try:
    REINFOLIB_API_KEY = dbutils.secrets.get(scope=REINFOLIB_SECRET_SCOPE, key=REINFOLIB_SECRET_KEY)
    print(f"✅ reinfolib API キーを取得しました（scope={REINFOLIB_SECRET_SCOPE}）")
except Exception as e:
    # キャッシュ（リポジトリ同梱 → Volume 配置済み）があれば API キー不要で続行
    cache_has_data = has_cached_files(f"{VOLUME_PATH}/reinfolib_cache", min_files=1, extensions=(".json",))
    if cache_has_data:
        print(f"⚠ reinfolib Secret 未登録ですが、Volume にキャッシュ済み JSON があるため API キー無しで続行")
    else:
        SAFE_MODE = True
        print(f"⚠ reinfolib Secret 未登録 + キャッシュも無し → SAFE_MODE（合成データのみで物件マスタを生成）")
        print(f"   解決策 1: databricks secrets put-secret --scope {REINFOLIB_SECRET_SCOPE} --key {REINFOLIB_SECRET_KEY}")
        print(f"   解決策 2: data/external/reinfolib/ に取得済み JSON を配置（PDL 1.0 / 出典：国土交通省 不動産情報ライブラリ）")

# COMMAND ----------

# DBTITLE 1,リポジトリ同梱の reinfolib JSON を Volume へ同期（API キー不要の利用者向け）
# 設計：
#   1. リポジトリ data/external/reinfolib/ に取得済み JSON があれば、Volume へコピーして即利用
#   2. リポジトリにも Volume にも無いものだけ、API キーで取得
#   3. API キーが Secret 未登録 + データも無い場合は SAFE_MODE で合成データのみ
#
# 出典：国土交通省 不動産情報ライブラリ（https://www.reinfolib.mlit.go.jp/）
# ライセンス：PDL 1.0、出典明示で再配布可
import os as _os

# リポジトリパス（NB 実行時のカレントは Workspace 上だが、相対パス推測のため複数候補を試す）
REPO_REINFOLIB_CANDIDATES = [
    "/Workspace/Repos/" + (_os.environ.get("USER") or "") + "/e2e_de_car_sales/sample/e2e_de_real_estate/data/external/reinfolib",
    "/Workspace/Users/" + (_os.environ.get("USER") or "") + "/e2e_de_real_estate/data/external/reinfolib",
    # ローカル開発時の相対パス
    "./data/external/reinfolib",
    "../data/external/reinfolib",
    "./sample/e2e_de_real_estate/data/external/reinfolib",
]

def _find_repo_reinfolib_dir() -> Optional[str]:
    for p in REPO_REINFOLIB_CANDIDATES:
        if _os.path.isdir(p) and any(f.endswith(".json") for f in _os.listdir(p)):
            return p
    return None


def sync_repo_reinfolib_to_volume() -> int:
    """リポジトリの data/external/reinfolib/*.json を Volume の reinfolib_cache/ にコピー。コピー件数を返す"""
    src_dir = _find_repo_reinfolib_dir()
    if not src_dir:
        print("   リポジトリに reinfolib JSON 無し（API 取得 or SAFE_MODE に fallback）")
        return 0
    dst_dir = f"{VOLUME_PATH}/reinfolib_cache"
    dbutils.fs.mkdirs(dst_dir)
    copied = 0
    for fn in sorted(_os.listdir(src_dir)):
        if not fn.endswith(".json"):
            continue
        src = _os.path.join(src_dir, fn)
        dst = f"{dst_dir}/{fn}"
        # FORCE_REFRESH=True なら上書き、それ以外は存在チェック
        try:
            existing = any(f.name == fn for f in dbutils.fs.ls(dst_dir) if f.isFile())
        except Exception:
            existing = False
        if existing and not FORCE_REFRESH:
            continue
        with open(src, "r", encoding="utf-8") as f:
            content = f.read()
        dbutils.fs.put(dst, content, overwrite=True)
        copied += 1
    print(f"   リポジトリ {src_dir} → Volume {dst_dir}: {copied} 件コピー（PDL 1.0 / 出典：国土交通省 不動産情報ライブラリ）")
    return copied


sync_repo_reinfolib_to_volume()


# DBTITLE 1,リポジトリ同梱の OSM GeoJSON を Volume へ同期（API キー不要、ODbL）
# 出典：© OpenStreetMap contributors（ODbL）
REPO_OSM_CANDIDATES = [
    "/Workspace/Repos/" + (_os.environ.get("USER") or "") + "/e2e_de_car_sales/sample/e2e_de_real_estate/data/external/osm",
    "/Workspace/Users/" + (_os.environ.get("USER") or "") + "/e2e_de_real_estate/data/external/osm",
    "./data/external/osm",
    "../data/external/osm",
    "./sample/e2e_de_real_estate/data/external/osm",
]


def _find_repo_osm_dir() -> Optional[str]:
    for p in REPO_OSM_CANDIDATES:
        if _os.path.isdir(p) and any(f.endswith(".geojson") for f in _os.listdir(p)):
            return p
    return None


def sync_repo_osm_to_volume() -> int:
    """リポジトリの data/external/osm/*.geojson を Volume の geo/osm/ にコピー"""
    src_dir = _find_repo_osm_dir()
    if not src_dir:
        print("   リポジトリに OSM GeoJSON 無し（Overpass API 取得 or エラー fallback）")
        return 0
    dst_dir = f"{VOLUME_PATH}/geo/osm"
    dbutils.fs.mkdirs(dst_dir)
    copied = 0
    for fn in sorted(_os.listdir(src_dir)):
        if not fn.endswith(".geojson"):
            continue
        src = _os.path.join(src_dir, fn)
        dst = f"{dst_dir}/{fn}"
        try:
            existing = any(f.name == fn for f in dbutils.fs.ls(dst_dir) if f.isFile())
        except Exception:
            existing = False
        if existing and not FORCE_REFRESH:
            continue
        with open(src, "r", encoding="utf-8") as f:
            content = f.read()
        dbutils.fs.put(dst, content, overwrite=True)
        copied += 1
    print(f"   リポジトリ {src_dir} → Volume {dst_dir}: {copied} 件コピー（ODbL / 出典：© OpenStreetMap contributors）")
    return copied


sync_repo_osm_to_volume()

# DBTITLE 1,reinfolib API（リポジトリにも Volume にも無いデータだけ取得）
# 仕様: https://www.reinfolib.mlit.go.jp/help/apiManual/xit001/
# 重要: priceClassification=01（取引価格情報のみ）に絞り、成約価格情報を混入させない
REINFOLIB_ENDPOINT = "https://www.reinfolib.mlit.go.jp/ex-api/external/XIT001"

def fetch_reinfolib(year: int, quarter: int, area_code: str) -> pd.DataFrame:
    """指定四半期・都道府県の取引価格情報を取得"""
    headers = {"Ocp-Apim-Subscription-Key": REINFOLIB_API_KEY}
    params = {
        "year": year,
        "quarter": quarter,
        "area": area_code,
        "priceClassification": "01",   # 取引価格情報のみ（成約価格情報を除外）
    }
    r = requests.get(REINFOLIB_ENDPOINT, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data.get("data", []))


def collect_transactions() -> pd.DataFrame:
    """reinfolib から取引価格情報を取得。Volume にキャッシュ JSON があれば再 DL をスキップ。
    SAFE_MODE（API キー無し + キャッシュ無し）の場合は空 DF を返す"""
    if SAFE_MODE:
        return pd.DataFrame()
    # API キーが無いがキャッシュはある場合、API 呼び出しはスキップしてキャッシュのみ読み込む
    api_disabled = (REINFOLIB_API_KEY is None)
    today = dt.date.today()
    quarters = []
    for offset in range(8):
        q_date = (today.replace(day=1) - dt.timedelta(days=offset * 90))
        y = q_date.year
        q = (q_date.month - 1) // 3 + 1
        quarters.append((y, q))
    quarters = sorted(set(quarters))

    # Volume キャッシュディレクトリ
    cache_dir = f"{VOLUME_PATH}/reinfolib_cache"
    dbutils.fs.mkdirs(cache_dir)

    frames = []
    n_cached = 0
    n_fetched = 0
    for pref_code in TARGET_PREF_CODES:
        for (y, q) in quarters:
            cache_path = f"{cache_dir}/{y}_Q{q}_{pref_code}.json"
            # 既存キャッシュチェック（FORCE_REFRESH 時はスキップ）
            cache_hit = (not FORCE_REFRESH) and any(
                f.name == f"{y}_Q{q}_{pref_code}.json" for f in dbutils.fs.ls(cache_dir) if f.isFile()
            )
            if cache_hit:
                try:
                    cached = json.loads(dbutils.fs.head(cache_path, max_bytes=50 * 1024 * 1024))
                    df = pd.DataFrame(cached.get("data", []))
                    if not df.empty:
                        df["pref_code"] = pref_code
                        frames.append(df)
                    n_cached += 1
                    continue
                except Exception as e:
                    print(f"⚠ キャッシュ読込失敗、API 再取得: {cache_path}: {e}")
            # キャッシュ無し + API キー無し なら API 呼出をスキップ
            if api_disabled:
                continue
            try:
                df = fetch_reinfolib(y, q, pref_code)
                # キャッシュ保存（{"data": [...]} 形式）
                dbutils.fs.put(cache_path, json.dumps({"data": df.to_dict(orient="records")}, ensure_ascii=False), overwrite=True)
                n_fetched += 1
                if not df.empty:
                    df["pref_code"] = pref_code
                    frames.append(df)
            except Exception as e:
                print(f"⚠ reinfolib 取得失敗: pref={pref_code}, {y}Q{q}: {e}")
    print(f"   reinfolib: キャッシュヒット {n_cached}, 新規 DL {n_fetched}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


tx_df = collect_transactions()

# 住宅地のみに絞る
HOUSING_TYPES = {"中古マンション等", "宅地(土地と建物)", "宅地(土地)"}
if not tx_df.empty and "Type" in tx_df.columns:
    tx_df = tx_df[tx_df["Type"].isin(HOUSING_TYPES)].reset_index(drop=True)

print(f"✅ reinfolib 取引データ: {len(tx_df):,} 件取得（SAFE_MODE={SAFE_MODE}）")
if not tx_df.empty:
    tx_df.head()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🗺️ 01-B. 国土数値情報（KSJ） ZIP ダウンロード
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC A29 / A31 / A33 / L01 / L02 / N02 / N03 / mesh1000 を 7 都府県分（一部全国）ダウンロード。<br>
# MAGIC <b>形式は Shapefile / GML が混在</b>します（KSJ のデータセットごと）。後工程の NB 03 で <code>geopandas</code> を使い拡張子に応じて読み込みます。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,KSJ ダウンロード URL マッピング（要確認）
# TODO(URL): KSJ 公式 https://nlftp.mlit.go.jp/ksj/index.html で各データセットの最新版 URL を確認し、必要に応じて修正してください。
# 注意: KSJ はデータセットごとに年度・バージョン番号・per-pref / 全国版が異なります。下記は本 README 作成時点の推測値。
KSJ_BASE = "https://nlftp.mlit.go.jp/ksj/gml/data"

KSJ_DATASETS = {
    "A29":      {"url_tmpl": f"{KSJ_BASE}/A29/A29-19/A29-19_{{pref}}_GML.zip",       "per_pref": True},
    "A31":      {"url_tmpl": f"{KSJ_BASE}/A31/A31-12/A31-12_{{pref}}_GML.zip",       "per_pref": True},
    "A33":      {"url_tmpl": f"{KSJ_BASE}/A33/A33-23/A33-23_{{pref}}_GML.zip",       "per_pref": True},
    "L01":      {"url_tmpl": f"{KSJ_BASE}/L01/L01-24/L01-24_{{pref}}_GML.zip",       "per_pref": True},
    "L02":      {"url_tmpl": f"{KSJ_BASE}/L02/L02-24/L02-24_{{pref}}_GML.zip",       "per_pref": True},
    "N02":      {"url_tmpl": f"{KSJ_BASE}/N02/N02-23/N02-23_GML.zip",                "per_pref": False},
    "N03":      {"url_tmpl": f"{KSJ_BASE}/N03/N03-2024/N03-20240101_{{pref}}_GML.zip", "per_pref": True},
    "mesh1000": {"url_tmpl": f"{KSJ_BASE}/mesh1000/mesh1000.zip",                    "per_pref": False},
}

# Geo データの欠落許容性
# - 必須（欠落時にエラー）: A29 / A31 / A33 / L01 / N03（用途地域・ハザード・地価・行政区域）
# - 任意（欠落時に警告のみ）: L02 / N02 / mesh1000
REQUIRED_KSJ_DATASETS = {"A29", "A31", "A33", "L01", "N03"}

# COMMAND ----------

# DBTITLE 1,KSJ ダウンロード共通関数
def download_zip(url: str, dest_zip: str, *, timeout: int = 180) -> bool:
    """URL から ZIP をダウンロード。HTTP 200 + 中身が有効な ZIP の場合のみ True"""
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        if r.status_code != 200:
            print(f"⚠ {url} → status {r.status_code}")
            return False
        ctype = r.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            print(f"⚠ {url} → HTML が返ってきました（URL 要確認）")
            return False
        with open(dest_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        if not is_valid_zip(dest_zip):
            print(f"⚠ {url} → ZIP が壊れているか空です")
            return False
        return True
    except Exception as e:
        print(f"⚠ ダウンロード失敗: {url}: {e}")
        return False


def extract_zip_to_volume(zip_path: str, target_dir: str) -> int:
    """ZIP を解凍して target_dir 配下に配置。Shapefile/GML の本体・兄弟ファイル（.shp/.shx/.dbf/.prj/.xml/.gml/.geojson/.cpg 等）を残し、ドキュメント類（.pdf/.txt）は除外"""
    count = 0
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith("/"):
                continue
            # ドキュメント類は除外（メタデータ XML は残す）
            if name.endswith((".pdf", ".txt")) and not name.endswith(("metadata.xml",)):
                continue
            data = zf.read(name)
            out_name = os.path.basename(name)
            if not out_name:
                continue
            with open(f"{target_dir}/{out_name}", "wb") as f:
                f.write(data)
            count += 1
    return count


def fetch_ksj(dataset: str, cfg: dict) -> int:
    """KSJ データセット 1 種を DL → Volume 配置。既存ファイルがあれば skip。成功 pref 数を返す"""
    out_dir = f"{VOLUME_PATH}/geo/{dataset}"
    expected_extensions = (".shp", ".gml", ".xml", ".geojson")

    if cfg["per_pref"]:
        targets = [(pref, cfg["url_tmpl"].format(pref=pref)) for pref in TARGET_PREF_CODES]
    else:
        targets = [("all", cfg["url_tmpl"])]

    # 全 pref が揃っているならスキップ
    if not should_fetch(out_dir, min_files=len(targets), extensions=expected_extensions):
        print(f"   {dataset}: Volume キャッシュ済み（FORCE_REFRESH=False のため skip）")
        return len(targets)

    tmp_root = f"/tmp/ksj_{dataset}"
    os.makedirs(tmp_root, exist_ok=True)
    success = 0
    for label, url in tqdm(targets, desc=f"KSJ {dataset}"):
        zip_path = f"{tmp_root}/{label}.zip"
        if not download_zip(url, zip_path):
            continue
        n = extract_zip_to_volume(zip_path, out_dir)
        if n > 0:
            success += 1
    shutil.rmtree(tmp_root, ignore_errors=True)
    return success


# 実行 & 必須データ欠落チェック
ksj_results: Dict[str, int] = {}
for ds_name, ds_cfg in KSJ_DATASETS.items():
    ksj_results[ds_name] = fetch_ksj(ds_name, ds_cfg)

print("\n--- KSJ ダウンロード結果 ---")
missing_required = []
n_prefs = len(TARGET_PREF_CODES)
for ds, n in ksj_results.items():
    cfg = KSJ_DATASETS[ds]
    expected = n_prefs if cfg["per_pref"] else 1
    status = "✅" if n == expected else ("⚠" if n > 0 else "❌")
    print(f"  {status} {ds}: {n}/{expected} 成功")
    # 必須データは per-pref 完全性（全都府県成功）を要求
    if ds in REQUIRED_KSJ_DATASETS and n < expected:
        missing_required.append(f"{ds}({n}/{expected})")

if missing_required:
    raise RuntimeError(
        f"必須 KSJ データの取得が完全ではありません: {missing_required}\n"
        f"→ KSJ 公式サイトで最新 URL を確認し、KSJ_DATASETS の url_tmpl を修正してください。\n"
        f"  per-pref 必須データセットは {sorted(REQUIRED_KSJ_DATASETS)} で、全 {n_prefs} 都府県分の取得が必要です。"
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📍 01-C. 位置参照情報（ISJ）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 大字・町丁目位置参照情報を 7 都府県分ダウンロード。物件住所のジオコーディングに使用。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,ISJ ダウンロード
# TODO(URL): ISJ 公式 https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi で最新バージョンを確認
# 注意: ISJ_VERSION は公開時点の値。最新バージョンに合わせて要修正。
ISJ_VERSION = "20.0b"
ISJ_BASE = "https://nlftp.mlit.go.jp/isj/dls/data"
ISJ_URL_TMPL = f"{ISJ_BASE}/{ISJ_VERSION}/{{pref}}000-{ISJ_VERSION}.zip"

isj_out = f"{VOLUME_PATH}/geo/isj"

# 既存キャッシュチェック：7 都府県分の CSV が揃っていれば skip
if not should_fetch(isj_out, min_files=len(TARGET_PREF_CODES), extensions=(".csv",)):
    print(f"✅ ISJ CSV: Volume キャッシュ済み（FORCE_REFRESH=False のため skip）")
else:
    tmp_isj = "/tmp/isj"
    os.makedirs(tmp_isj, exist_ok=True)
    isj_success = 0
    for pref in tqdm(TARGET_PREF_CODES, desc="ISJ"):
        url = ISJ_URL_TMPL.format(pref=pref)
        zip_path = f"{tmp_isj}/{pref}.zip"
        if not download_zip(url, zip_path):
            continue
        n = extract_zip_to_volume(zip_path, isj_out)
        if n > 0:
            isj_success += 1
    shutil.rmtree(tmp_isj, ignore_errors=True)

    if isj_success < len(TARGET_PREF_CODES):
        raise RuntimeError(
            f"ISJ の取得が完全ではありません: {isj_success}/{len(TARGET_PREF_CODES)} 都府県。"
            f"ISJ_VERSION / URL を ISJ 公式サイトで確認してください。"
        )
    print(f"✅ ISJ CSV 配置完了: {isj_success}/{len(TARGET_PREF_CODES)} 都府県")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🗽 01-D. OpenStreetMap POI（Overpass API → GeoJSON）
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 7 都府県のバウンディングボックス内のコンビニ・スーパー・学校 POI を Overpass API で取得し、<b>GeoJSON FeatureCollection</b> に変換して配置します。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,OSM Overpass → GeoJSON 変換
OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter"

BBOX_TARGETS = {
    "kanto":  (34.95, 138.95, 36.30, 140.90),
    "kansai": (34.30, 135.20, 35.10, 136.10),
    "chubu":  (34.55, 136.60, 35.60, 137.85),
    "kyushu": (33.10, 130.00, 34.10, 131.10),
}

OSM_TAGS = ["shop=convenience", "shop=supermarket", "amenity=school"]


def fetch_osm_for_bbox(bbox: Tuple[float, float, float, float], tag: str) -> dict:
    s, w, n, e = bbox
    key, value = tag.split("=")
    query = f"""
    [out:json][timeout:120];
    (
      node["{key}"="{value}"]({s},{w},{n},{e});
      way["{key}"="{value}"]({s},{w},{n},{e});
      relation["{key}"="{value}"]({s},{w},{n},{e});
    );
    out center;
    """
    r = requests.post(OVERPASS_ENDPOINT, data={"data": query}, timeout=180)
    r.raise_for_status()
    return r.json()


def overpass_to_geojson(overpass: dict, tag: str) -> dict:
    """Overpass JSON を GeoJSON FeatureCollection に変換（node/way/relation の center を使用）"""
    features = []
    for el in overpass.get("elements", []):
        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lon = el.get("lon") or (el.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        props = dict(el.get("tags", {}))
        props["osm_type"] = el.get("type")
        props["osm_id"] = el.get("id")
        props["tag"] = tag
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


osm_out = f"{VOLUME_PATH}/geo/osm"
expected_osm_count = len(BBOX_TARGETS) * len(OSM_TAGS)

# 既存キャッシュチェック：region × tag の組み合わせ数の GeoJSON が揃っていれば skip
if not should_fetch(osm_out, min_files=expected_osm_count, extensions=(".geojson",)):
    print(f"✅ OSM POI: Volume キャッシュ済み（FORCE_REFRESH=False のため skip）")
else:
    osm_total = 0
    osm_failures = []
    for region, bbox in BBOX_TARGETS.items():
        for tag in OSM_TAGS:
            try:
                raw = fetch_osm_for_bbox(bbox, tag)
                geojson = overpass_to_geojson(raw, tag)
                safe_tag = tag.replace("=", "_")
                with open(f"{osm_out}/{region}_{safe_tag}.geojson", "w", encoding="utf-8") as f:
                    json.dump(geojson, f, ensure_ascii=False)
                n_feat = len(geojson["features"])
                osm_total += n_feat
                print(f"   OSM {region} / {tag} → {n_feat} features")
            except Exception as e:
                print(f"⚠ OSM 取得失敗: {region}/{tag}: {e}")
                osm_failures.append(f"{region}/{tag}")

    if osm_total == 0:
        raise RuntimeError(
            f"OSM POI の取得が完全に失敗しました（合計 0 features）。"
            f"失敗内訳: {osm_failures}\n"
            f"Overpass API のレート制限や接続障害が原因の可能性があります。時間をおいて再実行してください。"
        )
    print(f"✅ OSM POI 取得完了（合計 {osm_total} features）")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🏢 01-E. 合成データ生成
# MAGIC <div style="border-left: 4px solid #1976D2; background: #E3F2FD; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 営業所 / 物件 / 顧客 / 内見・問合せ / 成約 / 市況指標 を合成生成。<br>
# MAGIC 物件マスタは reinfolib 取引データ（01-A）を種にし、不足分は合成補完して<b>必ず 3,000 件を確保</b>します。<br>
# MAGIC 時系列範囲：<code>properties.status</code> は現況、<code>inquiries / contracts</code> は過去 24 ヶ月分の履歴。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,営業所マスタ（30 件 / 7 都府県分散）
OFFICE_TYPES = ["戸建仲介", "マンション仲介", "投資物件専門"]

# 各都府県への営業所配分（合計 30）
OFFICE_ALLOCATION_LIST = [
    ("東京都",   8),
    ("神奈川県", 5),
    ("大阪府",   5),
    ("愛知県",   4),
    ("千葉県",   3),
    ("埼玉県",   3),
    ("福岡県",   2),
]

PREF_REPRESENTATIVE = {
    "東京都":   (35.6895, 139.6917),
    "神奈川県": (35.4478, 139.6425),
    "千葉県":   (35.6074, 140.1233),
    "埼玉県":   (35.8569, 139.6489),
    "大阪府":   (34.6863, 135.5200),
    "福岡県":   (33.6064, 130.4181),
    "愛知県":   (35.1815, 136.9066),
}

# 都府県名 → コード（00_config の TARGET_PREFECTURES は code→name なので逆引き辞書を作成）
PREF_NAME_TO_CODE = {v: k for k, v in TARGET_PREFECTURES.items()}

offices = []
office_id = 1
for pref, n_offices in OFFICE_ALLOCATION_LIST:
    base_lat, base_lng = PREF_REPRESENTATIVE[pref]
    for _ in range(n_offices):
        lat = base_lat + np.random.uniform(-0.25, 0.25)
        lng = base_lng + np.random.uniform(-0.25, 0.25)
        offices.append({
            "office_id": f"OF{office_id:04d}",
            "office_name": f"{pref.replace('府', '').replace('県', '').replace('都', '')}{fake.last_name()}店",
            "office_type": random.choice(OFFICE_TYPES),
            "prefecture": pref,
            "area_code": PREF_NAME_TO_CODE[pref],
            "city": fake.city(),
            "lat": round(lat, 6),
            "lng": round(lng, 6),
        })
        office_id += 1

offices_df = pd.DataFrame(offices)
offices_df.to_csv(f"{VOLUME_PATH}/offices.csv", index=False, encoding="utf-8")
print(f"✅ 営業所マスタ {len(offices_df)} 件: {VOLUME_PATH}/offices.csv")
offices_df.head()

# COMMAND ----------

# DBTITLE 1,物件マスタ（3,000 件保証 / reinfolib 種データ + 合成補完）
PROPERTY_TYPES = {"中古マンション等": "マンション", "宅地(土地と建物)": "戸建", "宅地(土地)": "戸建"}
INSULATION_GRADES = [1, 2, 3, 4, 5, 6, 7]
ENERGY_GRADES = [1, 2, 3, 4, 5, 6]
SEISMIC_GRADES = [1, 2, 3]
TARGET_PROPERTY_COUNT = 3_000


def synth_address(pref: str, city: str, district: str) -> str:
    chome = random.randint(1, 9)
    banchi = random.randint(1, 30)
    sub = random.randint(1, 30)
    return f"{pref}{city}{district}{chome}丁目{banchi}-{sub}"


def make_property_record(idx: int, row: Optional[pd.Series], office_ids: List[str]) -> dict:
    """reinfolib 行（row）から物件レコードを生成。row=None なら完全合成"""
    if row is not None:
        pref_code = row.get("pref_code")
        pref = TARGET_PREFECTURES.get(pref_code, "東京都")
        tx_type = row.get("Type", "宅地(土地と建物)")
        prop_type = PROPERTY_TYPES.get(tx_type, "戸建")
        city = row.get("Municipality") or fake.city()
        district = row.get("DistrictName") or fake.town()
        built_year = parse_japanese_year(row.get("BuildingYear")) or random.randint(1990, 2024)
        floor_area = safe_int(row.get("Area")) or random.randint(40, 120)
        land_area = (safe_int(row.get("LandArea")) or random.randint(80, 200)) if prop_type == "戸建" else None
        layout = row.get("FloorPlan") or random.choice(["1LDK", "2LDK", "3LDK", "4LDK"])
        nearest_st = row.get("NearestStation") or fake.city()
        walk_min = safe_int(row.get("TimeToNearestStation")) or random.randint(3, 25)
        price = safe_int(row.get("TradePrice")) or random.randint(20_000_000, 80_000_000)
    else:
        # 完全合成（SAFE_MODE または reinfolib 件数不足の補完）
        pref = random.choices(
            [p for p, _ in OFFICE_ALLOCATION_LIST],
            weights=[n for _, n in OFFICE_ALLOCATION_LIST],
        )[0]
        pref_code = PREF_NAME_TO_CODE[pref]
        prop_type = random.choice(["マンション", "戸建"])
        city = fake.city()
        district = fake.town()
        built_year = random.randint(1990, 2024)
        floor_area = random.randint(40, 120)
        land_area = random.randint(80, 200) if prop_type == "戸建" else None
        layout = random.choice(["1LDK", "2LDK", "3LDK", "4LDK"])
        nearest_st = fake.city()
        walk_min = random.randint(3, 25)
        price = random.randint(20_000_000, 80_000_000)

    base_lat, base_lng = PREF_REPRESENTATIVE[pref]
    lat = base_lat + np.random.uniform(-0.4, 0.4)
    lng = base_lng + np.random.uniform(-0.4, 0.4)

    return {
        "property_id": f"PR{idx:05d}",
        "property_type": prop_type,
        "prefecture": pref,
        "area_code": pref_code,
        "city": city,
        "district": district,
        "address": synth_address(pref, city, district),
        "lat": round(lat, 6),
        "lng": round(lng, 6),
        "built_year": built_year,
        "floor_area_sqm": floor_area,
        "land_area_sqm": land_area,
        "layout": layout,
        "nearest_station": nearest_st,
        "walk_minutes": walk_min,
        "energy_grade": random.choice(ENERGY_GRADES),
        "insulation_grade": random.choice(INSULATION_GRADES),
        "seismic_grade": random.choice(SEISMIC_GRADES),
        "listing_price": price,
        "assessment_price": int(price * np.random.uniform(0.92, 1.05)),
        "listed_at": (dt.date.today() - dt.timedelta(days=random.randint(0, 180))).isoformat(),
        "status": random.choices(
            ["売出中", "商談中", "成約", "取下げ"],
            weights=[55, 20, 20, 5],
            k=1,
        )[0],
        "office_id": random.choice(office_ids),
    }


office_ids_list = offices_df["office_id"].tolist()

# 1) reinfolib 種データから物件レコード生成
seed_rows = tx_df.sample(n=min(TARGET_PROPERTY_COUNT, len(tx_df)), random_state=SEED).reset_index(drop=True) if not tx_df.empty else pd.DataFrame()
properties: List[dict] = []
for i, row in seed_rows.iterrows():
    properties.append(make_property_record(i + 1, row, office_ids_list))

# 2) 不足分は完全合成で補完して 3,000 件を保証
shortage = TARGET_PROPERTY_COUNT - len(properties)
if shortage > 0:
    print(f"   reinfolib 種データ {len(properties)} 件 + 合成補完 {shortage} 件")
    for j in range(shortage):
        properties.append(make_property_record(len(properties) + 1, None, office_ids_list))

properties_df = pd.DataFrame(properties)
assert len(properties_df) == TARGET_PROPERTY_COUNT, f"物件件数不一致: {len(properties_df)}"
properties_df.to_csv(f"{VOLUME_PATH}/properties.csv", index=False, encoding="utf-8")
print(f"✅ 物件マスタ {len(properties_df)} 件: {VOLUME_PATH}/properties.csv")
properties_df.head()

# COMMAND ----------

# DBTITLE 1,顧客マスタ（2,500 件）
LIFE_STAGES = ["単身", "DINKS", "ファミリー（未就学児）", "ファミリー（学齢期）", "シニア"]
DESIRED_TYPES = ["マンション", "戸建", "どちらでも"]
GENDER = ["男", "女"]

customers = []
for i in range(2_500):
    pref = random.choices(
        [p for p, _ in OFFICE_ALLOCATION_LIST],
        weights=[n for _, n in OFFICE_ALLOCATION_LIST],
    )[0]
    age = random.choices([25, 35, 45, 55, 65], weights=[15, 30, 25, 20, 10])[0] + random.randint(-4, 4)
    income_band = random.choice(["300-500万", "500-800万", "800-1200万", "1200-2000万", "2000万+"])
    customers.append({
        "customer_id": f"CU{i+1:05d}",
        "name": fake.name(),
        "age": age,
        "gender": random.choice(GENDER),
        "phone": fake.phone_number(),
        "residential_area": pref,
        "area_code": PREF_NAME_TO_CODE[pref],
        "registered_office_id": random.choice(office_ids_list),
        "first_contact_date": (dt.date.today() - dt.timedelta(days=random.randint(30, 24 * 30))).isoformat(),
        "annual_income_band": income_band,
        "household_composition": random.choice(["独身", "夫婦のみ", "夫婦 + 子 1", "夫婦 + 子 2", "三世代"]),
        "life_stage": random.choice(LIFE_STAGES),
        "desired_property_type": random.choice(DESIRED_TYPES),
        "budget_max": random.choice([3_000, 5_000, 8_000, 12_000, 20_000]) * 10_000,
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(f"{VOLUME_PATH}/customers.csv", index=False, encoding="utf-8")
print(f"✅ 顧客マスタ {len(customers_df)} 件: {VOLUME_PATH}/customers.csv")
customers_df.head()

# COMMAND ----------

# DBTITLE 1,内見・問合せ履歴（12,500 件、うち成約 5,000 件以上を確保 / 直近 5 日は日次分割）
# 設計：funnel_stage='成約' を 5,000 件以上確保するために、ファネル件数を最初に固定する
TOTAL_INQUIRIES = 12_500
TARGET_CONTRACTS = 5_000
FUNNEL_BUDGET = {
    "成約":  TARGET_CONTRACTS,           # 5,000
    "申込":  1_500,
    "内見":  3_000,
    "反響":  2_000,
    "失注":  TOTAL_INQUIRIES - 5_000 - 1_500 - 3_000 - 2_000,  # 1,000
}
assert sum(FUNNEL_BUDGET.values()) == TOTAL_INQUIRIES

VISIT_KINDS = ["来店", "オンライン", "内見", "電話"]
MEMO_TEMPLATES = [
    "{station} 駅徒歩 {walk}分の{layout}を希望。{income}",
    "ハザードマップ懸念あり。浸水想定の確認希望。",
    "住宅ローンは {bank} 35 年固定で試算希望。",
    "リフォーム前提で価格交渉希望。{walk}分以内マスト。",
    "{stage_note} 早期成約に意欲的。",
]

inquiries: List[dict] = []
today = dt.date.today()
prop_ids = properties_df["property_id"].tolist()
cust_ids = customers_df["customer_id"].tolist()

inq_counter = 0
recent_5_dates = [today - dt.timedelta(days=d) for d in range(5)]

for stage, n_records in FUNNEL_BUDGET.items():
    for _ in range(n_records):
        inq_counter += 1
        # 約 8% を直近 5 日に偏重させて Autoloader デモ向きに
        if random.random() < 0.08:
            inq_date = random.choice(recent_5_dates)
        else:
            inq_date = today - dt.timedelta(days=random.randint(5, 24 * 30))
        is_open = stage in ("反響", "内見", "申込")
        memo = random.choice(MEMO_TEMPLATES).format(
            station=fake.city(),
            walk=random.randint(3, 20),
            layout=random.choice(["1LDK", "2LDK", "3LDK", "4LDK"]),
            income=random.choice(["世帯年収 800 万", "世帯年収 1200 万", "世帯年収 1800 万"]),
            bank=random.choice(["A 銀行", "B 銀行", "C 銀行"]),
            stage_note=random.choice(["即決希望", "比較検討中", "競合物件あり"]),
        )
        inquiries.append({
            "inquiry_id": f"IQ{inq_counter:06d}",
            "customer_id": random.choice(cust_ids),
            "office_id": random.choice(office_ids_list),
            "property_id": random.choice(prop_ids),
            "inquiry_date": inq_date.isoformat(),
            "visit_kind": random.choice(VISIT_KINDS),
            "funnel_stage": stage,
            "status": "オープン" if is_open else "クローズ",
            "memo": memo,
        })

inquiries_df = pd.DataFrame(inquiries)
assert (inquiries_df["funnel_stage"] == "成約").sum() == TARGET_CONTRACTS

# 直近 5 日分は日次 CSV、それ以外は history.csv
recent_mask = inquiries_df["inquiry_date"].isin([d.isoformat() for d in recent_5_dates])
inquiries_df[~recent_mask].to_csv(f"{VOLUME_PATH}/inquiries/history.csv", index=False, encoding="utf-8")
for d in recent_5_dates:
    sub = inquiries_df[inquiries_df["inquiry_date"] == d.isoformat()]
    sub.to_csv(f"{VOLUME_PATH}/inquiries/{d.isoformat()}.csv", index=False, encoding="utf-8")

print(f"✅ 内見・問合せ {len(inquiries_df)} 件（成約 {TARGET_CONTRACTS} 件含む）: {VOLUME_PATH}/inquiries/")

# COMMAND ----------

# DBTITLE 1,成約（5,000 件 / funnel_stage='成約' のみから生成 / 直近 5 日は日次分割）
PAYMENT_METHODS = ["住宅ローン（35 年固定）", "住宅ローン（変動）", "現金", "ペアローン"]

# funnel_stage='成約' のみを母集団にする（FUNNEL_BUDGET で 5,000 件を保証済み）
contract_seeds = inquiries_df[inquiries_df["funnel_stage"] == "成約"].reset_index(drop=True)
assert len(contract_seeds) == TARGET_CONTRACTS

contracts = []
for i, row in contract_seeds.iterrows():
    prop = properties_df[properties_df["property_id"] == row["property_id"]].iloc[0]
    listing = int(prop["listing_price"])
    discount_pct = np.random.uniform(0.0, 0.12)
    settled_price = int(listing * (1 - discount_pct))
    inq_date = dt.date.fromisoformat(row["inquiry_date"])
    contract_date = min(inq_date + dt.timedelta(days=random.randint(7, 60)), today)
    commission = int(settled_price * 0.03 + 60_000)

    contracts.append({
        "contract_id": f"CT{i+1:06d}",
        "inquiry_id": row["inquiry_id"],
        "customer_id": row["customer_id"],
        "office_id": row["office_id"],
        "property_id": row["property_id"],
        "contract_date": contract_date.isoformat(),
        "settled_price": settled_price,
        "listing_price": listing,
        "discount_amount": listing - settled_price,
        "commission": commission,
        "payment_method": random.choice(PAYMENT_METHODS),
    })

contracts_df = pd.DataFrame(contracts)

recent_mask_c = contracts_df["contract_date"].isin([d.isoformat() for d in recent_5_dates])
contracts_df[~recent_mask_c].to_csv(f"{VOLUME_PATH}/contracts/history.csv", index=False, encoding="utf-8")
for d in recent_5_dates:
    sub = contracts_df[contracts_df["contract_date"] == d.isoformat()]
    sub.to_csv(f"{VOLUME_PATH}/contracts/{d.isoformat()}.csv", index=False, encoding="utf-8")

print(f"✅ 成約 {len(contracts_df)} 件: {VOLUME_PATH}/contracts/")

# COMMAND ----------

# DBTITLE 1,不動産市況指標（reinfolib 集計値 + KSJ L01 由来の地価指数 + 金利/建築費）
# 取引価格指数：reinfolib の四半期データから 都府県（area_code）× 物件種別 の中央値を集計し、月次に forward-fill
# 地価指数：KSJ L01 由来は NB 03 で構築。01 ではダミー値（中央 100 ± 3）。NB 03 で上書き予定
# 住宅ローン金利・建築費指数：参考公表値ベースの合成（時系列トレンド付き）

# 「過去 24 ヶ月」を保証するため、今日の月初（day=1）を基準にして 23 ヶ月前の月初から開始
this_month_start = today.replace(day=1)
months = pd.date_range(this_month_start - pd.DateOffset(months=23), this_month_start, freq="MS")
assert len(months) == 24, f"期待: 24 ヶ月, 実際: {len(months)} ヶ月"

# reinfolib 取引データから取引価格指数を集計
def build_tx_price_index() -> pd.DataFrame:
    """都府県×物件種別×月の取引価格中央値 → 100 基準の指数"""
    if tx_df.empty:
        return pd.DataFrame()
    df = tx_df.copy()
    df["TradePrice"] = df["TradePrice"].apply(safe_int)
    df = df.dropna(subset=["TradePrice", "Period"])
    # Period は "令和X年第Y四半期" 形式。和暦パースして年月に変換
    def period_to_month(p: str) -> Optional[str]:
        try:
            era_year = int(p.split("年")[0].replace("令和", "").replace("平成", "").replace("昭和", ""))
            if "令和" in p:
                year = 2018 + era_year
            elif "平成" in p:
                year = 1988 + era_year
            else:
                year = era_year
            q = int(p.split("第")[1].split("四半期")[0])
            month = (q - 1) * 3 + 1
            return f"{year:04d}-{month:02d}-01"
        except Exception:
            return None
    df["month"] = df["Period"].apply(period_to_month)
    df = df.dropna(subset=["month"])
    df["property_type"] = df["Type"].map(PROPERTY_TYPES).fillna("戸建")
    df["area_code"] = df["pref_code"]
    agg = df.groupby(["month", "area_code", "property_type"])["TradePrice"].median().reset_index()
    # 都府県×種別ごとの基準値で正規化（=100）
    bases = agg.groupby(["area_code", "property_type"])["TradePrice"].median().rename("base").reset_index()
    agg = agg.merge(bases, on=["area_code", "property_type"])
    agg["tx_price_index"] = (agg["TradePrice"] / agg["base"] * 100).round(2)
    return agg[["month", "area_code", "property_type", "tx_price_index"]]


tx_index_df = build_tx_price_index()

# 月 × 都府県 × 物件種別 の全組み合わせを生成し、tx_index がある月だけマージ
combinations = [
    {"month": m.strftime("%Y-%m-%d"), "area_code": code, "area_name": name, "property_type": pt}
    for m in months
    for code, name in TARGET_PREFECTURES.items()
    for pt in ["マンション", "戸建"]
]
market_df = pd.DataFrame(combinations)
if not tx_index_df.empty:
    market_df = market_df.merge(tx_index_df, on=["month", "area_code", "property_type"], how="left")
else:
    market_df["tx_price_index"] = np.nan

# reinfolib は四半期データなので、月次に forward-fill：area_code × property_type 単位で月昇順ソート → ffill→bfill
# transform() を使い、group 境界を越えて他グループの値が漏れ込まないようにする
market_df = market_df.sort_values(["area_code", "property_type", "month"]).reset_index(drop=True)
market_df["tx_price_index"] = (
    market_df.groupby(["area_code", "property_type"])["tx_price_index"]
    .transform(lambda s: s.ffill().bfill())
)

# それでも NaN が残る組み合わせ（reinfolib に該当グループの取引が完全に無い）は 100 ± ノイズで補完
noise_series = pd.Series(100 + np.random.normal(0, 3, size=len(market_df)), index=market_df.index)
market_df["tx_price_index"] = market_df["tx_price_index"].fillna(noise_series).round(2)

# 地価指数：KSJ L01 由来は NB 03 で構築するため、01 ではダミー値（中央 100 ± 3）
# NB 03 で sl_geo_landprice 集計値で上書き予定
market_df["land_price_index"] = (100 + np.random.normal(0, 3, size=len(market_df))).round(2)

# 住宅ローン金利（月次変動）
loan_fix_series = 1.8 + np.cumsum(np.random.normal(0, 0.02, size=len(months)))
loan_var_series = 0.6 + np.cumsum(np.random.normal(0, 0.01, size=len(months)))
build_idx_series = 100 + np.cumsum(np.random.normal(0.3, 0.5, size=len(months)))
month_to_idx = {m.strftime("%Y-%m-%d"): i for i, m in enumerate(months)}
market_df["loan_rate_fixed_35y"] = market_df["month"].map(lambda m: round(loan_fix_series[month_to_idx[m]], 3))
market_df["loan_rate_variable"] = market_df["month"].map(lambda m: round(loan_var_series[month_to_idx[m]], 3))
market_df["construction_cost_index"] = market_df["month"].map(lambda m: round(build_idx_series[month_to_idx[m]], 2))

market_df = market_df[[
    "month", "area_code", "area_name", "property_type",
    "tx_price_index", "land_price_index",
    "loan_rate_fixed_35y", "loan_rate_variable",
    "construction_cost_index",
]]
market_df.to_csv(f"{VOLUME_PATH}/market_index.csv", index=False, encoding="utf-8")
print(f"✅ 市況指標 {len(market_df)} 件: {VOLUME_PATH}/market_index.csv（land_price_index は NB 03 で L01 由来に上書き予定）")
market_df.head()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📄 01-F. PDF（重要事項説明書 + 物件パンフ）の Volume 配置確認
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC PDF は <b>事前に手動で Volume に配置</b>する前提（Claude で生成した架空サンプル）。<br>
# MAGIC 配置先：<code>{VOLUME_PATH}/pdf/</code>  ファイル名規則：<code>jyusetsu_*.pdf</code>（重説 5）/ <code>pamphlet_*.pdf</code>（パンフ 5）
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,PDF 配置確認
expected_pdfs = [f"jyusetsu_{i+1:02d}.pdf" for i in range(5)] + [f"pamphlet_{i+1:02d}.pdf" for i in range(5)]
pdf_dir = f"{VOLUME_PATH}/pdf"
found = {f.name for f in dbutils.fs.ls(pdf_dir)}
missing = [f for f in expected_pdfs if f not in found]

if missing:
    print(f"⚠ 未配置の PDF が {len(missing)} 件:")
    for m in missing:
        print(f"   - {m}")
    print(f"\n→ {pdf_dir}/ に手動配置してください。")
else:
    print(f"✅ PDF 10 件すべて配置済み: {pdf_dir}/")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 🎤 01-G. MP3（接客録音）の Volume 配置確認
# MAGIC <div style="border-left: 4px solid #FFC107; background: #FFF8E1; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC MP3 も <b>事前に手動で Volume に配置</b>する前提。<br>
# MAGIC 配置先：<code>{VOLUME_PATH}/audio/</code>  ファイル名規則：<code>recording_*.mp3</code>（5〜10 件）
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,MP3 配置確認
audio_dir = f"{VOLUME_PATH}/audio"
audio_files = [f.name for f in dbutils.fs.ls(audio_dir) if f.name.endswith(".mp3")]
if len(audio_files) < 5:
    print(f"⚠ MP3 が {len(audio_files)} 件しか配置されていません（推奨 5 件以上）。")
    print(f"→ {audio_dir}/ に手動配置してください。")
else:
    print(f"✅ MP3 {len(audio_files)} 件配置済み: {audio_dir}/")
    for f in audio_files[:10]:
        print(f"   - {f}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 📁 Volume 配下のサマリ
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC 配置されたファイルの件数のみ確認します（全ファイル列挙はせず、ノートブック出力肥大化を避ける）。
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Volume 配下のファイル数サマリ
def count_volume_files(path: str) -> Tuple[int, float]:
    """指定 path 配下のファイル数と合計サイズ（MB）を返す（1 階層のみ）"""
    files = dbutils.fs.ls(path)
    n_files = sum(1 for f in files if f.isFile())
    size_mb = sum(f.size for f in files if f.isFile()) / 1024 / 1024
    return n_files, size_mb


print("=== Volume 配下サマリ ===")
for sub in SUBDIRS:
    path = f"{VOLUME_PATH}/{sub}" if sub else VOLUME_PATH
    n, size_mb = count_volume_files(path)
    label = sub if sub else "（ルート）"
    print(f"  {label:20s}: {n:4d} ファイル, {size_mb:8.2f} MB")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #4CAF50; background: #E8F5E9; padding: 12px 16px; border-radius: 4px; margin: 10px 0;">
# MAGIC <strong>✅ データ準備完了</strong><br>
# MAGIC 次は <code>02_SDPパイプライン定義.sql</code> を Spark Declarative Pipelines として登録し、<code>04_SDPパイプライン設定手順.py</code> に従って実行してください。<br>
# MAGIC 並行して <code>03_地理空間データパイプライン.py</code> で KSJ ファイル（Shapefile / GML）を Bronze/Silver に取り込みます。
# MAGIC </div>
