# 接客録音 MP3 - 架空データ

このディレクトリには、不動産仲介 E2E デモ用の**架空の接客録音**（MP3、計 8 件）を格納します。
[scripts/generate_audio.py](../../../../scripts/generate_audio.py) によって macOS の `say` コマンドと
ffmpeg で機械合成された日本語音声で、実在の会話・人物・物件とは関係ありません。

## ファイル命名規則

```
recording_NN.mp3
```

`NN` は 2 桁のシーケンス番号で、`inquiry_id = IQ{NN:06d}` に対応します
（例：`recording_03.mp3` → `IQ000003`）。
[05_AIとGeoによる補完処理.py](../../05_AIとGeoによる補完処理.py) の
`bz_audio_transcripts` テーブルで、`regexp_extract(path, 'recording_(\d+).mp3', 1)`
により inquiry_id 列に展開されます。

## ファイル一覧

| ファイル | inquiry_id | シナリオ | 話者 | 内容概要 |
|---|---|---|---|---|
| recording_01.mp3 | IQ000001 | 駅徒歩・周辺環境説明 | Kyoko（顧客）| マンション内見時。千歳船橋駅徒歩 8 分、商店街経由の道のり、南西角部屋・管理形態・修繕積立金の説明 |
| recording_02.mp3 | IQ000002 | ハザード説明 | Eddy（営業）| 横浜市港北区の戸建てについて、鶴見川浸水想定区域外であること、台風時の周辺河川状況、本物件の標高 25m を説明 |
| recording_03.mp3 | IQ000003 | 価格交渉（指値希望）| Kyoko（顧客）| 吹田市江坂町の戸建て、売出価格 4,980 万円に対し予算 4,500 万円、売主の買い替え事情を踏まえた指値相談 |
| recording_04.mp3 | IQ000004 | 買付申込書受領 | Eddy（営業）| 福岡市赤坂のマンション、申込価格 5,200 万円・手付 520 万円・住宅ローン特約付きの内容確認 |
| recording_05.mp3 | IQ000005 | 住宅ローン相談 | Kyoko（顧客）| 物件価格 6,480 万円・頭金 1,000 万円・借入 5,480 万円の試算。35 年固定 1.85% / 変動 0.6% の月返済比較 |
| recording_06.mp3 | IQ000006 | 競合物件比較 | Eddy（営業）| 他社の栄タワマンと矢場町物件の比較。築年数 vs 専有面積 vs 心理的瑕疵告知のトレードオフを説明 |
| recording_07.mp3 | IQ000007 | リフォーム前提交渉 | Kyoko（顧客）| 築 16 年戸建て、リフォーム費 500 万円見込みでの指値根拠。ホームインスペクション提案 |
| recording_08.mp3 | IQ000008 | 引渡日確認・決済段取り | Eddy（営業）| 来月 15 日 A 銀行麹町支店での決済、必要書類・残代金 4,380 万円・固定資産税精算・鍵引渡しの段取り |

**話者構成**：営業担当役を Eddy（男性、F0 ≈ 82Hz）、顧客側を Kyoko（女性、F0 ≈ 192Hz）に
交互配置。実運用での「営業 vs 顧客」両側の発話を Whisper で文字起こしする想定です。

> ⚠️ **macOS 標準 voice について**：当初 `Otoya`（男性）を指定していましたが、
> 近年の macOS には未収録となっており、`say -v Otoya` 指定は黙ってデフォルト voice
> （Kyoko 等）にフォールバックします。実機に標準収録され、かつピッチ計測で男性と
> 判別された Eddy を採用しています（Reed / Rocko も男性ですが、Eddy が最も
> 営業担当の話し方として自然）。

## ファイル仕様

| 項目 | 値 |
|---|---|
| フォーマット | MPEG-1 Layer III (MP3) + ID3v2.4 タグ |
| コーデック | libmp3lame（VBR 品質 5、約 56〜130 kbps） |
| サンプリングレート | 22,050 Hz |
| チャンネル | モノラル |
| 1 ファイル長さ | 約 30 秒〜2 分 |
| 1 ファイルサイズ | 約 250〜360 KB |

> ⚠️ **音声品質について**：macOS 標準 TTS で生成しているため、抑揚やイントネーションは
> 機械合成特有のものです。本デモの目的は「Whisper による文字起こし → LLM での要約・抽出」
> のパイプライン動作確認であり、自然な対話音声を再現するものではありません。

## ライセンス

自前生成の架空データのため**自由に利用・改変・再配布可能**です。
実在の物件・顧客・営業担当者とは無関係です。

## データ生成方法

データを**再生成する場合**：

```bash
# 全 8 件を生成（既存ファイルは skip）
python3 scripts/generate_audio.py

# 既存ファイルを上書き
python3 scripts/generate_audio.py --force

# 全ファイルを Kyoko の声で統一
python3 scripts/generate_audio.py --voice-override Kyoko
```

**前提条件**：
- macOS（`say` コマンドが必要）
- `brew install ffmpeg`（MP3 エンコード用）

**Linux/Windows 環境**で生成したい場合は、`scripts/generate_audio.py` の
`synthesize()` 関数を `gTTS` / `pyttsx3` / Cloud TTS（Google / Azure / ElevenLabs）
等に差し替えてください。

## デモでの使われ方

`01_データ準備.py` 実行時は Volume 配下にコピーされず、リポジトリの本ディレクトリを
そのまま Databricks Repos 経由で参照することを想定しています
（`01_データ準備.py` の 01-G セクションで配置数のみ確認）。

`05_AIとGeoによる補完処理.py` で：

1. `bz_audio_transcripts` テーブルへ `binaryFile` 形式で読込
2. `ai_query()` + Whisper モデルで文字起こし
3. `sl_inquiries_enriched` テーブルで `inquiry_id` をキーに JOIN
4. ファネル分析・顧客インサイト抽出に利用
