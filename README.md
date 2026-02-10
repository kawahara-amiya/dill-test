# dill-test

`dill` でシリアライズされた pickle ファイルのプロトコルを確認・変換するためのサンプルリポジトリです。

## 概要

- `dill` を使って関数を含む Python オブジェクトを pickle 化します。
- 既存の pickle ファイルの「プロトコルバージョン」をヘッダーから読み取ります。
- 必要に応じて、指定したプロトコルバージョンへ再シリアライズ（変換）します。
- Docker コンテナ内で pickle を作成する例も含まれます。

## ファイル構成

- `protocol.py`  
  指定した pickle ファイルのプロトコルバージョンをヘッダーから取得するユーティリティ関数 `get_pickle_protocol()` を提供します。

- `pickle_convert.py`  
  - `save_pickle()` で `/app/code.pkl` に dill による pickle を作成するサンプル。  
  - `convert_pickle()` で指定プロトコルに変換します。  
  - `__main__` ブロックでは `/app/code.pkl` のプロトコルを確認し、`target_protocol = 4` に変換する処理のサンプルがあります。

- `pickle_converter.py`  
  コマンドラインツール用の簡易スクリプト。  
  `python pickle_converter.py <input.pkl> <target_protocol>` の形式で呼び出し、
  - 現在のプロトコルを確認
  - 既に指定プロトコルであれば何もせず終了
  - 異なっていれば指定プロトコルで再保存
 します。

- `save_pickle.py`  
  `/app/code.pkl` に dill でサンプルオブジェクトを保存するスクリプト。Docker コンテナ内での利用を前提としたパスになっています。

- `save_pickle_here.py`  
  カレントディレクトリ (`./code_here.pkl`) に dill プロトコル 3 でサンプルオブジェクトを保存するスクリプト。

- `load_pickle.py`  
  カレントディレクトリの `code.pkl` についてプロトコルを表示し、`dill.load()` でロードする簡単な確認用スクリプトです（プロトコルの違いによるエラー動作確認用途）。

- `main.py`  
  `protocol.get_pickle_protocol("code.pkl")` を呼び出し、プロトコルを表示する最小実行例。

- `Dockerfile`  
  Python 3.8 ベースのコンテナで `dill==0.2.8.2` をインストールし、`save_pickle.py` を実行して `/app/code.pkl` を作成するための定義です。

- `requirements.txt`  
  ローカル実行用に `dill==0.4.0` を指定しています。

## 環境

- Python 3.8 以上推奨
- ローカル実行時は `requirements.txt` を利用します。

```bash
pip install -r requirements.txt
```

Docker を使う場合は、同梱の `Dockerfile` をビルドして利用します。

```bash
docker build -t dill-test .
docker run --rm -v %CD%:/app dill-test  # Windows PowerShell 例
```

## 典型的な使い方

### 1. ローカルでサンプル pickle を作成

```bash
python save_pickle_here.py  # ./code_here.pkl が作成される
```

作成されたファイルのプロトコルを確認:

```bash
python main.py  # または protocol.get_pickle_protocol("code.pkl") を直接利用
```

### 2. 任意の pickle ファイルのプロトコル変換

`pickle_converter.py` を使い、任意の pickle を別プロトコルに変換できます。

```bash
python pickle_converter.py your_file.pkl 4  # プロトコル 4 に変換
```

### 3. Docker コンテナ内での pickle 作成

```bash
docker build -t dill-test .
docker run --rm dill-test
```

コンテナ内で `/app/code.pkl` が生成されます。マウントを使ってホストと共有することもできます。

```bash
docker run --rm -v %CD%:/app dill-test
```

## 注意点

- `dill` のバージョンによって対応しているプロトコルバージョンが異なります。  
  コンテナ内 (`dill==0.2.8.2`) とローカル (`dill==0.4.0`) で挙動が変わる例として利用できます。
- `convert_pickle` は上書き保存を行うため、必要なら事前にバックアップを取ってください。

## ライセンス

このリポジトリは個人学習用サンプルとして想定されています。必要に応じて適宜ライセンスを追加してください。
