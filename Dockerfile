# Python 3.6 をベースにする
FROM python:3.6

# 作業ディレクトリ
WORKDIR /app

COPY req-old.txt .
# dill 0.4.0 をインストール
RUN pip install -r req-old.txt

# ホスト側でマウントするか COPY でスクリプトを入れる
COPY load_pickle.py .

# pickle を作るコマンドをデフォルト実行
CMD ["python", "load_pickle.py", "0.pkl"]
