# Python 3.6 をベースにする
FROM python:3.8

# 作業ディレクトリ
WORKDIR /app

# dill 0.4.0 をインストール
RUN pip install dill==0.2.8.2

# ホスト側でマウントするか COPY でスクリプトを入れる
COPY save_pickle.py .

# pickle を作るコマンドをデフォルト実行
CMD ["python", "save_pickle.py"]
