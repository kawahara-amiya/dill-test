import dill

# サンプル関数
def foo(x, y=10):
    return x * 2 + y

# 関数入りのデータ
data = {"func": foo, "value": 42}

# dill でシリアライズしてファイルに保存
with open("./code_here.pkl", "wb") as f:
    dill.dump(data, f, protocol=3)

print("pickle created!")
