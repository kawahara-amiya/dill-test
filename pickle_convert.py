import dill

def get_pickle_protocol(filename: str) -> int:
    with open(filename, "rb") as f:
        header = f.read(2)  # PROTOマーカー + version バイト
    # header は b"\x80\x05" のような形式なので、index 1 がプロトコル番号
    if header and header[0] == 0x80:
        return header[1]
    raise ValueError("プロトコル情報が見つかりません")

def save_pickle() -> str:
    # サンプル関数
    def foo(x, y=10):
        return x * 2 + y

    # 関数入りのデータ
    data = {"func": foo, "value": 42}
    filename = "/app/code.pkl"

    # dill でシリアライズしてファイルに保存
    with open(filename, "wb") as f:
        dill.dump(data, f)

    print("pickle created!")
    return filename

def convert_pickle(filename: str, target_protocol: int) -> None:
    with open(filename, "rb") as f:
        obj = dill.load(f)

    with open(filename, "wb") as f:
        dill.dump(obj, f, protocol=target_protocol)


if __name__ == "__main__":
    # pickle_file = save_pickle()
    pickle_file = "/app/code.pkl"
    current_protocol = get_pickle_protocol(pickle_file)
    print("Current pickle protocol:", current_protocol)

    print("試しにloadしてみます...")
    with open(pickle_file, "rb") as f:
        obj = dill.load(f)
    print("Loaded object:", obj)

    target_protocol = 4
    if current_protocol != target_protocol:
        convert_pickle(pickle_file, target_protocol)
        print(f"Converted pickle to protocol {target_protocol}")
    else:
        print("No conversion needed.")