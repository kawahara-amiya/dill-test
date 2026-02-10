def get_pickle_protocol(filename: str) -> int:
    with open(filename, "rb") as f:
        header = f.read(2)  # PROTOマーカー + version バイト
    # header は b"\x80\x05" のような形式なので、index 1 がプロトコル番号
    if header and header[0] == 0x80:
        return header[1]
    raise ValueError("プロトコル情報が見つかりません")
