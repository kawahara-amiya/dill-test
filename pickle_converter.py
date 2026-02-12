import dill
import sys

def get_pickle_protocol(filename: str) -> int:
    with open(filename, "rb") as f:
        header = f.read(2)  # PROTOマーカー + version バイト
    # header は b"\x80\x05" のような形式なので、index 1 がプロトコル番号
    if header and header[0] == 0x80:
        return header[1]

def convert_pickle(filename: str, target_protocol: int) -> None:
    with open(filename, "rb") as f:
        obj = dill.load(f)

    with open(filename, "wb") as f:
        dill.dump(obj, f, protocol=target_protocol)


if __name__ == "__main__":
    in_file = sys.argv[1]
    target_proto = int(sys.argv[2])

    current_proto = get_pickle_protocol(in_file)
    if current_proto == target_proto:
        exit(0)
    
    convert_pickle(in_file, target_proto)