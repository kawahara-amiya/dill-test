import dill
import protocol
import sys
import json
import importlib

def encode_attr(val):
    # 基本型はそのまま
    if isinstance(val, (str, int, float, bool, type(None))):
        return val
    # dict → キーと値を再帰的に変換
    if isinstance(val, dict):
        return {k: encode_attr(v) for k, v in val.items()}
    # list / tuple → 再帰的に変換
    if isinstance(val, (list, tuple)):
        return [encode_attr(v) for v in val]
    # その他オブジェクト → __dict__ を使う（最終的に再帰）
    if hasattr(val, "__dict__"):
        return { "__class__": val.__class__.__name__,
                 "__module__": val.__class__.__module__,
                 "attrs": encode_attr(val.__dict__) }
    # 上記以外は文字列化
    return str(val)

def encode_obj(obj):
    """Python3.6 でロードしたオブジェクトを JSON に変換"""
    return {
        "__class__": obj.__class__.__name__,
        "__module__": obj.__class__.__module__,
        "attrs": encode_attr(obj.__dict__),
    }

def decode_obj(d):
    """JSON → オブジェクト再構築"""
    cls = getattr(importlib.import_module(d["__module__"]), d["__class__"])
    inst = cls.__new__(cls)                     # インスタンスだけ作る
    inst.__dict__.update(d["attrs"])            # 属性を書き戻す
    return inst

file_name = sys.argv[1]
proto = protocol.get_pickle_protocol(file_name)
print("Pickle protocol:", proto)

# dill で読み込み
with open(file_name, "rb") as f:
    obj = dill.load(f)

print(type(obj))
print(obj)

# dict へ変換して JSON 出力
json_file = file_name.replace(".pkl", ".json")
with open(json_file, "w") as f:
    json.dump(encode_obj(obj), f, indent=2)

print(f"Exported to {json_file}")

# JSON から読み込み
with open(json_file, "r") as f:
    loaded_raw = json.load(f)

loaded_obj = decode_obj(loaded_raw)
print("Loaded from JSON:", loaded_obj)
print(type(loaded_obj))
