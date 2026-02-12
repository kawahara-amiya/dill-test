import dill
import protocol
import sys
import json

file_name = sys.argv[1]
proto = protocol.get_pickle_protocol(file_name)
print("Pickle protocol:", proto)

with open(file_name, "rb") as f:
    obj = dill.load(f)

print(type(obj))
print(obj)

# dictをjsonにエクスポート
json_file = file_name.replace(".pkl", ".json")
with open(json_file, "w") as f:
    json.dump(obj, f, default=str)

print(f"Exported to {json_file}")

# jsonから読み込み
with open(json_file, "r") as f:
    loaded_obj = json.load(f)

print("Loaded from JSON:", loaded_obj)
