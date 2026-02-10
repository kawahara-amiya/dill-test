import dill
import protocol

file_name = "code.pkl"
proto = protocol.get_pickle_protocol(file_name)
print("Pickle protocol:", proto)

with open(file_name, "rb") as f:
    obj = dill.load(f)   # ここでエラーになるはず

print(obj)
