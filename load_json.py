import json
import importlib

def decode_from_json(json_path):
    """
    JSON ファイルを読み込み、
    JSON -> Python オブジェクト(そのクラスインスタンス) に復元する
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # モジュール名とクラス名を取り出す
    module_name = data.get("__module__")
    class_name = data.get("__class__")
    attrs = data.get("attrs", {})

    if module_name is None or class_name is None:
        raise ValueError("JSON に __module__ / __class__ 情報がありません")

    # モジュールを動的に import
    module = importlib.import_module(module_name)  # turn0search1

    # クラスオブジェクトを取得
    cls = getattr(module, class_name)

    # インスタンスを生成（コンストラクタを呼ばない）
    inst = cls.__new__(cls)

    # 保存された属性を復元
    inst.__dict__.update(attrs)

    return inst


if __name__ == "__main__":
    import sys

    json_file = sys.argv[1]  # 例: "model.json"
    obj = decode_from_json(json_file)

    print("Loaded object:", obj)
    print("Type:", type(obj))
