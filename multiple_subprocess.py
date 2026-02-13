import io
from scipy.stats import gaussian_kde, norm
import numpy as np
import re
import struct
import os
import subprocess

def parse_dataset(dataset_str: str) -> list:
    """二次元配列まで、numpy listの文字列をパースする"""
    dataset_str = dataset_str.strip().strip('\'"')
    # 全数値をフラットに取り出す
    nums = np.fromstring(re.sub(r'[\[\]]', ' ', dataset_str), sep=' ')
    # 2次元配列なら内側の [] を見て行数・列数を推測
    rows = re.findall(r'\[([^\[\]]+)\]', dataset_str)
    if rows:
        row_lens = [len(np.fromstring(r, sep=' ')) for r in rows]
        if len(set(row_lens)) == 1:
            return nums.reshape((len(rows), row_lens[0])).tolist()
    return nums.tolist()

def get_bytes_dict():
    bytes_dict = {}
    with open('0.pkl', 'rb') as f:
        data = io.BytesIO(f.read())
        data.seek(0)
        bytes_dict[1] = data.getvalue()

    with open('0_copy.pkl', 'rb') as f:
        data = io.BytesIO(f.read())
        data.seek(0)
        bytes_dict[2] = data.getvalue()
    return bytes_dict

def restore_kde(prob_dens_func_json: any, bw_method):
    """json構造のprob_dens_funcをkde関数にパースする"""
    attrs = prob_dens_func_json["attrs"]
    dataset = parse_dataset(attrs["dataset"])
    return gaussian_kde(dataset, bw_method=bw_method)

def create_payload(buffers: dict[int, bytes]) -> bytes:
    payload = bytearray()

    for key, data in buffers.items():
        payload.extend(struct.pack(">I", key))      # 4バイト key
        payload.extend(struct.pack(">I", len(data)))# 4バイト length
        payload.extend(data)                        # 本体

    return bytes(payload)

def convert_pickles_to_jsonarr(scr_data: dict[int, bytes], timeout: int = 60) -> str:
    """
    サブプロセスで Python 3.6 を使用して、
    pickle バイト辞書を stdin 経由で渡し、
    JSON 文字列を stdout から受け取る。
    """

    # Dockerfileで定義した環境変数
    TASK_ROOT = os.getenv("LAMBDA_TASK_ROOT")
    CONVERTER_SCRIPT = f"{TASK_ROOT}/Modules/migration/subprocess/pickle_json_converter.bin"

    cmd = [CONVERTER_SCRIPT, "--in", "-"]
    try:
        proc = subprocess.run(
            cmd,
            input=create_payload(scr_data),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            timeout=timeout
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Subprocess failed with return code {e.returncode}: {e.stderr.decode()}") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("Subprocess timed out") from e

    json_text = proc.stdout.decode("utf-8")
    return json_text


if __name__ == "__main__":
    bytes_dict = get_bytes_dict()
    jsonarr = convert_pickles_to_jsonarr(bytes_dict)