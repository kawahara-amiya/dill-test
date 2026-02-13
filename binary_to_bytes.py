import sys
from io import BytesIO

def read_file_as_bytes(file_path):
    """ファイルを読み込んでbytesで返す（S3.download_fileobjのモック）"""
    try:
        with open(file_path, 'rb') as f:
            data = f.read()
        return BytesIO(data)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def mock_s3_download(bucket_name, key, fileobj):
    """S3.Bucket.download_fileobjのモック"""
    # 実際にはS3から読み込むが、ここはローカルファイルから読む
    data = read_file_as_bytes(key)
    fileobj.write(data.getvalue())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python binary_to_bytes.py <file_path>", file=sys.stderr)
        sys.exit(1)
    
    file_path = sys.argv[1]
    buffer = BytesIO()
    mock_s3_download("mock_bucket", file_path, buffer)
    
    # bytesを標準出力に送信
    sys.stdout.buffer.write(buffer.getvalue())