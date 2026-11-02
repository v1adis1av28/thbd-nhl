"""Список объектов бакета. Запускается внутри контейнера Airflow."""

import os

import boto3
from botocore.client import Config


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    bucket = os.environ["MINIO_BUCKET"]
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=40)
    contents = response.get("Contents") or []
    print(f"bucket {bucket}: показано {len(contents)} объектов")
    for item in contents:
        print(f"{item['Size']:8} {item['Key']}")


if __name__ == "__main__":
    main()
