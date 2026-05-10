import os
import boto3
from botocore.exceptions import ClientError


def garantizar_bucket(bucket_name):
    endpoint   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")

    s3 = boto3.client('s3', endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' existe.")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"🎉 Bucket '{bucket_name}' creado.")
