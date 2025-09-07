import io
import logging

import pandas as pd
from airflow.hooks.base import BaseHook
from minio import Minio


class MinioClient:
    def __init__(self, conn_id="minio_default"):
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        endpoint = extra.get("endpoint_url") or f"{conn.host}:{conn.port or 9000}"
        access_key = extra.get("aws_access_key_id") or conn.login
        secret_key = extra.get("aws_secret_access_key") or conn.password
        secure = extra.get("secure", False)

        logging.info("🟢 Connecting to MinIO at %s", endpoint)
        self.client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

    def ensure_bucket(self, bucket: str):
        if not self.client.bucket_exists(bucket):
            logging.info("📦 Creating bucket: %s", bucket)
            self.client.make_bucket(bucket)
        else:
            logging.info("📦 Bucket exists: %s", bucket)

    def _upload(self, bucket: str, object_name: str, data: bytes):
        self.ensure_bucket(bucket)
        self.client.put_object(bucket, object_name, io.BytesIO(data), length=len(data))
        logging.info("✅ Successfully uploaded %s", object_name)

    def upload_parquet(self, bucket: str, object_name: str, df: pd.DataFrame):
        logging.info(
            "⬆️ Uploading parquet %s to bucket %s (shape: %s)",
            object_name,
            bucket,
            df.shape,
        )
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        self._upload(bucket, object_name, buf.read())

    def get_parquet(self, bucket: str, object_name: str) -> pd.DataFrame:
        logging.info("⬇️ Fetching parquet %s from bucket %s", object_name, bucket)
        try:
            obj = self.client.get_object(bucket, object_name)
            data = io.BytesIO(obj.read())
            df = pd.read_parquet(data)
            logging.info(
                "✅ Successfully read parquet %s (%d rows)", object_name, len(df)
            )
            return df
        except Exception as e:
            logging.error(
                "❌ Failed to read parquet %s from bucket %s: %s",
                object_name,
                bucket,
                e,
            )
            raise

    def upload_json(self, bucket: str, object_name: str, data: dict):
        import json

        self._upload(bucket, object_name, json.dumps(data).encode("utf-8"))

    def list_objects(self, bucket: str, prefix: str = ""):
        logging.info("📄 Listing objects in bucket %s with prefix '%s'", bucket, prefix)
        return list(self.client.list_objects(bucket, prefix=prefix, recursive=True))
