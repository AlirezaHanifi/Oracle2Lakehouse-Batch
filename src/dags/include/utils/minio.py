"""
MinIO Client for S3-compatible storage operations.

This module provides a lightweight client to:
1. Connect to MinIO using Airflow connection IDs
2. Manage buckets (create if missing)
3. Upload binary file-like objects to buckets
4. Download objects from buckets as streams
"""

from __future__ import annotations

import io
import logging
from typing import Dict


class MinioClient:
    """Client for interacting with MinIO S3-compatible storage."""

    def __init__(self, conn_id: str = "minio_default"):
        """
        Initialize MinIO client using Airflow connection.

        Args:
            conn_id: Airflow connection ID for MinIO
        """
        from airflow.sdk.bases.hook import BaseHook
        from minio import Minio

        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson

        self.endpoint = extra.get("endpoint_url") or f"{conn.host}:{conn.port or 9000}"
        self.access_key = extra.get("aws_access_key_id") or conn.login
        self.secret_key = extra.get("aws_secret_access_key") or conn.password
        self.secure = extra.get("secure", False)

        logging.info("🟢 Connecting to MinIO at %s", self.endpoint)
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def get_credentials(self) -> Dict[str, str]:
        """Get S3 credentials for external services."""
        return {
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
            "endpoint_url": f"http{'s' if self.secure else ''}://{self.endpoint}",
        }

    def ensure_bucket(self, bucket: str) -> None:
        """
        Create a bucket if it doesn't exist.

        Args:
            bucket: Name of the bucket to create/check
        """
        if not self.client.bucket_exists(bucket):
            logging.info("📦 Creating bucket: %s", bucket)
            self.client.make_bucket(bucket)
        else:
            logging.info("📦 Bucket exists: %s", bucket)

    def upload_fileobj(
        self, bucket: str, object_name: str, data: io.BytesIO, length: int
    ) -> str:
        """Upload a binary file-like object to MinIO and return the s3 path."""
        logging.info(
            "⬆️ Streaming upload to s3://%s/%s (bytes=%d)", bucket, object_name, length
        )
        self.ensure_bucket(bucket)
        data.seek(0)
        self.client.put_object(bucket, object_name, data, length=length)
        logging.info("✅ Stream upload complete: s3://%s/%s", bucket, object_name)
        return f"s3://{bucket}/{object_name}"

    def download_fileobj(self, bucket: str, object_name: str) -> io.BytesIO:
        """Download an object and return it as a BytesIO stream."""
        logging.info("⬇️ Streaming download s3://%s/%s", bucket, object_name)
        obj = self.client.get_object(bucket, object_name)
        data = obj.read()
        buf = io.BytesIO(data)
        buf.seek(0)
        logging.info(
            "✅ Stream download complete: s3://%s/%s (bytes=%d)",
            bucket,
            object_name,
            len(data),
        )
        return buf

