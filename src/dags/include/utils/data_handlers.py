"""
Data handling utilities for Oracle2Lakehouse-Batch.

This module provides functionality for:
1. Data transformation and processing
2. File handling and S3 operations
3. Common data manipulation functions
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    import pendulum
    import polars as pl

    from .minio import MinioClient


def get_minio_client(spec: Dict[str, Any]) -> MinioClient:
    """Get MinIO client from spec."""
    from .minio import MinioClient

    return MinioClient(conn_id=spec.get("minio_conn", "minio_default"))


def get_bucket_name(spec: Dict[str, Any], stage: str = "raw") -> str:
    """Get bucket name from spec.

    Args:
        spec: Table specification
        stage: Data stage ('raw' or 'processed')
    """
    base_bucket = spec.get("minio", {}).get("bucket", "data")
    return f"{base_bucket}-{stage}"


def get_prefix(spec: Dict[str, Any]) -> str:
    """Get S3 prefix from spec."""
    return spec.get("minio", {}).get("prefix", spec["table_id"].replace(".", "/"))


def ensure_buckets(spec: Dict[str, Any]) -> None:
    """Ensure all required buckets exist."""
    client = get_minio_client(spec)
    for stage in ["raw", "processed"]:
        bucket_name = get_bucket_name(spec, stage)
        client.ensure_bucket(bucket_name)
        logging.info("✅ Ensured %s bucket exists: %s", stage, bucket_name)


def upload_df_to_minio(
    spec: Dict[str, Any],
    df: pl.DataFrame,
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    stage: str = "raw",
) -> str:
    """Upload Polars DataFrame to MinIO as Parquet with idempotent naming.

    Args:
        spec: Table specification
        df: DataFrame to upload
        data_interval_start: Start of the data interval
        data_interval_end: End of the data interval
        stage: Storage stage ('raw' or 'processed')

    The file name pattern will be: schema__table__data_interval_start__data_interval_end.parquet
    """
    import io

    minio = get_minio_client(spec)
    bucket = get_bucket_name(spec, stage)
    prefix = get_prefix(spec)

    schema, table = spec["table_id"].split(".")
    start_ts = data_interval_start.strftime("%Y%m%d%H%M%S")
    end_ts = data_interval_end.strftime("%Y%m%d%H%M%S")

    object_name = f"{prefix}/{schema}__{table}__{start_ts}__{end_ts}.parquet"

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    minio.upload_fileobj(
        bucket=bucket,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
    )
    return f"s3://{bucket}/{object_name}"


def handle_data_payload(
    spec: Dict[str, Any],
    df: Optional[pl.DataFrame],
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
) -> Dict[str, Any]:
    """Handle DataFrame payload with size threshold and proper timestamps."""
    if df is None or df.is_empty():
        return {"mode": "empty", "data": None, "path": None}

    path = upload_df_to_minio(
        spec=spec,
        df=df,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )
    return {"mode": "s3", "data": None, "path": path}


def load_df_from_payload(
    spec: Dict[str, Any], payload: Dict[str, Any]
) -> Optional[pl.DataFrame]:
    """Load DataFrame from payload dict."""
    import polars as pl

    mode = payload.get("mode")
    if mode == "empty":
        return None
    if mode == "data":
        return payload.get("data")
    elif mode == "s3":
        path = payload["path"]
        bucket, key = path.replace("s3://", "").split("/", 1)
        minio = get_minio_client(spec)
        file_obj = minio.download_fileobj(bucket, key)
        return pl.read_parquet(file_obj)
    raise ValueError("Unsupported payload mode")


def get_s3_path_from_payload(
    spec: Dict[str, Any],
    payload: Dict[str, Any],
    data_interval_start: Optional[pendulum.DateTime] = None,
    data_interval_end: Optional[pendulum.DateTime] = None,
) -> Optional[str]:
    """Get or create S3 path from data payload."""
    import pendulum

    mode = payload.get("mode")
    if mode == "empty":
        return None
    if mode == "s3":
        return payload["path"]
    elif mode == "data":
        df = payload["data"]
        now = pendulum.now()
        start = data_interval_start or now
        end = data_interval_end or now
        return upload_df_to_minio(
            spec=spec, df=df, data_interval_start=start, data_interval_end=end
        )
    raise ValueError("Unsupported payload mode")


def transform_data(
    spec: Dict[str, Any],
    extracted_data: Dict[str, Any],
    data_interval_start: Optional[pendulum.DateTime] = None,
    data_interval_end: Optional[pendulum.DateTime] = None,
) -> Dict[str, Any]:
    """Transform extracted data using DuckDB and Polars."""
    import pendulum

    df = load_df_from_payload(spec, extracted_data)
    if df is None:
        return {"mode": "empty", "data": None, "path": None}

    if spec.get("drop_columns"):
        df = df.drop(spec["drop_columns"])

    now = pendulum.now()
    start = data_interval_start or now
    end = data_interval_end or now

    return {
        "mode": "s3",
        "path": upload_df_to_minio(
            spec=spec,
            df=df,
            data_interval_start=start,
            data_interval_end=end,
            stage="processed",
        ),
        "rows": len(df),
        "data": None,
    }
