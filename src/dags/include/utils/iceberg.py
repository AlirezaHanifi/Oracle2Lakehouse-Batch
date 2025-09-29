import logging
from typing import Optional

import pandas as pd
import pyarrow as pa
from airflow.hooks.base import BaseHook
from pyarrow import fs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType


class IcebergClient:
    def __init__(self, conn_id: str = "iceberg_default"):
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson

        self.warehouse_path = extra.get("warehouse_path", "s3://raw")
        s3_endpoint = extra.get("s3_endpoint_url", "http://minio:9000")
        s3_access_key = conn.login or extra.get("aws_access_key_id", "minioadmin")
        s3_secret_key = conn.password or extra.get(
            "aws_secret_access_key", "minioadmin"
        )

        if not self.warehouse_path or not s3_endpoint:
            raise ValueError("warehouse_path and s3_endpoint_url must be provided")

        port = conn.port or 8181
        catalog_uri = f"http://{conn.host}:{port}"

        self.catalog_properties = {
            "type": "rest",
            "uri": catalog_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.path-style-access": "true",
            "warehouse": self.warehouse_path,
            "s3.ssl-enabled": "false",
            "s3.region": "us-east-1",
        }

        logging.info("🔵 Connecting to Iceberg catalog at %s", catalog_uri)
        self.catalog = load_catalog("airflow_catalog", **self.catalog_properties)

        if s3_endpoint.startswith("http://"):
            endpoint_no_scheme = s3_endpoint.replace("http://", "")
            scheme = "http"
        elif s3_endpoint.startswith("https://"):
            endpoint_no_scheme = s3_endpoint.replace("https://", "")
            scheme = "https"
        else:
            endpoint_no_scheme = s3_endpoint
            scheme = "http"

        self.fs = fs.S3FileSystem(
            endpoint_override=endpoint_no_scheme,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            scheme=scheme,
            region="us-east-1",
        )

    def write(
        self,
        table_id: str,
        df: pd.DataFrame,
        drop_cols: Optional[list] = None,
    ):
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")

        arrow_table = pa.Table.from_pandas(df)
        namespace, table_name = table_id.split(".")

        try:
            namespaces = self.catalog.list_namespaces()
            existing_namespaces = [
                ns[0] if isinstance(ns, tuple) else ns for ns in namespaces
            ]
            if namespace not in existing_namespaces:
                self.catalog.create_namespace(namespace)
                logging.info("📁 Created namespace: %s", namespace)

            try:
                table = self.catalog.load_table(table_id)
                logging.info("📖 Found existing table: %s", table_id)
            except NoSuchTableError:
                schema_fields = []
                for i, (column, dtype) in enumerate(df.dtypes.items()):
                    if pd.api.types.is_integer_dtype(dtype):
                        field_type = LongType()
                    elif pd.api.types.is_float_dtype(dtype):
                        field_type = DoubleType()
                    else:
                        field_type = StringType()
                    schema_fields.append(
                        NestedField(
                            field_id=i + 1,
                            name=column,
                            field_type=field_type,
                            is_optional=True,
                        )
                    )

                table = self.catalog.create_table(
                    identifier=table_id,
                    schema=Schema(*schema_fields),
                    properties={
                        "format-version": "2",
                        "write.target-file-size-bytes": "268435456",
                        "write.delete.mode": "copy-on-write",
                        "write.update.mode": "copy-on-write",
                        "write.merge.mode": "copy-on-write",
                        "write.distribution-mode": "none",
                        "write.location": f"{self.warehouse_path}/{namespace}/{table_name}",
                        "write.data.path": "data",
                        "write.metadata.auto-compact": "true",
                        "write.metadata.previous-versions-max": "2",
                        "write.parquet.compression-codec": "zstd",
                        "write.parquet.compression-level": "3",
                        "created_at": pd.Timestamp.now().isoformat(),
                        "table_type": "managed",
                        "last_sync_type": "none",
                        "last_processed_date": "none",
                    },
                )
                logging.info("📝 Created new table: %s", table_id)

            try:
                execution_date = pd.Timestamp.now().strftime("%Y%m%d")

                if "full_sync" in table.metadata.properties.get("last_sync_type", ""):
                    last_processed = table.metadata.properties.get(
                        "last_processed_date"
                    )
                    if last_processed == execution_date:
                        logging.info(
                            "⏭️ Already processed data for date %s, skipping.",
                            execution_date,
                        )
                        return table.location()

                    transaction = table.transaction()
                    transaction.overwrite(arrow_table)
                    logging.info("🔄 Overwrote all data with new data")
                else:
                    last_processed = table.metadata.properties.get(
                        "last_incremental_date"
                    )
                    if last_processed == execution_date:
                        logging.info(
                            "⏭️ Already processed incremental data for date %s, skipping.",
                            execution_date,
                        )
                        return table.location()

                    transaction = table.transaction()
                    transaction.append(arrow_table)
                    logging.info("➕ Appended new data")

                transaction = table.transaction()
                properties = {
                    "last_sync_type": "full_sync",
                    "last_row_count": str(len(df)),
                    "last_sync_timestamp": pd.Timestamp.now().isoformat(),
                    "last_processed_date": execution_date,
                    "rows_written": str(len(df)),
                }
                transaction.set_properties(properties)

                try:
                    table.refresh()
                    if hasattr(table, "rewrite_data_files"):
                        table.rewrite_data_files()
                        logging.info("📦 Compacted data files")
                except Exception as e:
                    logging.warning("⚠️ Could not compact files: %s", str(e))

                logging.info("✅ Operation completed successfully")
                logging.info("✅ Written %d rows to table: %s", len(df), table_id)
                logging.info("📍 Table location: %s", table.location())

                return table.location()

            except Exception as e:
                logging.error("❌ Transaction failed: %s", str(e))
                raise

        except Exception as e:
            logging.error("❌ Failed to write to table %s: %s", table_id, str(e))
            raise
