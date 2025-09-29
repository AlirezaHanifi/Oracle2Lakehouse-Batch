from typing import Optional

import pandas as pd

from dags.include.utils.iceberg import IcebergClient


class AuditClient:
    def __init__(
        self, minio_endpoint: str = "http://minio:9000", bucket: str = "audit"
    ):
        self.minio_endpoint = minio_endpoint
        self.bucket = bucket
        self.iceberg_client = IcebergClient()

    def _table_path(self, table_id: str) -> str:
        """Generate the table path for audit logs"""
        schema, table = table_id.split(".")
        return f"audit.{schema}_{table}_audit"

    def write(
        self,
        table_id: str,
        row_count: int,
        sync_mode: str,
        status: str,
        data_interval_start: Optional[str] = None,
        data_interval_end: Optional[str] = None,
        details: str = "",
    ) -> str:
        audit_table_id = self._table_path(table_id)
        df = pd.DataFrame(
            [
                {
                    "table_id": str(table_id),
                    "row_count": int(row_count),
                    "sync_mode": str(sync_mode),
                    "status": str(status),
                    "details": str(details),
                    "data_interval_start": str(data_interval_start)
                    if data_interval_start
                    else None,
                    "data_interval_end": str(data_interval_end)
                    if data_interval_end
                    else None,
                    "synced_at": str(pd.Timestamp.now()),
                }
            ]
        )

        self.iceberg_client.write(table_id=audit_table_id, df=df)

        return audit_table_id
