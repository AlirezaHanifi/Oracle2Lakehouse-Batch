"""
Audit module for Oracle2Lakehouse-Batch.

1. This module records synchronization audit logs in ClickHouse.
2. It ensures that the audit table exists and logs each synchronization
operation with the number of inserted rows, status, and timestamp
"""

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from .clickhouse import ClickHouseClient


def log_sync_audit(
    spec: Dict[str, Any], rows_inserted: int, ch_client: ClickHouseClient
) -> None:
    import logging

    audit_db = spec["clickhouse"].get("audit_database", spec["clickhouse"]["database"])
    audit_table = spec["clickhouse"].get(
        "audit_table", f"{spec['clickhouse']['table']}_audit"
    )

    ch_client.create_table_if_not_exists(
        database=audit_db,
        table=audit_table,
        columns=[
            ("table_id", "String"),
            ("row_count", "Int64"),
            ("status", "String"),
            ("synced_at", "DateTime64(6)"),
        ],
        engine=spec["clickhouse"].get("audit_engine", "MergeTree()"),
        order_by="synced_at",
    )

    query = f"""
        INSERT INTO {audit_db}.{audit_table}
        (table_id, row_count, status, synced_at)
        VALUES
        ('{spec["table_id"]}', {rows_inserted}, 'success', toTimeZone(now64(), 'Asia/Tehran'))
    """

    try:
        ch_client.execute(query)
        logging.info(
            "✅ Logged audit record for %s (%d rows)", spec["table_id"], rows_inserted
        )
    except Exception as e:
        logging.error("❌ Failed to log audit record: %s", e)
        raise
