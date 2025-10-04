"""Audit module for Oracle2Lakehouse-Batch.

This module writes a single centralized audit table for all syncs. It will:
1. Ensure that the configured audit database and table exist.
2. Insert a single audit record per synchronization run with optional enrichment fields.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from dags.include.utils.clickhouse import ClickHouseClient


def _format_value_for_sql(v: Any) -> str:
    """Convert a Python value into a ClickHouse-compatible SQL literal.

    Args:
        v: The Python value to format.

    Returns:
        A string representation of the value suitable for inline use in
        ClickHouse SQL statements.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    s = s.replace("'", "\\'")
    return f"'{s}'"


def log_sync_audit(
    spec: Dict[str, Any],
    rows_inserted: int,
    ch_client: ClickHouseClient,
    enrichment: Dict[str, Any] = {},
) -> None:
    """Insert a single audit record into a centralized ClickHouse audit table.

    This function ensures the audit table exists, then logs one record
    representing a single synchronization run.  
    It captures essential information such as the table identifier,
    synchronization mode, record count, source/destination systems,
    and optional enrichment metadata.

    Args:
        spec: The synchronization specification (YAML-parsed dictionary)
            describing the table and sync configuration.
        rows_inserted: The number of rows inserted during the sync (best effort).
        ch_client: An instance of ClickHouseClient used for executing queries.
        enrichment: Optional dictionary containing additional metadata, e.g.:
            - `batch_id`: Unique batch or run identifier.
            - `data_interval_start`, `data_interval_end`: ISO 8601 timestamps.
            - `dag_id`, `run_id`: Airflow DAG and run identifiers.
            - `source`, `destination`: Override system identifiers.
            - `status`: Execution status ("success" or "failed").
            - `bytes_transferred`: Volume of data processed (in bytes).
    """
    AUDIT_DB: str = "_metadata"
    AUDIT_TABLE: str = "sync_audit"

    columns = [
        ("sync_id", "UUID"),
        ("table_id", "String"),
        ("sync_mode", "String"),
        ("dag_id", "String"),
        ("run_id", "String"),
        ("logical_date", "String"),
        ("data_interval_start", "String"),
        ("data_interval_end", "String"),
        ("ds_nodash", "String"),
        ("ts", "String"),
        ("source", "String"),
        ("destination", "String"),
        ("records_processed", "UInt64"),
        ("bytes_transferred", "UInt64"),
        ("status", "String"),
    ]

    ch_client.create_table_if_not_exists(
        database=AUDIT_DB,
        table=AUDIT_TABLE,
        columns=columns,
        engine="MergeTree()",
        order_by="(logical_date, table_id)",
    )

    sync_id = str(uuid.uuid4())
    table_id = spec["table_id"]
    dag_id = enrichment.get("dag_id", "")
    run_id = enrichment.get("run_id", "")
    logical_date = enrichment.get("logical_date", "")
    data_interval_start = enrichment.get("data_interval_start", "")
    data_interval_end = enrichment.get("data_interval_end", "")
    ds_nodash = enrichment.get("ds_nodash", "")
    ts = enrichment.get("ts", "")
    sync_mode = spec.get("sync_mode", "full")
    source = spec.get("oracle", {}).get("service", "oracle")
    destination = spec.get("clickhouse", {}).get("database", "clickhouse")
    records_processed = int(rows_inserted or 0)
    bytes_transferred = enrichment.get("bytes_transferred") or 0
    status = enrichment.get("status", "success")

    cols = [
        "sync_id",
        "table_id",
        "sync_mode",
        "dag_id",
        "run_id",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "ds_nodash",
        "ts",
        "source",
        "destination",
        "records_processed",
        "bytes_transferred",
        "status",
    ]

    vals = [
        _format_value_for_sql(sync_id),
        _format_value_for_sql(table_id),
        _format_value_for_sql(sync_mode),
        _format_value_for_sql(dag_id),
        _format_value_for_sql(run_id),
        _format_value_for_sql(logical_date),
        _format_value_for_sql(data_interval_start),
        _format_value_for_sql(data_interval_end),
        _format_value_for_sql(ds_nodash),
        _format_value_for_sql(ts),
        _format_value_for_sql(source),
        _format_value_for_sql(destination),
        _format_value_for_sql(records_processed),
        _format_value_for_sql(bytes_transferred),
        _format_value_for_sql(status),
    ]

    cols_sql = ", ".join(cols)
    vals_sql = ", ".join(vals)

    query = f"INSERT INTO {AUDIT_DB}.{AUDIT_TABLE} ({cols_sql}) VALUES ({vals_sql})"

    try:
        ch_client.execute(query)
        logging.info(
            "✅ Logged centralized audit record for %s (rows=%s, run=%s)",
            table_id,
            records_processed,
            run_id,
        )
    except Exception as e:
        logging.exception("❌ Failed to insert centralized audit record: %s", e)
        raise
