"""
ClickHouse utility module for Oracle2Lakehouse-Batch.

This module provides functionality to:
1. Manage ClickHouse connections
2. Create and modify tables
3. Load data from S3/MinIO
4. Execute queries and fetch results
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union


class ClickHouseClient:
    """Client for interacting with ClickHouse database.

    This wraps the underlying clickhouse-connect client and provides a few
    convenience helpers used across the DAGs.
    """

    def __init__(self, conn_id: str = "clickhouse_default"):
        import clickhouse_connect
        from airflow.sdk import Connection as SDKConnection

        conn = SDKConnection.get(conn_id)
        params = {
            "host": str(conn.host) if conn.host else "localhost",
            "port": int(conn.port) if conn.port else 8123,
            "username": str(conn.login) if conn.login else None,
            "password": str(conn.password) if conn.password else None,
        }
        if conn.extra_dejson:
            params.update(conn.extra_dejson)

        self.client = clickhouse_connect.get_client(**params)
        self._client_params = params.copy()
        logging.info(
            "🔵 Connected to ClickHouse at %s:%s", params["host"], params["port"]
        )

    def execute(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Execute a query and return the raw client response.

        We don't try to coerce results here; callers should use helpers below
        when they expect specific shapes (scalars, rows, etc.).
        """
        logging.debug("🔎 Executing ClickHouse query: %s", query.replace("\n", " "))
        try:
            return self.client.command(query, parameters=parameters, settings=settings)
        except Exception as exc:
            logging.exception("❌ ClickHouse query failed: %s", exc)
            raise

    def _unwrap_scalar(self, result: Any) -> Optional[Any]:
        """Return a single scalar from common client return shapes.

        Accepts:
        - direct scalar (int/float/str)
        - list/tuple with first element either a row (list/tuple) or mapping
        - list/tuple of scalars
        - dict-like first element
        - None -> None
        """
        if result is None:
            return None
        if isinstance(result, (int, float, str)):
            return result

        # list/tuple result: take first element
        try:
            first = result[0]
        except Exception:
            return None

        # row-like [(val,)] or [[val]]
        if isinstance(first, (list, tuple)) and first:
            return first[0]

        # mapping-like [{'count()': 1}]
        if isinstance(first, dict):
            vals = list(first.values())
            return vals[0] if vals else None

        # flat list of scalars
        if isinstance(result, (list, tuple)) and result:
            return result[0]

        return None

    def _ensure_database(self, database: str) -> None:
        logging.debug("📁 Ensuring database exists: %s", database)
        self.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

    def _table_exists(self, database: str, table: str) -> bool:
        query = (
            f"SELECT count() FROM system.tables WHERE database = '{database}' "
            f"AND name = '{table}'"
        )
        raw = self.execute(query)
        cnt = self._unwrap_scalar(raw)
        exists = bool(cnt)
        logging.debug(
            "🔎 Table exists check %s.%s -> %s (count=%s)", database, table, exists, cnt
        )
        return exists

    def _get_table_count(self, database: str, table: str) -> int:
        try:
            if not self._table_exists(database, table):
                logging.debug(
                    "⚠️ Table %s.%s does not exist when counting -> 0", database, table
                )
                return 0
        except Exception:
            logging.debug(
                "⚠️ Table existence check failed for %s.%s; attempting count",
                database,
                table,
            )

        query = f"SELECT count() FROM {database}.{table}"
        try:
            raw = self.execute(query)
        except Exception as exc:
            msg = str(exc)
            if "UNKNOWN_TABLE" in msg or "Unknown table" in msg or "Code: 60" in msg:
                logging.debug(
                    "❌ Count failed because table missing %s.%s: %s",
                    database,
                    table,
                    msg,
                )
                return 0
            raise

        val = self._unwrap_scalar(raw)
        try:
            return int(val) if val is not None else 0
        except (ValueError, TypeError):
            logging.warning("⚠️ Could not coerce table count to int: %r", val)
            return 0

    def create_table_if_not_exists(
        self,
        database: str,
        table: str,
        columns: List[Tuple[str, str]],
        engine: str = "MergeTree()",
        order_by: Union[str, List[str]] = "tuple()",
        partition_by: Optional[str] = None,
        settings: Optional[Dict[str, str]] = None,
    ) -> None:
        """Create a table with specified columns if it does not exist.

        This method constructs a CREATE TABLE IF NOT EXISTS statement and runs it.
        Use this when you already know the schema. For schema inference from
        Parquet we rely on ClickHouse's CREATE ... AS SELECT behaviour in
        `load_parquet_from_s3`.
        """
        self._ensure_database(database)

        columns_sql = ",\n    ".join(f"{name} {type_}" for name, type_ in columns)
        order_by_sql = (
            f"({', '.join(order_by)})" if isinstance(order_by, list) else order_by
        )

        query = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table} (
            {columns_sql}
        ) ENGINE = {engine}
        """

        if partition_by:
            query += f"\nPARTITION BY {partition_by}"

        query += f"\nORDER BY {order_by_sql}"

        if settings:
            settings_sql = ",\n".join(f"{k}={v}" for k, v in settings.items())
            query += f"\nSETTINGS\n{settings_sql}"

        logging.info("📦 Creating/ensuring table %s.%s", database, table)
        self.execute(query)

    def load_parquet_from_s3(
        self,
        database: str,
        table: str,
        s3_path: str,
        s3_credentials: Dict[str, str],
        expected_rows: Optional[int] = None,
    ) -> int:
        """Load a Parquet file from S3/MinIO into a ClickHouse table.

        Behaviour:
        - If table does not exist, create it from the Parquet schema via
          CREATE TABLE ... AS SELECT FROM s3(...)
        - Otherwise INSERT INTO ... SELECT FROM s3(...)

        Returns the number of rows inserted according to `expected_rows` when
        provided, otherwise returns the delta in table row count (best effort).
        """
        if not s3_path.startswith("s3://"):
            raise ValueError("s3_path must start with s3://")

        _, rest = s3_path.split("s3://", 1)
        bucket, key = rest.split("/", 1)
        endpoint = s3_credentials.get("endpoint_url", "minio:9000").rstrip("/")
        endpoint = f"http://{endpoint}" if not endpoint.startswith("http") else endpoint
        url = f"{endpoint}/{bucket}/{key}"
        access = s3_credentials.get("access_key_id", "")
        secret = s3_credentials.get("secret_access_key", "")

        logging.info(
            "📥 Starting ClickHouse load from S3: %s -> %s.%s", url, database, table
        )

        self._ensure_database(database)

        before_count = (
            self._get_table_count(database, table) if expected_rows is None else None
        )

        exists = self._table_exists(database, table)
        if not exists:
            query = f"""
            CREATE TABLE {database}.{table}
            ENGINE = MergeTree
            ORDER BY tuple() AS
            SELECT *
            FROM s3(
                '{url}',
                '{access}',
                '{secret}',
                'Parquet'
            )"""
            logging.info(
                "📦 Table %s.%s does not exist. Creating from Parquet schema.",
                database,
                table,
            )
        else:
            query = f"""
            INSERT INTO {database}.{table}
            SELECT *
            FROM s3(
                '{url}',
                '{access}',
                '{secret}',
                'Parquet'
            )"""
            logging.info(
                "📥 Table %s.%s exists. Inserting data from Parquet.", database, table
            )

        self.execute(query)

        if expected_rows is not None:
            logging.info(
                "✅ Assuming %d rows were inserted (provided by upstream).",
                expected_rows,
            )
            return expected_rows

        after_count = self._get_table_count(database, table)
        inserted = max(0, after_count - (before_count or 0))
        logging.info(
            "✅ Load complete. Table rows before=%s after=%s inserted=%s",
            before_count,
            after_count,
            inserted,
        )
        return inserted
