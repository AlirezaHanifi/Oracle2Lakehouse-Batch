"""
Oracle Client for data extraction using Airflow connections.

This module enables:
1. Connecting to Oracle databases via Airflow hooks
2. Executing SQL queries with optional parameters
3. Returning results as Polars DataFrames
4. Handling large datasets efficiently with streaming fetch
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl


class OracleClient:
    """Client for interacting with Oracle databases."""

    def __init__(self, conn_id: str = "oracle_default"):
        """
        Initialize Oracle client using Airflow connection.

        Args:
            conn_id: Airflow connection ID for Oracle database
        """
        from airflow.providers.oracle.hooks.oracle import OracleHook

        logging.info("🟢 Connecting to Oracle with conn_id=%s", conn_id)
        self.hook = OracleHook(oracle_conn_id=conn_id)

    def fetch_table(
        self,
        table: str,
        query: Optional[str] = None,
        columns: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> pl.DataFrame:
        """
        Fetch data from an Oracle table using optional custom query and parameters.

        Args:
            table: Table name (schema.table format)
            query: Optional custom SQL query template. Use :param_name for parameters
            columns: Optional list of columns to select
            params: Optional parameters for the query

        Returns:
            Polars DataFrame containing the query results

        Examples:
            # Simple table fetch
            df = client.fetch_table("schema.table")

            # Custom query with parameters
            df = client.fetch_table(
                "schema.table",
                query="SELECT * FROM :table WHERE created_at > :start_date",
                params={"start_date": "2025-01-01"}
            )
        """
        import polars as pl

        logging.info("📥 Fetching data from table=%s", table)

        if query is None:
            cols = ", ".join(columns) if columns else "*"
            query = f"SELECT {cols} FROM {table}"

        try:
            bind_params = params or {}
            if ":table" in query:
                bind_params["table"] = table

            with self.hook.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, bind_params)
                columns = [col[0].lower() for col in cursor.description]
                data = cursor.fetchall()

            df = pl.DataFrame(data, schema=columns, orient="row")

            logging.info(
                "✅ Query completed successfully (%d rows, %d columns)",
                len(df),
                len(df.columns),
            )
            return df

        except Exception as e:
            logging.error("❌ Query failed: %s", str(e))
            raise
