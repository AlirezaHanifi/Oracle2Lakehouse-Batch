import logging

import pandas as pd
from airflow.providers.oracle.hooks.oracle import OracleHook


class OracleClient:
    def __init__(self, conn_id: str = "oracle_default"):
        logging.info("🟢 Connecting to Oracle with conn_id=%s", conn_id)
        self.hook = OracleHook(oracle_conn_id=conn_id)

    def build_full_query(self, table: str, columns: list[str] | None = None) -> str:
        select_cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {select_cols} FROM {table}"
        logging.info("📝 Built full query for table=%s", table)
        return query

    def build_incremental_query(
        self,
        table: str,
        incremental_key: str,
        start_ts: str,
        end_ts: str,
        columns: list[str] | None = None,
    ) -> str:
        select_cols = ", ".join(columns) if columns else "*"
        query = (
            f"SELECT {select_cols} FROM {table} "
            f"WHERE {incremental_key} >= TO_TIMESTAMP('{start_ts}', 'YYYY-MM-DD HH24:MI:SS') "
            f"AND {incremental_key} < TO_TIMESTAMP('{end_ts}', 'YYYY-MM-DD HH24:MI:SS')"
        )
        logging.info(
            "📝 Built incremental query for table=%s (key=%s, start=%s, end=%s)",
            table,
            incremental_key,
            start_ts,
            end_ts,
        )
        return query

    def run_query(self, query: str, chunksize: int | None = None) -> pd.DataFrame:
        logging.info("▶️ Running query (chunksize=%s)", chunksize)
        try:
            with self.hook.get_conn() as conn:
                if chunksize:
                    dfs = []
                    for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                        logging.info("📄 Retrieved chunk with rows=%d", len(chunk))
                        dfs.append(chunk)
                    df = pd.concat(dfs, ignore_index=True)
                else:
                    df = pd.read_sql(query, conn)
                df.columns = [col.lower() for col in df.columns]
            logging.info("✅ Query finished successfully (%d rows)", len(df))
            return df
        except Exception as e:
            logging.error("❌ Query failed: %s", e)
            raise

    def drop_columns(self, df: pd.DataFrame, drop_cols: list[str]) -> pd.DataFrame:
        drop_cols = [col.lower() for col in drop_cols]
        before_cols = set(df.columns)
        df = df.drop(columns=drop_cols, errors="ignore")
        after_cols = set(df.columns)
        dropped = before_cols - after_cols
        if dropped:
            logging.info("🧹 Dropped columns: %s", list(dropped))
        return df

    def fetch_table(
        self,
        table: str,
        sync_mode: str = "full",
        incremental_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        drop_cols: list[str] | None = None,
        chunksize: int | None = None,
    ) -> pd.DataFrame:
        logging.info("📥 Fetching table=%s in %s mode", table, sync_mode)

        if sync_mode == "full":
            query = self.build_full_query(table)
        elif sync_mode == "incremental":
            if not incremental_key or not start_ts or not end_ts:
                raise ValueError(
                    "Incremental mode requires incremental_key, start_ts, and end_ts"
                )
            query = self.build_incremental_query(
                table, incremental_key, start_ts, end_ts
            )
        else:
            raise ValueError(f"Unsupported sync_mode: {sync_mode}")

        df = self.run_query(query, chunksize)

        if drop_cols:
            df = self.drop_columns(df, drop_cols)

        logging.info(
            "📦 Finished fetching table=%s (%d rows, %d columns)",
            table,
            len(df),
            len(df.columns),
        )
        return df
