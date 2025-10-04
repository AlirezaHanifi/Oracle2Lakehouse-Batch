"""
DAG Factory for Oracle2Lakehouse-Batch.

This module dynamically generates Apache Airflow DAGs for synchronizing data
from Oracle to ClickHouse using MinIO/S3 as intermediate storage.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from airflow.sdk import dag, task
from dags.include.utils import data_handlers
from dags.include.utils.clickhouse import ClickHouseClient
from dags.include.utils.oracle import OracleClient

if TYPE_CHECKING:
    from pendulum import DateTime


def load_table_specs() -> List[Dict[str, Any]]:
    import glob
    import os

    import yaml

    base = os.environ.get("TABLE_SPEC_DIR", "/opt/airflow/dags/include/table_specs")
    files = glob.glob(f"{base}/**/*.yaml", recursive=True)
    specs: List[Dict[str, Any]] = []
    for p in files:
        with open(p, "r") as f:
            s = yaml.safe_load(f) or {}
            if s.get("enabled", True):
                specs.append(s)
    return specs


def _extract_from_oracle(
    spec: Dict[str, Any],
    data_interval_start: DateTime,
    data_interval_end: DateTime,
) -> Dict[str, Any]:
    oracle = OracleClient(conn_id=spec.get("oracle_conn", "oracle_default"))
    query = spec.get("query")

    if query:
        params = spec.get("params", {})
    else:
        time_col = spec.get("time_column")
        if time_col:
            start, end = data_interval_start, data_interval_end
            query = f"SELECT * FROM {spec['table_id']} WHERE {time_col} >= :start AND {time_col} < :end"
            params = {"start": start, "end": end}
        else:
            query = f"SELECT * FROM {spec['table_id']}"
            params = {}

    logging.info("🔄 Extracting data from Oracle table %s", spec["table_id"])
    pl_df = oracle.fetch_table(spec["table_id"], query=query, params=params)

    if pl_df is None or pl_df.is_empty():
        logging.info("⚠️ No data extracted from %s", spec["table_id"])
        return {"mode": "empty", "path": None, "data": None}

    s3_path = data_handlers.upload_df_to_minio(
        spec=spec,
        df=pl_df,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )
    logging.info("✅ Successfully extracted and uploaded data for %s", spec["table_id"])
    return {"mode": "s3", "path": s3_path, "data": None}


def _load_to_clickhouse(
    spec: Dict[str, Any],
    transformed_data: Dict[str, Any],
    data_interval_start: Optional[DateTime] = None,
    data_interval_end: Optional[DateTime] = None,
) -> Dict[str, Any]:
    """Load data from S3 to ClickHouse."""
    s3_path = data_handlers.get_s3_path_from_payload(
        spec=spec,
        payload=transformed_data,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )
    if s3_path is None:
        logging.info("⚠️ No data to load for %s", spec["table_id"])
        return {"rows_loaded": 0}

    rows = transformed_data.get("rows", 0)
    if not rows:
        logging.info("⚠️ No rows to load for %s", spec["table_id"])
        return {"rows_loaded": 0}

    minio = data_handlers.get_minio_client(spec)
    ch = ClickHouseClient(conn_id=spec.get("clickhouse_conn", "clickhouse_default"))
    creds = minio.get_credentials()

    logging.info("🔄 Starting load to ClickHouse for %s", spec["table_id"])

    ch.load_parquet_from_s3(
        database=spec["clickhouse"]["database"],
        table=spec["clickhouse"]["table"],
        s3_path=s3_path,
        s3_credentials=creds,
    )

    logging.info("✅ Loaded %d rows to ClickHouse for %s", rows, spec["table_id"])
    return {"rows_loaded": rows}


def create_dag(table_spec: Dict[str, Any]):
    import pendulum
    from pendulum import DateTime

    dag_id = f"sync_{table_spec['table_id'].replace('.', '_')}"

    @dag(
        dag_id=dag_id,
        schedule=table_spec.get("schedule", "@daily"),
        start_date=cast(
            DateTime,
            pendulum.parse(
                table_spec.get("start_date", "2025-09-01").strip(),
                strict=False,
            ),
        ).in_timezone("Asia/Tehran"),
        catchup=table_spec.get("catchup", False),
        doc_md=table_spec.get(
            "description", "Dynamically generated data synchronization DAG."
        ),
        tags=table_spec.get("tags", ["batch", "oracle-to-clickhouse"]),
        default_args={"owner": table_spec.get("owner", "airflow")},
    )
    def dynamic_sync_dag():
        from airflow.providers.standard.operators.empty import EmptyOperator

        @task
        def ensure_bucket(spec: Dict[str, Any]) -> None:
            return data_handlers.ensure_buckets(spec)

        @task
        def extract_data(spec: Dict[str, Any], **context) -> Dict[str, Any]:
            return _extract_from_oracle(
                spec, context["data_interval_start"], context["data_interval_end"]
            )

        @task
        def transform_data(
            spec: Dict[str, Any], extracted_data: Dict[str, Any], **context
        ) -> Dict[str, Any]:
            return data_handlers.transform_data(
                spec=spec,
                extracted_data=extracted_data,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )

        @task
        def load_data(
            spec: Dict[str, Any], transformed_data: Dict[str, Any], **context
        ) -> Dict[str, Any]:
            return _load_to_clickhouse(
                spec=spec,
                transformed_data=transformed_data,
                data_interval_start=context["data_interval_start"],
                data_interval_end=context["data_interval_end"],
            )

        @task
        def log_audit(spec: Dict[str, Any], load_result: Dict[str, Any]) -> None:
            from dags.include.utils.audit import log_sync_audit

            ch = ClickHouseClient(
                conn_id=spec.get("clickhouse_conn", "clickhouse_default")
            )
            log_sync_audit(spec, load_result["rows_loaded"], ch)

        start = EmptyOperator(task_id="start")
        bucket_ready = ensure_bucket(spec=table_spec)
        extracted = extract_data(spec=table_spec)
        transformed = transform_data(spec=table_spec, extracted_data=extracted)
        load_complete = load_data(spec=table_spec, transformed_data=transformed)
        audit_complete = log_audit(spec=table_spec, load_result=load_complete)
        end = EmptyOperator(task_id="end", trigger_rule="one_success")

        (
            start
            >> bucket_ready
            >> extracted
            >> transformed
            >> load_complete
            >> audit_complete
            >> end
        )

    return dynamic_sync_dag()


for spec in load_table_specs():
    dag_id = f"sync_{spec['table_id'].replace('.', '_')}"
    globals()[dag_id] = create_dag(spec)
