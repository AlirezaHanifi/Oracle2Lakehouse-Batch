"""
Airflow DAG Generator for Oracle to MinIO Synchronization.

This script dynamically generates an Airflow DAG for each table specified in YAML configuration
files. It supports two synchronization modes: 'incremental' and 'full'.

- A branch operator determines the sync mode at runtime.
- Incremental loads append data using timestamped Parquet files.
- Full loads overwrite a single, static Parquet file for each table.
"""

import glob
import logging
from typing import Any, Dict, List, Tuple

import pendulum
import yaml
from airflow.decorators import branch_task, dag, task
from airflow.operators.empty import EmptyOperator
from dags.include.utils.minio import MinioClient
from dags.include.utils.oracle import OracleClient

TABLE_SPEC_DIR: str = "/opt/airflow/dags/include/table_specs"


def _get_clients() -> Tuple[OracleClient, MinioClient]:
    oracle_client = OracleClient()
    minio_client = MinioClient()
    minio_client.ensure_bucket("raw")
    minio_client.ensure_bucket("audit")
    return oracle_client, minio_client


def _execute_full_sync(table_spec: Dict[str, Any], ts_nodash: str) -> None:
    table_id = table_spec["table_id"]
    schema, table = table_id.split(".")
    logging.info("📥 Starting FULL sync for %s", table_id)
    oracle, minio = _get_clients()

    df = oracle.fetch_table(
        table=table_id,
        sync_mode="full",
        drop_cols=table_spec.get("drop_columns", []),
    )

    if df.empty:
        logging.warning(
            "⏹️ No data found for full sync of %s. Skipping upload.", table_id
        )
        return

    object_name = f"{schema}/{table}.parquet"
    audit_object = f"{schema}/{table}/full_load_audit_{ts_nodash}.json"

    minio.upload_parquet(bucket="raw", object_name=object_name, df=df)

    audit_record = {
        "table_id": table_id,
        "sync_mode": "full",
        "rows": len(df),
        "object": object_name,
        "status": "success",
        "synced_at": pendulum.now("Asia/Tehran").isoformat(),
    }
    minio.upload_json(bucket="audit", object_name=audit_object, data=audit_record)
    logging.info("✅ FULL sync finished for %s. Object: %s", table_id, object_name)


def _execute_incremental_sync(
    table_spec: Dict[str, Any],
    data_interval_start: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
) -> None:
    table_id = table_spec["table_id"]
    schema, table = table_id.split(".")
    logging.info("📥 Starting INCREMENTAL sync for %s", table_id)
    oracle, minio = _get_clients()

    df = oracle.fetch_table(
        table=table_id,
        sync_mode="incremental",
        incremental_key=table_spec.get("incremental_key"),
        start_ts=data_interval_start.to_datetime_string(),
        end_ts=data_interval_end.to_datetime_string(),
        drop_cols=table_spec.get("drop_columns", []),
    )

    if df.empty:
        logging.warning(
            "⏹️ No new data for incremental sync of %s. Skipping upload.", table_id
        )
        return

    object_name = f"{schema}/{table}/{data_interval_end.isoformat()}.parquet"
    audit_object = f"{schema}/{table}/{data_interval_end.isoformat()}.json"

    minio.upload_parquet(bucket="raw", object_name=object_name, df=df)

    audit_record = {
        "table_id": table_id,
        "sync_mode": "incremental",
        "rows": len(df),
        "interval_start": str(data_interval_start),
        "interval_end": str(data_interval_end),
        "object": object_name,
        "status": "success",
        "synced_at": pendulum.now("Asia/Tehran").isoformat(),
    }
    minio.upload_json(bucket="audit", object_name=audit_object, data=audit_record)
    logging.info(
        "✅ INCREMENTAL sync finished for %s. Object: %s", table_id, object_name
    )


def load_table_specs() -> List[Dict[str, Any]]:
    specs = []
    yaml_files = glob.glob(f"{TABLE_SPEC_DIR}/**/*.yaml", recursive=True)
    for file_path in yaml_files:
        with open(file_path, "r") as f:
            table_spec = yaml.safe_load(f)
            if table_spec.get("enabled", True):
                specs.append(table_spec)
    return specs


def create_sync_dag(table_spec: Dict[str, Any]):
    table_id = table_spec["table_id"]
    dag_id = f"sync_{table_id.replace('.', '_')}"

    @dag(
        dag_id=dag_id,
        schedule=table_spec.get("schedule", "@daily"),
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        default_args={"owner": "Alireza", "retries": 1},
        tags=["batch", "lakehouse", "oracle-to-minio"],
        doc_md=f"### Sync DAG for `{table_id}`\n**Sync Mode:** `{table_spec['sync_mode']}`",
    )
    def dynamic_sync_dag():
        @branch_task
        def choose_sync_mode() -> str:
            mode = table_spec["sync_mode"]
            logging.info("🔀 Sync mode for %s is '%s'.", table_id, mode)
            if mode == "incremental":
                return "incremental_sync"
            return "full_sync"

        @task(task_id="incremental_sync")
        def incremental_sync_task(
            data_interval_start: pendulum.DateTime, data_interval_end: pendulum.DateTime
        ):
            _execute_incremental_sync(
                table_spec, data_interval_start, data_interval_end
            )

        @task(task_id="full_sync")
        def full_sync_task(ts_nodash: str):
            _execute_full_sync(table_spec, ts_nodash)

        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end", trigger_rule="one_success")
        (
            start
            >> choose_sync_mode()  # type: ignore[call-arg]
            >> [incremental_sync_task(), full_sync_task()]
            >> end
        )

    return dynamic_sync_dag()


for spec in load_table_specs():
    dag_id = f"sync_{spec['table_id'].replace('.', '_')}"
    globals()[dag_id] = create_sync_dag(spec)
