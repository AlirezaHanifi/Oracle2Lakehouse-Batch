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

import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

TABLE_SPEC_DIR = "/opt/airflow/dags/include/table_specs"


def _execute_full_sync(table_spec, ts_nodash, **context):
    from dags.include.utils.audit import AuditClient
    from dags.include.utils.iceberg import IcebergClient
    from dags.include.utils.oracle import OracleClient

    table_id = table_spec["table_id"]
    logging.info("📥 Starting FULL sync for %s", table_id)

    oracle_client = OracleClient()
    iceberg_client = IcebergClient()
    audit_client = AuditClient(minio_endpoint="http://minio:9000", bucket="audit")

    df = oracle_client.fetch_table(
        table=table_id,
        sync_mode="full",
        drop_cols=table_spec.get("drop_columns", []),
    )

    if df.empty:
        logging.warning(
            "⏹️ No data found for full sync of %s. Skipping write.", table_id
        )
        audit_client.write(
            table_id=table_id,
            row_count=0,
            sync_mode="full",
            status="skipped",
            details="No data found",
            data_interval_end=context["data_interval_end"].isoformat()
            if context.get("data_interval_end")
            else None,
        )
        return

    table_path = iceberg_client.write(
        table_id=table_id,
        df=df,
        drop_cols=table_spec.get("drop_columns", []),
    )

    audit_client.write(
        table_id=table_id,
        row_count=len(df),
        sync_mode="full",
        status="success",
        data_interval_end=context["data_interval_end"].isoformat()
        if context.get("data_interval_end")
        else None,
        details=f"Written to {table_path}",
    )
    logging.info("✅ FULL sync finished for %s", table_id)


def _execute_incremental_sync(table_spec, data_interval_start, data_interval_end):
    from dags.include.utils.audit import AuditClient
    from dags.include.utils.iceberg import IcebergClient
    from dags.include.utils.oracle import OracleClient

    table_id = table_spec["table_id"]
    logging.info("📥 Starting INCREMENTAL sync for %s", table_id)

    oracle_client = OracleClient()
    iceberg_client = IcebergClient()
    audit_client = AuditClient(minio_endpoint="http://minio:9000", bucket="audit")

    df = oracle_client.fetch_table(
        table=table_id,
        sync_mode="incremental",
        incremental_key=table_spec.get("incremental_key"),
        start_ts=data_interval_start.to_datetime_string(),
        end_ts=data_interval_end.to_datetime_string(),
        drop_cols=table_spec.get("drop_columns", []),
    )

    if df.empty:
        logging.warning(
            "⏹️ No new data for incremental sync of %s. Skipping write.", table_id
        )
        audit_client.write(
            table_id=table_id,
            row_count=0,
            sync_mode="incremental",
            status="skipped",
            details="No new data found",
            data_interval_start=data_interval_start.isoformat(),
            data_interval_end=data_interval_end.isoformat(),
        )
        return

    table_path = iceberg_client.write(
        table_id=table_id,
        df=df,
        drop_cols=table_spec.get("drop_columns", []),
    )

    audit_client.write(
        table_id=table_id,
        row_count=len(df),
        sync_mode="incremental",
        status="success",
        data_interval_start=data_interval_start.isoformat(),
        data_interval_end=data_interval_end.isoformat(),
        details=f"Written to {table_path}",
    )
    logging.info("✅ INCREMENTAL sync finished for %s", table_id)


def load_table_specs():
    specs = []
    yaml_files = glob.glob(f"{TABLE_SPEC_DIR}/**/*.yaml", recursive=True)
    for file_path in yaml_files:
        with open(file_path, "r") as f:
            table_spec = yaml.safe_load(f)
            if table_spec.get("enabled", True):
                specs.append(table_spec)
    return specs


@task(task_id="ensure_buckets")
def ensure_buckets():
    from dags.include.utils.minio import MinioClient

    minio_client = MinioClient()
    minio_client.ensure_bucket("raw")


@task.branch
def choose_sync_mode(table_spec):
    table_id = table_spec["table_id"]
    mode = table_spec["sync_mode"]
    logging.info("🔀 Sync mode for %s is '%s'.", table_id, mode)
    if mode == "incremental":
        return "incremental_sync"
    return "full_sync"


@task(task_id="incremental_sync")
def incremental_sync_task(table_spec, data_interval_start, data_interval_end):
    _execute_incremental_sync(table_spec, data_interval_start, data_interval_end)


@task(task_id="full_sync")
def full_sync_task(table_spec, ts_nodash, **context):
    _execute_full_sync(table_spec, ts_nodash, **context)


def create_sync_dag(table_spec):
    table_id = table_spec["table_id"]
    schema, table = table_id.split(".")
    dag_id = f"sync_{table_id.replace('.', '_')}"

    @dag(
        dag_id=dag_id,
        schedule=table_spec.get("schedule", "@daily"),
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        default_args={"owner": "Alireza"},
        tags=["batch", "lakehouse", "oracle-to-iceberg"],
        doc_md=(
            f"### Sync DAG for `{table_id}`\n"
            f"**Sync Mode:** `{table_spec['sync_mode']}`\n"
            "**Storage:** Apache Iceberg"
        ),
    )
    def dynamic_sync_dag():
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end", trigger_rule="one_success")

        create_buckets = ensure_buckets()

        branch_task = choose_sync_mode(table_spec=table_spec)
        incremental_task_instance = incremental_sync_task(table_spec=table_spec)
        full_task_instance = full_sync_task(table_spec=table_spec)

        (
            start
            >> create_buckets
            >> branch_task
            >> [incremental_task_instance, full_task_instance]
        )

        [incremental_task_instance, full_task_instance] >> end

    return dynamic_sync_dag()


for spec in load_table_specs():
    dag_id = f"sync_{spec['table_id'].replace('.', '_')}"
    globals()[dag_id] = create_sync_dag(spec)
