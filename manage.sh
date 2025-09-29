#!/bin/bash
set -e

AIRFLOW_SERVICE_NAME="airflow-scheduler"
ORACLE_SERVICE_NAME="FREEPDB1"

start_and_configure() {
    echo "🚀 Starting Docker Compose services..."
    docker compose up -d
    echo "✅ Docker services are up and running."

    echo "⏳ Waiting for MinIO to be ready..."
    until curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do
        echo "   ... MinIO not ready yet, waiting 2 seconds"
        sleep 2
    done
    echo "✅ MinIO is ready."

    echo " Sourcing .env file..."
    if [ ! -f .env ]; then
        echo "❌ Error: .env file not found. Please create one in the current directory."
        exit 1
    fi
    source .env

    echo "⏳ Waiting for the Airflow scheduler to be healthy..."
    MAX_RETRIES=20
    RETRY_COUNT=0
    while ! docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow db check &>/dev/null; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo "❌ Airflow scheduler is not ready after several attempts. Aborting."
            exit 1
        fi
        echo "   ... Airflow DB not ready, waiting 2 seconds (Attempt ${RETRY_COUNT}/${MAX_RETRIES})"
        sleep 2
    done
    echo "✅ Airflow is ready."

    echo "🔗 Adding Airflow connections in parallel..."

    docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow connections add 'minio_default' \
        --conn-type 'aws' \
        --conn-extra '{
            "aws_access_key_id": "'"$MINIO_ROOT_USER"'",
            "aws_secret_access_key": "'"$MINIO_ROOT_PASSWORD"'",
            "endpoint_url": "minio:9000"
        }' >/dev/null 2>&1 &

    docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow connections add 'oracle_default' \
        --conn-type 'oracle' \
        --conn-host 'oracle' \
        --conn-login "$ORACLE_APP_USER" \
        --conn-password "$ORACLE_APP_USER_PASSWORD" \
        --conn-port '1521' \
        --conn-extra '{
            "service_name": "'"$ORACLE_SERVICE_NAME"'"
        }' >/dev/null 2>&1 &

    docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow connections add 'iceberg_default' \
        --conn-type 'iceberg' \
        --conn-host 'iceberg-rest' \
        --conn-port '8181' \
        --conn-login "$MINIO_ROOT_USER" \
        --conn-password "$MINIO_ROOT_PASSWORD" \
        --conn-extra '{
            "catalog_type": "rest",
            "warehouse_path": "s3://raw",
            "s3_endpoint_url": "http://minio:9000",
            "catalog_impl": "org.apache.iceberg.rest.RESTCatalog",
            "io_impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.access-key-id": "'"$MINIO_ROOT_USER"'",
            "s3.secret-access-key": "'"$MINIO_ROOT_PASSWORD"'",
            "s3.endpoint": "http://minio:9000",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
            "s3.ssl-enabled": "false"
        }' >/dev/null 2>&1 &

    wait
    echo "✅ All connections have been added."

    echo -e "\n🎉 Success! Services are running and connections have been added to Airflow."
}

stop_services() {
    echo "🛑 Stopping and removing Docker Compose services..."
    docker compose down -v
    echo "✅ All services have been stopped and cleaned up."
}

COMMAND=$1

case "$COMMAND" in
up | "")
    start_and_configure
    ;;
down)
    stop_services
    ;;
*)
    echo "❌ Error: Invalid command '$COMMAND'"
    echo "Usage: $0 [up|down]"
    exit 1
    ;;
esac