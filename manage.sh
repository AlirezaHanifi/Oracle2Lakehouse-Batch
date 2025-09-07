#!/bin/bash
set -e

AIRFLOW_SERVICE_NAME="airflow-scheduler"
ORACLE_SERVICE_NAME="FREEPDB1"

start_and_configure() {
    echo "🚀 Starting Docker Compose services..."
    docker compose up -d
    echo "✅ Docker services are up and running."

    echo " sourcing .env file..."
    if [ ! -f .env ]; then
        echo "❌ Error: .env file not found. Please create one in the current directory."
        exit 1
    fi
    source .env

    echo "⏳ Waiting for the Airflow scheduler to be healthy..."
    MAX_RETRIES=10
    RETRY_COUNT=0
    while ! docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow db check &> /dev/null; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo "❌ Airflow scheduler is not ready after several attempts. Aborting."
            exit 1
        fi
        echo "   ... Airflow DB not ready, waiting 15 seconds (Attempt ${RETRY_COUNT}/${MAX_RETRIES})"
        sleep 15
    done
    echo "✅ Airflow is ready."

    echo "🔗 Adding 'minio_default' connection..."
    docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow connections add 'minio_default' \
        --conn-type 'aws' \
        --conn-extra "{\"aws_access_key_id\": \"$MINIO_ROOT_USER\", \"aws_secret_access_key\": \"$MINIO_ROOT_PASSWORD\", \"endpoint_url\": \"minio:$MINIO_HOST_PORT_API\"}" 2> /dev/null

    echo "🔗 Adding 'oracle_default' connection..."
    docker compose exec -T "$AIRFLOW_SERVICE_NAME" airflow connections add 'oracle_default' \
        --conn-type 'oracle' \
        --conn-host 'oracle' \
        --conn-login "$ORACLE_APP_USER" \
        --conn-password "$ORACLE_APP_USER_PASSWORD" \
        --conn-port '1521' \
        --conn-extra "{\"service_name\": \"${ORACLE_SERVICE_NAME}\"}" 2> /dev/null

    echo -e "\n🎉 Success! Services are running and connections have been added to Airflow."
}

stop_services() {
    echo "🛑 Stopping and removing Docker Compose services..."
    docker compose down -v
    echo "✅ All services have been stopped and cleaned up."
}


COMMAND=$1

case "$COMMAND" in
    up|"")
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