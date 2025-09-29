ARG AIRFLOW_IMAGE_NAME="apache/airflow:3.0.0"
FROM ${AIRFLOW_IMAGE_NAME}

ENV PYTHONTWRITEBYTECODE=1
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"

USER airflow

COPY ./docker/airflow/requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt