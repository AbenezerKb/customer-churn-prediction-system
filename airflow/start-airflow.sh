#!/bin/bash

set -e

export AIRFLOW_HOME=${PWD}/airflow

export AIRFLOW__LOGGING__BASE_LOG_FOLDER=${PWD}/airflow/logs
export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/airflow/dags

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$(cat /run/secrets/a_postgres_user):$(cat /run/secrets/a_postgres_password)@${A_POSTGRES_HOST}:${A_POSTGRES_PORT}/$(cat /run/secrets/a_postgres_db)"

export AIRFLOW__CORE__EXECUTOR=SequentialExecutor

export AIRFLOW_CONN_POSTGRES_TELECOM_DB="postgres://$(cat /run/secrets/r_postgres_user):$(cat /run/secrets/r_postgres_password)@${R_POSTGRES_HOST}:${R_POSTGRES_PORT}/$(cat /run/secrets/r_postgres_db)"

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES


airflow db migrate

python3 migration.py

mkdir -p "${AIRFLOW_HOME}/logs"

airflow users create --username $(cat /run/secrets/airflow_username) --firstname $(cat /run/secrets/airflow_firstname) --lastname $(cat /run/secrets/airflow_lastname) --role $(cat /run/secrets/airflow_role) --email $(cat /run/secrets/airflow_email) --password $(cat /run/secrets/airflow_password)

airflow scheduler &

airflow api-server --port 8080 &

airflow dags reserialize &

wait