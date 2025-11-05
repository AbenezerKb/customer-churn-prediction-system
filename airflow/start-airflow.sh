#!/bin/bash

set -e

export AIRFLOW_HOME=${PWD}/airflow

export AIRFLOW__LOGGING__BASE_LOG_FOLDER=${PWD}/airflow/logs
export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/airflow/dags


export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${A_POSTGRES_USER}:${A_POSTGRES_PASSWORD}@${A_POSTGRES_HOST}:${A_POSTGRES_PORT}/${A_POSTGRES_DB}"

export AIRFLOW__CORE__EXECUTOR=SequentialExecutor

export AIRFLOW_CONN_POSTGRES_TELECOM_DB="postgres://${R_POSTGRES_USER}:${R_POSTGRES_PASSWORD}@${R_POSTGRES_HOST}:${R_POSTGRES_PORT}/${R_POSTGRES_DB}"

export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

python3 migration.py

airflow db migrate

mkdir -p "${AIRFLOW_HOME}/logs"

airflow users create --username ${AIRFLOW_USERNAME} --firstname ${AIRFLOW_FIRSTNAME} --lastname ${AIRFLOW_LASTNAME} --role ${AIRFLOW_ROLE} --email ${AIRFLOW_EMAIL} --password ${AIRFLOW_PASSWORD}

airflow scheduler &

airflow api-server --port 8080 &

airflow dags reserialize &

wait