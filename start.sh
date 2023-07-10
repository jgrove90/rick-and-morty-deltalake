#!/bin/bash

mkdir -p ./airflow/logs ./airflow/dags ./airflow/plugins ./airflow/config && \
echo "AIRFLOW_UID=$(id -u)" > .env