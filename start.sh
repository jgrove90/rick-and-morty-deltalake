#!/bin/bash

# mkdir -p ./logs/airflow ./airflow/dags ./airflow/plugins ./airflow/config && \
# echo "AIRFLOW_UID=$(id -u)" > .env

# build docker images
# docker build --target python -t rick-and-morty-deltalake . && \
docker build --target airflow -t apache-airflow . 