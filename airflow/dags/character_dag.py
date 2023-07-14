from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append("/opt/airflow/src")

from dag_operators import *

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "schedule_interval": None,
    "start_date": datetime(2023, 1, 1),
    "email": ["test@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "rick_and_morty_character_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
)

with dag:
    extract_task = PythonOperator(
        task_id="extract_character_data",
        python_callable=extractCharacterData,
        provide_context=True,
    )
    load_task = PythonOperator(
        task_id="load_character_data",
        python_callable=loadCharacterData,
        provide_context=True,
    )
    transform_silver_task = PythonOperator(
        task_id="transform_character_data_silver",
        python_callable=silverTransformCharacterData,
        provide_context=True,
    )

    extract_task >> load_task >> transform_silver_task
