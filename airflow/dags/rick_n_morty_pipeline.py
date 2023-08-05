from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
import sys

sys.path.append("/opt/airflow/src")

from app_utils import *
from transform import *
from load import *
from data_classes import *


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "schedule_interval": None,
    "start_date": datetime.now(),
    "email": ["test@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    "rick_and_morty_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",
)
def rick_and_morty_pipeline():
    @task(task_id=f"Check_API_Response")
    def checkAPIRepsonse():
        url = "https://rickandmortyapi.com/api"
        response = requests.Session().get(url)
        if response.status_code == 200:
            print("API reached.")
        else:
            raise ValueError("API not reached! Service may be down.")

    def extract_and_load_bronze_data(endpoint, dataclass_instance, bronze_table):
        @task(task_id=f"Extract_{endpoint.capitalize()}")
        def extract_data():
            data = ApiRequest(f"{BASE_URL}/{endpoint}").extractData(dataclass_instance)
            if endpoint == CHARACTER_ENDPOINT:
                return CharacterTransformation(data).bronze()
            elif endpoint == LOCATION_ENDPOINT:
                return LocationTransformation(data).bronze()
            elif endpoint == EPISODE_ENDPOINT:
                return EpisodeTransformation(data).bronze()

        @task(task_id=f"Load_{endpoint.capitalize()}")
        def load_data(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(bronze_table)

        return load_data(extract_data())

    def transform_and_load_silver_data(bronze_table, silver_table):
        lable = bronze_table.split("/")[-1].capitalize()

        @task(task_id=f"Transform_{lable}")
        def transform_data():
            bronze_data = DeltaTable(bronze_table).to_pandas()

            if bronze_table == BRONZE_CHARACTER_TABLE:
                df_silver_data = CharacterTransformation(bronze_data).silver()
                DeltaTableManager(df_silver_data).writeToDeltaLake(silver_table)
                return df_silver_data
            elif bronze_table == BRONZE_LOCATION_TABLE:
                df_silver_data = LocationTransformation(bronze_data).silver()
                DeltaTableManager(df_silver_data).writeToDeltaLake(silver_table)
                return df_silver_data
            elif bronze_table == BRONZE_EPISODE_TABLE:
                df_silver_data = EpisodeTransformation(bronze_data).silver()
                DeltaTableManager(df_silver_data).writeToDeltaLake(silver_table)
                return df_silver_data

        @task(task_id=f"Load_{lable}")
        def load_data(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(silver_table)

        return load_data(transform_data())

    def transform_and_load_gold_data(silver_table, gold_table):
        lable = silver_table.split("/")[-1].capitalize()

        @task(task_id=f"Transform_{lable}")
        def transform_data():
            silver_data = DeltaTable(silver_table).to_pandas()
            if silver_table == SILVER_CHARACTER_TABLE:
                df_gold_data = CharacterTransformation(silver_data).gold()
                return df_gold_data
            elif silver_table == SILVER_LOCATION_TABLE:
                df_gold_data = LocationTransformation(silver_data).gold()
                return df_gold_data
            elif silver_table == SILVER_EPISODE_TABLE:
                df_gold_data = EpisodeTransformation(silver_data).gold()
                return df_gold_data

        @task(task_id=f"Load_{lable}")
        def load_data(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(gold_table)

        return load_data(transform_data())

    def create_data_model(gold_table: str):
        lable = gold_table.split("/")[-1].capitalize()

        @task(task_id=f"{lable}")
        def insert_fact_table():
            fact_table = DataModel.factTable()
            return fact_table

        @task(task_id=f"Load_{lable}")
        def load_data(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(gold_table)

        return load_data(insert_fact_table())

    with TaskGroup("Bronze_Layer") as bronze_group:
        extract_and_load_bronze_data(
            CHARACTER_ENDPOINT, Character, BRONZE_CHARACTER_TABLE
        )
        extract_and_load_bronze_data(LOCATION_ENDPOINT, Location, BRONZE_LOCATION_TABLE)
        extract_and_load_bronze_data(EPISODE_ENDPOINT, Episode, BRONZE_EPISODE_TABLE)

    with TaskGroup("Silver_Layer") as silver_group:
        transform_and_load_silver_data(BRONZE_CHARACTER_TABLE, SILVER_CHARACTER_TABLE)
        transform_and_load_silver_data(BRONZE_LOCATION_TABLE, SILVER_LOCATION_TABLE)
        transform_and_load_silver_data(BRONZE_EPISODE_TABLE, SILVER_EPISODE_TABLE)

    with TaskGroup("Gold_Layer") as gold_group:
        with TaskGroup("Core_Tables") as core_table:
            transform_and_load_gold_data(SILVER_CHARACTER_TABLE, GOLD_CHARACTER_TABLE)
            transform_and_load_gold_data(SILVER_LOCATION_TABLE, GOLD_LOCATION_TABLE)
            transform_and_load_gold_data(SILVER_EPISODE_TABLE, GOLD_EPISODE_TABLE)
        with TaskGroup("Data_Models") as data_model:
            create_data_model(FACT_TABLE)

        core_table >> data_model

    @task(task_id="Export_Gold_Tables_to_CSV")
    def export_delta_tables():
        return delta_tables_to_csv("./deltalake/rick_and_morty/gold")

    (
        checkAPIRepsonse()
        >> bronze_group
        >> silver_group
        >> gold_group
        >> export_delta_tables()
    )


rick_and_morty_pipeline()
