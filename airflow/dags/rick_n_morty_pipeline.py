from datetime import datetime, timedelta
from airflow.decorators import task, dag
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
    "start_date": datetime(2023, 1, 1),
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

def rick_and_morty_character_pipeline():

    @task()
    def checkAPIRepsonse():
        url = "https://rickandmortyapi.com/api"
        response = requests.Session().get(url)
        if response.status_code == 200:
            print("API reached.")
            pass
        else:
            raise ValueError("API not reached! Service may be down.")


    @task()
    def extractCharacterData():
        character_data = ApiRequest(BASE_URL/CHARACTER_ENDPOINT).extractData(Character)
        df_bronze_character = Transformation(character_data).bronzeCharacter()
        return df_bronze_character
    
    @task()
    def loadCharacterData(dataframe: DataFrame):
        DeltaTableManager(dataframe).writeToDeltaLake(BRONZE_CHARACTER_TABLE)
        
    @task()
    def silverTransformCharacterData():
        # silver_data
        bronze_character_data = DeltaTable(BRONZE_CHARACTER_TABLE).to_pandas()
        df_silver_character = Transformation(bronze_character_data).silverCharacter()
        DeltaTableManager(df_silver_character).writeToDeltaLake(SILVER_CHARACTER_TABLE)
    
    @task
    def goldTransformCharacterData():
        # silver_data
        silver_character_data = DeltaTable(SILVER_CHARACTER_TABLE).to_pandas()
        df_silver_character = Transformation(silver_character_data).goldCharacter()
        DeltaTableManager(df_silver_character).writeToDeltaLake(GOLD_CHARACTER_TABLE)

    checkAPIRepsonse() >> loadCharacterData(extractCharacterData()) >> [silverTransformCharacterData() >> goldTransformCharacterData()]

rick_and_morty_character_pipeline()
