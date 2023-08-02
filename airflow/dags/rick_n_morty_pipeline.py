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


    with TaskGroup('extractBronzeData') as bronze_group:
        # character
        @task()
        def extractCharacterData():
            character_data = ApiRequest(BASE_URL/CHARACTER_ENDPOINT).extractData(Character)
            return Transformation(character_data).Character().bronze()
        
        @task()
        def loadCharacterData(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(BRONZE_CHARACTER_TABLE)
        
        # location
        @task()
        def extractLocationData():
            location_data = ApiRequest(BASE_URL/LOCATION_ENDPOINT).extractData(Location) 
            return Transformation(location_data).Location().bronze()
        
        @task()
        def loadLocationData(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(BRONZE_LOCATION_TABLE)

        # episode
        @task()
        def extractEpisodeData():
            episode_data = ApiRequest(BASE_URL/EPISODE_ENDPOINT).extractData(Episode) 
            return Transformation(episode_data).Episode().bronze()
        
        @task()
        def loadEpisodeData(dataframe: DataFrame):
            DeltaTableManager(dataframe).writeToDeltaLake(BRONZE_EPISODE_TABLE)

        loadCharacterData(extractCharacterData())
        loadLocationData(extractLocationData())
        loadEpisodeData(extractEpisodeData())
    
        
    with TaskGroup('transformSilverData') as silver_group:        
        @task()
        def silverTransformCharacterData():
            bronze_character_data = DeltaTable(BRONZE_CHARACTER_TABLE).to_pandas()
            df_silver_character = Transformation(bronze_character_data).Character().silver()
            DeltaTableManager(df_silver_character).writeToDeltaLake(SILVER_CHARACTER_TABLE)

        @task()
        def silverTransformLocationData():
            bronze_location_data = DeltaTable(BRONZE_LOCATION_TABLE).to_pandas()
            df_silver_location = Transformation(bronze_location_data).Location().silver()
            DeltaTableManager(df_silver_location).writeToDeltaLake(SILVER_LOCATION_TABLE)

        @task()
        def silverTransformEpisodeData():
            bronze_episode_data = DeltaTable(BRONZE_EPISODE_TABLE).to_pandas()
            df_silver_episode = Transformation(bronze_episode_data).Episode().silver()
            DeltaTableManager(df_silver_episode).writeToDeltaLake(SILVER_EPISODE_TABLE)

        silverTransformCharacterData()
        silverTransformLocationData()
        silverTransformEpisodeData()


    with TaskGroup('transformGoldData') as gold_group:        
        @task()
        def goldTransformCharacterData():
            silver_character_data = DeltaTable(SILVER_CHARACTER_TABLE).to_pandas()
            df_gold_character = Transformation(silver_character_data).Character().gold()
            DeltaTableManager(df_gold_character).writeToDeltaLake(GOLD_CHARACTER_TABLE)

        @task()
        def goldTransformLocationData():
            silver_location_data = DeltaTable(SILVER_LOCATION_TABLE).to_pandas()
            df_gold_location = Transformation(silver_location_data).Location().gold()
            DeltaTableManager(df_gold_location).writeToDeltaLake(GOLD_LOCATION_TABLE)

        @task()
        def goldTransformEpisodeData():
            silver_episode_data = DeltaTable(SILVER_EPISODE_TABLE).to_pandas()
            df_gold_episode = Transformation(silver_episode_data).Episode().gold()
            DeltaTableManager(df_gold_episode).writeToDeltaLake(GOLD_EPISODE_TABLE)

        goldTransformCharacterData()
        goldTransformLocationData()
        goldTransformEpisodeData()

        @task()
        def updateFactTable():
            fact_table = Transformation.factTable()
            DeltaTableManager(fact_table).writeToDeltaLake(FACT_TABLE)
    

    checkAPIRepsonse() >> bronze_group >> silver_group >> gold_group >> updateFactTable()
    
rick_and_morty_character_pipeline()
