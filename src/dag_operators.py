from app_utils import *
from transformations import *
from table_manager import *
from data_classes import *
from deltalake import DeltaTable

# CHARACTER PIPELINE
def extractCharacterData(**context):
    character_data = ApiRequest(BASE_URL/CHARACTER_ENDPOINT).extractData(Characters)
    df_bronze_character = Transformations(character_data).bronzeCharacter()
    context['ti'].xcom_push(key='character_data', value=df_bronze_character)

def loadCharacterData(**context):
    df_character_data = context['ti'].xcom_pull(key='character_data')
    DeltaTableManager(df_character_data).writeToDeltaLake(BRONZE_CHARACTERS_TABLE)
    
def silverTransformCharacterData():
    # silver_data
    bronze_character_data = DeltaTable(
        "./deltalake/rick_and_morty/bronze/characters"
    ).to_pandas()
    df_silver_character = Transformations(bronze_character_data).silverCharacter()
    DeltaTableManager(df_silver_character).writeToDeltaLake(SILVER_CHARACTERS_TABLE)

# LOCATION PIPELINE
def extractLocationData(**context):
    location_data = ApiRequest(BASE_URL/LOCATION_ENDPOINT).extractData(Location)
    df_bronze_location = Transformations(location_data).bronzeLocation()
    context['ti'].xcom_push(key='location_data', value=df_bronze_location)

def loadLocationData(**context):
    df_location_data = context['ti'].xcom_pull(key='location_data')
    DeltaTableManager(df_location_data).writeToDeltaLake(BRONZE_LOCATION_TABLE)

def silverTransformLocationData():
    bronze_location_data = DeltaTable("./deltalake/rick_and_morty/bronze/location").to_pandas()
    df_silver_location = Transformations(bronze_location_data).silverLocation()
    DeltaTableManager(df_silver_location).writeToDeltaLake(SILVER_LOCATION_TABLE)

# EPISODE PIPELINE
def extractEpisodeData(**context):
    episode_data = ApiRequest(BASE_URL/EPISODE_ENDPOINT).extractData(Episode)
    df_bronze_episode = Transformations(episode_data).bronzeEpisode()
    context['ti'].xcom_push(key='episode_data', value=df_bronze_episode)

def loadEpisodeData(**context):
    df_location_data = context['ti'].xcom_pull(key='eposode_data')
    DeltaTableManager(df_location_data).writeToDeltaLake(BRONZE_EPISODES_TABLE)

def silverTransformEpisodeData():
    bronze_episode_data = DeltaTable(BRONZE_EPISODES_TABLE).to_pandas()
    df_silver_episode = Transformations(bronze_episode_data).silverEpisode()
    DeltaTableManager(df_silver_episode).writeToDeltaLake(SILVER_EPISODES_TABLE)

