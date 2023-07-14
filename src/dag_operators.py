from app_utils import *
from transformations import *
from table_manager import *
from deltalake import DeltaTable
from pandas import DataFrame


def extractCharacterData(**context) -> DataFrame:
    character_api_data = getCharacterData()
    df_bronze_character = Transformations(character_api_data).createDataFrame()
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
    


# def main():
#     bronzeCharacterData()
#     silverCharacterData()


# if __name__ == "__main__":



