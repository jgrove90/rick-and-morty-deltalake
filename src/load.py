import deltalake as dl
from pyarrow import Schema


BRONZE_CHARACTERS_TABLE = "./deltalake/rick_and_morty/bronze/character"
SILVER_CHARACTERS_TABLE = "./deltalake/rick_and_morty/silver/character"
GOLD_CHARACTERS_TABLE = "./deltalake/rick_and_morty/gold/character"

BRONZE_LOCATION_TABLE = "./deltalake/rick_and_morty/bronze/location"
SILVER_LOCATION_TABLE = "./deltalake/rick_and_morty/silver/location"
GOLD_LOCATION_TABLE = "./deltalake/rick_and_morty/gold/location"

BRONZE_EPISODE_TABLE = "./deltalake/rick_and_morty/bronze/episode"
SILVER_EPISODE_TABLE = "./deltalake/rick_and_morty/silver/episode"
GOLD_EPISODE_TABLE = "./deltalake/rick_and_morty/gold/episode"

class DeltaTableManager:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def getSchema(self) -> Schema:
        return Schema.from_pandas(self.dataframe)

    def writeToDeltaLake(self, path: str) -> None:
        dl.write_deltalake(
            path, self.dataframe, schema=self.getSchema(), mode="overwrite"
        )