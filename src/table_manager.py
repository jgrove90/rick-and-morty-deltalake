import deltalake as dl
from pyarrow import Schema


BRONZE_CHARACTERS_TABLE = "./deltalake/rick_and_morty/bronze/characters"
SILVER_CHARACTERS_TABLE = "./deltalake/rick_and_morty/silver/characters"

BRONZE_LOCATION_TABLE = "./deltalake/rick_and_morty/bronze/location"
SILVER_LOCATION_TABLE = "./deltalake/rick_and_morty/silver/location"

BRONZE_EPISODES_TABLE = "./deltalake/rick_and_morty/bronze/episodes"
SILVER_EPISODES_TABLE = "./deltalake/rick_and_morty/silver/episodes"


class DeltaTableManager:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def getSchema(self) -> Schema:
        return Schema.from_pandas(self.dataframe)

    def writeToDeltaLake(self, path: str) -> None:
        dl.write_deltalake(
            path, self.dataframe, schema=self.getSchema(), mode="overwrite"
        )