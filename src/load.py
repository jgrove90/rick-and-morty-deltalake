import deltalake as dl
from pyarrow import Schema

BASE_PATH = "./deltalake/rick_and_morty"

BRONZE_CHARACTER_TABLE = f"{BASE_PATH}/bronze/character"
SILVER_CHARACTER_TABLE = f"{BASE_PATH}/silver/character"
GOLD_CHARACTER_TABLE = f"{BASE_PATH}/gold/character"

BRONZE_LOCATION_TABLE = f"{BASE_PATH}/bronze/location"
SILVER_LOCATION_TABLE = f"{BASE_PATH}/silver/location"
GOLD_LOCATION_TABLE = f"{BASE_PATH}/gold/location"

BRONZE_EPISODE_TABLE = f"{BASE_PATH}/bronze/episode"
SILVER_EPISODE_TABLE = f"{BASE_PATH}/silver/episode"
GOLD_EPISODE_TABLE = f"{BASE_PATH}/gold/episode"

FACT_TABLE = f"{BASE_PATH}/gold/fact"


class DeltaTableManager:
    """
    A utility class for managing Delta Lake tables.
    """

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def getSchema(self) -> Schema:
        """
        Get the schema of the managed DataFrame.

        Returns:
        Schema: The schema of the managed DataFrame.
        """
        return Schema.from_pandas(self.dataframe)

    def writeToDeltaLake(self, path: str):
        """
        Write the managed DataFrame to a Delta Lake table.

        Parameters:
        path (str): The path where the Delta Lake table will be written.
        """
        dl.write_deltalake(
            path, self.dataframe, schema=self.getSchema(), mode="overwrite"
        )
