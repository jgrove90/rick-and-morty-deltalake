from typing import List
from app_utils import *
import pandas as pd
from deltalake import DeltaTable
from load import *


# TODO add logging
# TODO add doc string
class Transformation:
    def __init__(self, data: List[dict] | pd.DataFrame):
        self.data = data

    def createDataFrame(self) -> DataFrame:
        df = createDataFrame(self.data)
        return df

    class Character:
        def bronze(self):
            df = self.createDataFrame().drop(columns=["type", "url"])
            return df

        def silver(self):
            df = (
                self.data.drop(columns=["episode"])
                .assign(origin=self.data.origin.apply(lambda _df: getID(_df)))
                .assign(location=self.data.location.apply(lambda _df: getID(_df)))
                .assign(
                    created=self.data.created.apply(lambda _df: dateTimeFormat(_df))
                )
                .astype({"origin": pd.Int64Dtype(), "location": pd.Int64Dtype()})
                .rename(columns={"origin": "origin_id", "location": "location_id"})
            )
            return df

        def gold(self):
            df = self.data.drop(columns=["origin_id", "location_id", "created"]).rename(
                columns={"id": "char_id"}
            )
            return df

    class Location:
        def bronze(self):
            df = self.createDataFrame().drop(columns=["residents", "url"])
            return df

        def silver(self):
            df = self.data.assign(
                created=self.data.created.apply(lambda _df: dateTimeFormat(_df))
            )
            return df

        def gold(self):
            df = self.data.drop(columns=["created"]).rename(columns={"id": "loc_id"})
            return df

    class Episode:
        def bronze(self):
            df = self.createDataFrame().drop(columns="url")
            return df

        def silver(self):
            df = self.data.assign(
                air_date=self.data.air_date.apply(lambda _df: dateTimeFormat(_df)),
                created=self.data.created.apply(lambda _df: dateTimeFormat(_df)),
            )
            return df

        def gold(self):
            df = self.data.drop(columns=["characters", "created"]).rename(
                columns={"id": "ep_id"}
            )
            return df

    def factTable(self):
        ep_data = DeltaTable(SILVER_EPISODE_TABLE).to_pandas()
        char_data = DeltaTable(SILVER_CHARACTER_TABLE).to_pandas()

        fact_table = (
            ep_data.assign(characters=ep_data.characters.apply(lambda _df: getID(_df)))
            .explode("characters")
            .rename(columns={"characters": "char_id", "id": "ep_id"})
            .join(char_data.set_index("id"), on="char_id", lsuffix="_")
            .drop(
                columns=[
                    "name",
                    "air_date",
                    "episode",
                    "created",
                    "name_",
                    "status",
                    "species",
                    "gender",
                    "image",
                    "created_",
                ]
            )
        )
        return fact_table
