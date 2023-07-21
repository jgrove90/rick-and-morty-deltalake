from typing import List
from app_utils import *
import pandas as pd
from deltalake import DeltaTable
from load import DeltaTableManager


# TODO add logging
# TODO add doc string
class Transformations:
    def __init__(self, data: List[dict]):
        self.data = data

    def createDataFrame(self) -> DataFrame:
        df = createDataFrame(self.data)
        return df

    class Character:
        pass

    def bronzeCharacter(self):
        df = self.createDataFrame().drop(columns=["type", "url"])
        return df

    def silverCharacter(self):
        df = (
            self.data.drop(columns=["episode"])
            .assign(origin=self.data.origin.apply(lambda _df: getID(_df)))
            .assign(location=self.data.location.apply(lambda _df: getID(_df)))
            .assign(created=self.data.created.apply(lambda _df: dateTimeFormat(_df)))
            .astype({"origin": pd.Int64Dtype(), "location": pd.Int64Dtype()})
            .rename(columns={"origin": "origin_id", "location": "location_id"})
        )
        return df

    def goldCharacter(self):
        df = self.data.drop(columns=["origin_id", "location_id", "created"]).rename(
            columns={"id": "char_id"}
        )
        return df

    class Location:
        pass

    def bronzeLocation(self):
        df = self.createDataFrame().drop(columns=["residents", "url"])
        return df

    def silverLocation(self):
        df = self.data.assign(
            created=self.data.created.apply(lambda _df: dateTimeFormat(_df))
        )
        return df

    def goldLocation(self):
        df = self.data.drop(columns=["created"]).rename(columns={"id": "loc_id"})
        return df

    class Episode:
        pass

    def bronzeEpisode(self):
        df = self.createDataFrame().drop(columns="url")
        return df

    def silverEpisode(self):
        df = self.data.assign(
            air_date=self.data.air_date.apply(lambda _df: dateFormat(_df)),
            created=self.data.created.apply(lambda _df: dateTimeFormat(_df)),
        )
        return df

    def goldEpisode(self):
        df = self.data.drop(columns=["characters", "created"]).rename(
            columns={"id": "ep_id"}
        )
        return df

    def factlessFactTable(self):
        ep_data = DeltaTable("./deltalake/rick_and_morty/silver/episode").to_pandas()
        char_data = DeltaTable(
            "./deltalake/rick_and_morty/silver/characters"
        ).to_pandas()

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

