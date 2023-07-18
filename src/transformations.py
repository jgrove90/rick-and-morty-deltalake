from typing import List
from app_utils import *
import pandas as pd


# TODO add logging
# TODO add doc string
# TODO add try/except
class Transformations:
    def __init__(self, data: List[dict]):
        self.data = data

    def createDataFrame(self) -> DataFrame:
        df = createDataFrame(self.data)
        return df

    def bronzeCharacter(self):
        df = self.createDataFrame().drop(columns=["type", "url"])
        return df

    def silverCharacter(self):
        df = (
            self.data.drop(columns=["episode"])
            .assign(origin=self.data.origin.apply(lambda _df: getID(_df)))
            .assign(location=self.data.location.apply(lambda _df: getID(_df)))
            .assign(created=self.data.created.apply(lambda _df: dateTimeParse(_df)))
            .astype({"origin": pd.Int64Dtype(), "location": pd.Int64Dtype()})
            .rename(columns={"origin": "origin_id", "location": "location_id"})
        )
        return df

    def bronzeLocation(self):
        df = self.createDataFrame().drop(columns=["residents", "url"])
        return df

    def silverLocation(self):
        df = self.data.assign(
            created=self.data.created.apply(lambda _df: dateTimeParse(_df))
        )
        return df

    def bronzeEpisode(self):
        df = self.createDataFrame().drop(columns="url")
        return df

    def silverEpisode(self):
        df = self.data.assign(
            air_date=self.data.air_date.apply(lambda _df: dateParse(_df)),
            created=self.data.created.apply(lambda _df: dateTimeParse(_df))
        )
        return df