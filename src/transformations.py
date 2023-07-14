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

    def silverCharacter(self):
        
        df = (
            self.data[
                [
                    "id",
                    "name",
                    "status",
                    "species",
                    "gender",
                    "origin",
                    "location",
                    "image",
                    "created",
                ]
            ]
            .assign(origin=self.data.origin.apply(lambda _df: getID(_df)))
            .assign(location=self.data.location.apply(lambda _df: getID(_df)))
            .assign(created=self.data.created.apply(lambda _df: dateTimeParse(_df)))
            .astype({"origin": pd.Int64Dtype(), "location": pd.Int64Dtype()})
            .rename(columns={"origin": "origin_id", "location": "location_id"})
        )
        return df
