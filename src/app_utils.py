import re
from typing import List
import pandas as pd
from pandas import DataFrame, to_datetime
from datetime import datetime
from extract import *
import numpy as np

REGEX_URL_ID = r"\d+$"


def createDataFrame(data: List[dict]) -> DataFrame:
    return DataFrame(data)


##TODO FIX
def getID(data: dict | np.ndarray) -> int:
    if isinstance(data, dict):
        url = data.get("url")
        if url:
            return int(re.search(REGEX_URL_ID, data).group())
        else:
            return None
    elif isinstance(data, np.ndarray):
        char_id = []
        data = data.tolist()
        for url in data:
            char = re.search(REGEX_URL_ID, url).group()
            char_id.append(int(char))
        return char_id
    else:
        return None


def dateTimeFormat(date: str) -> str:
    date_time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_format = "%B %d, %Y"

    try:
        datetime_obj = datetime.strptime(date, date_time_format)
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            datetime_obj = datetime.strptime(date, date_format)
            return datetime_obj.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format")
