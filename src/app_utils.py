import re
from typing import List
from pandas import DataFrame, to_datetime
from datetime import datetime
from api_requests import *


def createDataFrame(data: List[dict]) -> DataFrame:
    return DataFrame(data)


def getID(data: dict) -> int:
    url = data.get("url")
    if url:
        return re.search(r"\d+$", url).group()
    else:
        return None


def dateTimeFormat(date: str):
    datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    datetime_obj = datetime.strptime(date, datetime_format)
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")


def dateFormat(date: str):
    datetime_format = "%B %d, %Y"
    datetime_obj = datetime.strptime(date, datetime_format)
    return datetime_obj.strftime("%Y-%m-%d")
