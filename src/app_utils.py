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

def getIDList(data: List) -> List:
    ids = []
    for url in data:
        parts = url.split("/")
        if parts[-1].isdigit():
            url = int(parts[-1])

def dateTimeParse(date: str):
    datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    datetime_obj = datetime.strptime(date, datetime_format)
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

def dateParse(date: str):
    datetime_format = "%B %d, %Y"
    datetime_obj = datetime.strptime(date, datetime_format)
    return datetime_obj.strftime("%Y-%m-%d")
