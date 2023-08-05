import re
from typing import List
import pandas as pd
from pandas import DataFrame, to_datetime
from datetime import datetime
from extract import *
import numpy as np
import os
import deltalake as dt

REGEX_URL_ID = r"\d+$"


def createDataFrame(data: List[dict]) -> DataFrame:
    """
    Create a DataFrame from a list of dictionaries.

    Parameters:
    data (List[dict]): List of dictionaries to create the DataFrame from.

    Returns:
    DataFrame: The created DataFrame.
    """
    return DataFrame(data)


##TODO FIX
def getID(data: dict | np.ndarray) -> int:
    """
    Extract and return the numeric ID from a dictionary or an array of URLs.

    Parameters:
    data (dict | np.ndarray): Input dictionary or array of URLs.

    Returns:
    int | List[int] | None: Extracted ID or list of IDs if input is an array of URLs, None if not found.
    """
    if isinstance(data, dict):
        url = data.get("url")
        if url:
            return int(re.search(REGEX_URL_ID, url).group())
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
    """
    Convert a date string into a standardized date format.

    Parameters:
    date (str): Input date string in various formats.

    Returns:
    str: Date string in the standardized format.
    """
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


def delta_tables_to_csv(delta_path: str):
    """
    Convert Delta tables located in subfolders to CSV format.

    Parameters:
    delta_path (str): Path to the directory containing subfolders with Delta tables.
    """
    csv_path = "./csv_data"

    if not os.path.exists(csv_path):
        os.mkdir(csv_path)

    subfolders = [
        folder
        for folder in os.listdir(delta_path)
        if os.path.isdir(os.path.join(delta_path, folder))
    ]

    for folder in subfolders:
        delta_data = dt.DeltaTable(f"{delta_path}/{folder}").to_pandas()
        delta_data.to_csv(f"{csv_path}/{folder}.csv", index=False)
