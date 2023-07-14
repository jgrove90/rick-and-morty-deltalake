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
        return re.search(r'\d+$', url).group()
    else: 
        return None
    
def dateTimeParse(date: str):
    datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    datetime_obj = datetime.strptime(date, datetime_format)

    
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

# TODO add logging
# TODO add try/except
def getPages(ENDPOINT: str) -> int:
    """
    Retrieves the total number of pages available for data from the specified API endpoint.

    Args:
        ENDPOINT (str): The API endpoint for data.

    Returns:
        int: The total number of pages available.
    """
    total_pages = (
        ApiRequest(f"{BASE_URL}/{ENDPOINT}/?page=1").extractCharacterData().info.pages
    )
    return total_pages

# TODO add logging
# TODO add try/except
def getCharacterData() -> List[dict]:
    """
    Retrieves character data from multiple pages and returns a list of dictionaries.

    Returns:
        List[dict]: A list containing character data as dictionaries.
    """
    data = []
    
    #getPages(CHARACTER_ENDPOINT)+1
    for n_page in range(1, 3):
        results = (
            ApiRequest(f"{BASE_URL}/{CHARACTER_ENDPOINT}/?page={n_page}")
            .extractCharacterData()
            .results
        )   


        for result in results:
            result_dict = asdict(result)
            data.append(result_dict)

    return data