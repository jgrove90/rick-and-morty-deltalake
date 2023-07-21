import requests
import data_classes as dc
from dataclasses import asdict, dataclass
from typing import List

BASE_URL = "https://rickandmortyapi.com/api"

CHARACTER_ENDPOINT = "character"
LOCATION_ENDPOINT = "location"
EPISODE_ENDPOINT = "episode"


# TODO add logging
# TODO add try/except
class ApiRequest:
    """
    A class for making API requests and extracting character data.

    Args:
        endpoint (str): The API endpoint URL.

    Attributes:
        endpoint (str): The API endpoint URL.

    Methods:
        response(): Sends an HTTP GET request to the endpoint and returns the JSON response.
        extractCharacterData(): Extracts character data from the JSON response.
    """

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    # TODO add logging
    # TODO add try/except
    def extractData(self, dataclass_name: dataclass) -> List[dict]:
        """
        Extracts location data from the JSON response.

        Returns:
            dc.Response: A dataclass Response object containing location data,
                         or None if no location data is available.
        """
        data = []

        # start of pagination (updates with every loop)
        next_url = self.endpoint

        while next_url:
            # makes request
            response = requests.Session().get(next_url).json()
            results = response.get("results")

            if results:
                # passes JSON to dataclasess
                dataclass_results = [dataclass_name(**item) for item in results]
                dataclass_info = dc.Info(**response.get("info"))
                dataclass_response = dc.Response(
                    info=dataclass_info, results=dataclass_results 
                )

                # sets the next_url from current request
                next_url = dataclass_response.info.next

                # appends data to list
                for result in dataclass_response.results:
                    result_dict = asdict(result)
                    data.append(result_dict)
            else:
                None

        return data or None
