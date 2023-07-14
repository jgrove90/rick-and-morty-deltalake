import requests
import data_classes as dc
from dataclasses import asdict
from typing import List

BASE_URL = "https://rickandmortyapi.com/api"

CHARACTER_ENDPOINT = "character"

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
    def response(self) -> dict:
        """
        Sends an HTTP GET request to the endpoint and returns the JSON response.

        Returns:
            dict: The JSON response as a dictionary.
        """
        session = requests.Session()
        return session.get(self.endpoint).json()

    # TODO add logging
    # TODO add try/except
    def extractCharacterData(self) -> dc.Response:
        """
        Extracts character data from the JSON response.

        Returns:
            dc.Response: A dataclass Response object containing character data,
                         or None if no character data is available.
        """
        response = self.response()
        character_data = response.get("results")

        # pass dataclass
        if character_data:
            characters = [dc.Characters(**item) for item in character_data]
            info = dc.Info(**response.get("info"))
            return dc.Response(info=info, results=characters)
        else:
            return None