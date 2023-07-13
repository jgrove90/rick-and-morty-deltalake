import requests
import data_classes as dc
from dataclasses import asdict
from typing import List

BASE_URL = "https://rickandmortyapi.com/api"

CHARACTER_ENDPOINT = "character"

# TODO add logging
# TODO add doc string
# TODO add try/except
class ApiRequest:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def response(self):
        session = requests.Session()
        return session.get(self.endpoint).json()

    def extractCharacterData(self):
        response = self.response()
        character_data = response.get("results")

        # pass dataclass
        if character_data:
            characters = [dc.Characters(**item) for item in character_data]
            info = dc.Info(**response.get("info"))
            return dc.Response(info=info, results=characters)
        else:
            return None

# TODO add logging
# TODO add doc string
# TODO add try/except
def getPages(ENDPOINT: str) -> int:
    total_pages = (
        ApiRequest(f"{BASE_URL}/{ENDPOINT}/?page=1").extractCharacterData().info.pages
    )
    return total_pages

# TODO add logging
# TODO add doc string
# TODO add try/except
def getCharacterData() -> List[dict]:

    data = []

    for n_page in range(1, getPages(CHARACTER_ENDPOINT) + 1):
        results = (
            ApiRequest(f"{BASE_URL}/{CHARACTER_ENDPOINT}/?page={n_page}")
            .extractCharacterData()
            .results
        )

        data.append(asdict(results[0]))

    return data