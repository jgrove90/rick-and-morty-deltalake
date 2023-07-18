from dataclasses import dataclass
from typing import List

@dataclass
class Characters:
    id: int
    name: str
    status: str
    species: str
    type: str
    gender: str
    origin: dict
    location: dict
    image: str
    episode: List[str]
    url: str
    created: str

@dataclass
class Episode:
    id: int
    name: str
    air_date: str
    episode: str
    characters: List[str]
    url: str
    created: str

@dataclass
class Location:
    id: int
    name: str
    type: str
    dimension: str
    residents: List[str]
    url: str
    created: str

@dataclass
class Info:
    count: int
    pages: int
    next: str
    prev: str

@dataclass
class Response:
    info: Info
    results: List[Characters]