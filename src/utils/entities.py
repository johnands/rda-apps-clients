from dataclasses import dataclass
from enum import Enum, auto
from typing import *


@dataclass
class RequestConfig:
    parameters: str
    levels: str
    products: str

@dataclass
class Area:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

class Areas:
    GLOBAL = Area(-90, 90, -180, 180)
    EUROPE = Area(32, 74, -28, 46)

@dataclass
class Task:
    name: str
    argument: int | Dict[str, str]

class ResponseTypes(Enum):
    SUCCESS = auto()
    ERROR = auto()
    EXCEPTION = auto()

@dataclass
class Response:
    type: ResponseTypes
    response: Dict[str, Any] | None