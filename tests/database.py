from dataclasses import dataclass
from datetime import datetime


@dataclass
class Person:
    id: int
    family_name: str
    given_name: str
    birth_date: datetime
    perm_address_id: int
    temp_address_id: int


@dataclass
class Address:
    id: int
    city: str


@dataclass
class PersonCity:
    family_name: str
    given_name: str
    city: str
