from dataclasses import dataclass
from datetime import datetime


@dataclass
class Person:
    id: int
    birth_date: datetime
    birth_year: int
    family_name: str
    given_name: str
    perm_address_id: int
    temp_address_id: int


@dataclass
class Address:
    id: int
    city: str