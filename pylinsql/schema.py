from dataclasses import dataclass


@dataclass
class ForeignKey:
    name: str
    primary_table: str
    primary_column: str
