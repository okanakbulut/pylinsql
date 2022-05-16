import dataclasses
from typing import Optional

from pylinsql.generator.schema import ForeignKey, PrimaryKey
from strong_typing.inspection import is_type_enum


def is_composite_type(cls: type) -> bool:
    "True if the Python class is to be represented as a PostgreSQL composite type."

    return not is_type_enum(cls) and not hasattr(cls, "primary_key")


def is_table_type(cls: type) -> bool:
    "True if the Python class is to be represented as a PostgreSQL table type."

    return hasattr(cls, "primary_key")


def get_primary_key(cls: type) -> Optional[PrimaryKey]:
    primary_key = getattr(cls, "primary_key", None)
    return primary_key  # perform implicit type cast


def get_foreign_key(field: dataclasses.Field) -> Optional[ForeignKey]:
    foreign_key = field.metadata.get("foreign_key")
    if isinstance(foreign_key, ForeignKey):
        return foreign_key  # perform implicit type cast
    else:
        return None
