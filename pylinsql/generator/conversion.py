import datetime
import decimal
import re
import uuid
from typing import Any, List, Optional, Type, TypeVar

from strong_typing.auxiliary import (
    Annotated,
    MaxLength,
    Precision,
    Storage,
    float32,
    float64,
    int16,
    int32,
    int64,
)
from strong_typing.inspection import is_generic_list, unwrap_generic_list

T = TypeVar("T")


def cast_if_not_none(typ: Type[T], value: Optional[Any]) -> Optional[T]:
    "Coerces an optional value into the specified type unless the value is None."

    if value is None:
        return None
    else:
        return typ(value)


def sql_quoted_id(name: str) -> str:
    id = name.replace('"', '""')
    return f'"{id}"'


_sql_quoted_str_table = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
    }
)


def sql_quoted_str(text: str) -> str:
    if re.search(r"[\b\f\n\r\t]", text):
        string = text.translate(_sql_quoted_str_table)
        return f"E'{string}'"
    else:
        string = text.replace("'", "''")
        return f"'{string}'"


def sql_to_python_type(sql_type: str) -> type:
    "Maps a PostgreSQL type to a native Python type."

    if sql_type.endswith("[]"):
        return List[sql_to_python_type(sql_type[:-2])]

    sql_type = sql_type.lower()

    if sql_type == "boolean":
        return bool
    if sql_type == "smallint":
        return int16
    if sql_type in ["int", "integer"]:
        return int32
    if sql_type == "bigint":
        return int64
    if sql_type == "real":
        return Annotated[float, Storage(4)]
    if sql_type in ["double", "double precision"]:
        return Annotated[float, Storage(8)]
    if sql_type in ["character varying", "text"]:
        return str
    if sql_type in ["decimal", "numeric"]:
        return decimal.Decimal
    if sql_type == "date":
        return datetime.date
    if sql_type in ["time", "time with time zone", "time without time zone"]:
        return datetime.time
    if sql_type == "interval":
        return datetime.timedelta
    if sql_type in [
        "timestamp",
        "timestamp with time zone",
        "timestamp without time zone",
    ]:
        return datetime.datetime
    if sql_type in ["json", "jsonb"]:
        return str
    if sql_type == "uuid":
        return uuid.UUID

    m = re.match(r"^character varying[(](\d+)[)]$", sql_type)
    if m is not None:
        len = int(m.group(1))
        return Annotated[str, MaxLength(len)]

    m = re.match(r"^(?:decimal|numeric)[(](\d+)(?:,\s*(\d+))?[)]$", sql_type)
    if m is not None:
        precision = int(m.group(1))
        scale = int(m.group(2)) if m.group(2) else 0
        return Annotated[decimal.Decimal, Precision(precision, scale)]

    m = re.match(r"^time[(](\d+)[)](?: with(?:out)? time zone)?$", sql_type)
    if m is not None:
        precision = int(m.group(1))
        return Annotated[datetime.time, Precision(precision)]

    m = re.match(r"^timestamp[(](\d+)[)](?: with(?:out)? time zone)?$", sql_type)
    if m is not None:
        precision = int(m.group(1))
        return Annotated[datetime.datetime, Precision(precision)]

    raise NotImplementedError(f"unrecognized database type: {sql_type}")


def python_to_sql_type(typ: type) -> str:
    "Maps a native Python type to a PostgreSQL type."

    if typ is bool:
        return "boolean"
    if typ is int16:
        return "smallint"
    if typ is int32 or typ is int:
        return "int"
    if typ is int64:
        return "bigint"
    if typ is float32:
        return "real"
    if typ is float64 or typ is float:
        return "double precision"
    if typ is str:
        return "text"
    if typ is decimal.Decimal:
        return "decimal"
    if typ is datetime.datetime:
        return "timestamp without time zone"
    if typ is datetime.date:
        return "date"
    if typ is datetime.time:
        return "time without time zone"
    if typ is datetime.timedelta:
        return "interval"
    if typ is uuid.UUID:
        return "uuid"

    if is_generic_list(typ):
        inner_type = python_to_sql_type(unwrap_generic_list(typ))
        return f"{inner_type}[]"

    raise NotImplementedError(f"unmappable Python type: {repr(typ)}")
