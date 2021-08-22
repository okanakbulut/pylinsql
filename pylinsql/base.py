import dataclasses
from typing import Any, Dict, Optional, Protocol, Type, TypeVar

T = TypeVar("T")


def is_lambda(v) -> bool:
    "True if (and only if) argument holds a lambda function."

    LAMBDA = lambda: None
    return isinstance(v, type(LAMBDA)) and v.__name__ == LAMBDA.__name__


def optional_cast(typ: Type[T], value: Optional[Any]) -> Optional[T]:
    "Coerces an optional value into the specified type unless the value is None."

    if value is None:
        return None
    else:
        return typ(value)


class DataClass(Protocol):
    "Identifies a type as a dataclass type."

    __dataclass_fields__: Dict


def is_dataclass_type(typ) -> bool:
    "True if the argument corresponds to a data class type (but not an instance)."

    return isinstance(typ, type) and dataclasses.is_dataclass(typ)


def is_dataclass_instance(obj) -> bool:
    "True if the argument corresponds to a data class instance (but not a type)."

    return not isinstance(obj, type) and dataclasses.is_dataclass(obj)
