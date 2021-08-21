from typing import Any, Optional, Type, TypeVar

T = TypeVar("T")


def optional_cast(typ: Type[T], value: Optional[Any]) -> Optional[T]:
    if value is None:
        return None
    else:
        return typ(value)