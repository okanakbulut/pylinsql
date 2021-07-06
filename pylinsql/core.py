"""
Construct a SQL query from a Python expression.
"""

import dataclasses
import datetime
from typing import List, Tuple, Type, TypeVar, Union, overload

T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")


class EntityProxy:
    def __init__(self, types: List[Type]):
        self.types = types

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


@overload
def entity(cls1: Type[T1]) -> List[T1]:
    ...


@overload
def entity(cls1: Type[T1], cls2: Type[T2]) -> List[Tuple[T1, T2]]:
    ...


@overload
def entity(cls1: Type[T1], cls2: Type[T2], cls3: Type[T3]) -> List[Tuple[T1, T2, T3]]:
    ...


@overload
def entity(
    cls1: Type[T1], cls2: Type[T2], cls3: Type[T3], cls4: Type[T4]
) -> List[Tuple[T1, T2, T3, T4]]:
    ...


@overload
def entity(
    cls1: Type[T1], cls2: Type[T2], cls3: Type[T3], cls4: Type[T4], cls5: Type[T5]
) -> List[Tuple[T1, T2, T3, T4, T5]]:
    ...


@overload
def entity(
    cls1: Type[T1],
    cls2: Type[T2],
    cls3: Type[T3],
    cls4: Type[T4],
    cls5: Type[T5],
    cls6: Type[T6],
) -> List[Tuple[T1, T2, T3, T4, T5, T6]]:
    ...


def is_dataclass_type(typ):
    "True if the argument corresponds to a data class type (but not an instance)."

    return isinstance(typ, type) and dataclasses.is_dataclass(typ)


def is_dataclass_instance(obj):
    "True if the argument corresponds to a data class instance (but not a type)."

    return not isinstance(obj, type) and dataclasses.is_dataclass(obj)


def entity(*cls) -> List:
    "Represents the list of entities (a.k.a. tables in SQL) to query from."

    if not all(is_dataclass_type(typ) for typ in cls):
        raise TypeError("all entities must be of a dataclass type")

    return EntityProxy(cls)


def full_join(left: T, right: T) -> bool:
    "Perform a full outer join between two relations."
    ...


def inner_join(left: T, right: T) -> bool:
    "Perform an inner join between two relations."
    ...


def left_join(left: T, right: T) -> bool:
    "Perform a left outer join between two relations."
    ...


def right_join(left: T, right: T) -> bool:
    "Perform a right outer join between two relations."
    ...


def asc(_: T) -> T:
    "Order items in ascending order."
    ...


def desc(_: T) -> T:
    "Order items in descending order."
    ...


def avg(_: T) -> T:
    "Aggregation function: Mean of items in set."
    ...


def count(_: T) -> T:
    "Aggregation function: Number of items in set."
    ...


def max(_: T) -> T:
    "Aggregation function: Greatest item in set."
    ...


def min(_: T) -> T:
    "Aggregation function: Smallest item in set."
    ...


def sum(_: T) -> T:
    "Aggregation function: Sum of items in set."
    ...


def now() -> datetime.datetime:
    ...


def year(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


def month(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


def day(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


def hour(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


def minute(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


def second(dt: Union[datetime.datetime, datetime.timedelta]) -> int:
    ...


class _QueryParameter:
    "A placeholder in a parameterized query."

    def __init__(self, index):
        self.index = index

    @property
    def name(self):
        "Expression representation (i.e. as used in a Python generator expression)."

        return f"p_{self.index}"

    def __str__(self):
        "PostgreSQL representation (i.e. as used in a SQL query)."

        return f"${self.index}"


p_1 = _QueryParameter(1)
p_2 = _QueryParameter(2)
p_3 = _QueryParameter(3)
p_4 = _QueryParameter(4)
p_5 = _QueryParameter(5)
p_6 = _QueryParameter(6)
p_7 = _QueryParameter(6)
p_8 = _QueryParameter(6)
p_9 = _QueryParameter(6)


class _DefaultValue:
    "Specifies that an argument should assume its default value."

    pass


DEFAULT = _DefaultValue


class Query:
    "A query constructed from a Python generator expression."

    def __init__(self, sql):
        self.sql = sql

    def __str__(self) -> str:
        return self.sql
