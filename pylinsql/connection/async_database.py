"""
SQL database connection handling with the async/await syntax.
"""

from __future__ import annotations

import dataclasses
import os
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import asyncpg
from strong_typing.inspection import is_dataclass_type

T = TypeVar("T")


def cast_if_not_none(typ: Type[T], value: Optional[Any]) -> Optional[T]:
    "Coerces an optional value into the specified type unless the value is None."

    if value is None:
        return None
    else:
        return typ(value)


@dataclasses.dataclass(eq=True, frozen=True)
class ConnectionParameters:
    "Encapsulates database connection parameters."

    user: str = dataclasses.field(
        default_factory=lambda: os.getenv("PSQL_USERNAME", "postgres")
    )
    password: str = dataclasses.field(
        default_factory=lambda: os.getenv("PSQL_PASSWORD", "")
    )
    database: str = dataclasses.field(
        default_factory=lambda: os.getenv("PSQL_DATABASE", "postgres")
    )
    host: str = dataclasses.field(
        default_factory=lambda: os.getenv("PSQL_HOSTNAME", "localhost")
    )
    port: int = dataclasses.field(
        default_factory=lambda: int(os.getenv("PSQL_PORT", "5432"))
    )
    command_timeout: int = 60
    schema: str = dataclasses.field(
        default_factory=lambda: os.getenv("PSQL_SCHEMA", "public")
    )

    def as_kwargs(self) -> Dict[str, Union[str, int]]:
        "Connection string parameters as keyword arguments."

        d = dataclasses.asdict(self)
        del d["schema"]
        return d


class BasicConnection:
    "An extension of asyncpg connection class with auxiliary methods."

    conn: asyncpg.Connection

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def typed_fetch(self, typ: T, query: str, *args) -> List[T]:
        """Maps all columns of a database record to a Python data class."""

        if not is_dataclass_type(typ):
            raise TypeError(f"{typ} must be a dataclass type")

        records = await self.conn.fetch(query, *args)
        return self._typed_fetch(typ, records)

    async def typed_fetch_column(
        self, typ: Type[T], query: str, *args, column: int = 0
    ) -> List[T]:
        """Maps a single column of a database record to a Python class."""

        records = await self.conn.fetch(query, *args)
        return [cast_if_not_none(typ, record[column]) for record in records]

    async def typed_fetch_value(
        self, typ: Type[T], query: str, *args, column: int = 0
    ) -> T:
        value = await self.conn.fetchval(query, *args, column=column)
        return cast_if_not_none(typ, value)

    def _typed_fetch(self, typ: Type[T], records: List[asyncpg.Record]) -> List[T]:
        results = []
        for record in records:
            result = object.__new__(typ)

            if is_dataclass_type(typ):
                for field in dataclasses.fields(typ):
                    key = field.name
                    value = record.get(key, None)
                    if value is not None:
                        setattr(result, key, value)
                    elif field.default:
                        setattr(result, key, field.default)
                    else:
                        raise RuntimeError(
                            f"object field {key} without default value is missing a corresponding database record column"
                        )
            else:
                for key, value in record.items():
                    setattr(result, key, value)

            results.append(result)
        return results

    async def raw_fetch(
        self, query: str, *args, timeout=None, record_class=None
    ) -> List[asyncpg.Record]:

        return await self.conn.fetch(
            query, *args, timeout=timeout, record_class=record_class
        )

    async def raw_fetchval(
        self, query: str, *args, column: int = 0, timeout: Optional[float] = None
    ) -> Any:
        return await self.conn.fetchval(query, *args, column=column, timeout=timeout)

    async def raw_execute(
        self, query: str, *args, timeout: Optional[float] = None
    ) -> str:
        return await self.conn.execute(query, *args, timeout=timeout)

    async def raw_executemany(
        self, command: str, args: Iterable, timeout: Optional[float] = None
    ) -> None:
        return await self.conn.executemany(command, args, timeout=timeout)


async def _create_connection(params: ConnectionParameters) -> asyncpg.Connection:
    return await asyncpg.connect(**params.as_kwargs())


@asynccontextmanager
async def connection(
    params: ConnectionParameters = None,
) -> AsyncIterator[BasicConnection]:
    if params is None:
        params = ConnectionParameters()
    conn = await _create_connection(params)
    try:
        yield BasicConnection(conn)
    finally:
        await conn.close()
