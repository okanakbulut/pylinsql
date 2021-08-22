import dataclasses
import datetime
import io
import keyword
import typing
from dataclasses import Field, dataclass
from typing import Any, Dict, List, Optional, TextIO, Tuple, Type, TypeVar, Union

from .async_database import DatabaseClient
from .base import optional_cast
from .core import Dataclass
from .schema import ForeignKey

T = TypeVar("T")


def _db_type_to_py_type(db_type: str, nullable: bool) -> type:
    "Maps a PostgreSQL type to a Python type."

    if db_type == "character varying" or db_type == "text":
        py_type = str
    elif db_type == "boolean":
        py_type = bool
    elif db_type == "smallint" or db_type == "integer" or db_type == "bigint":
        py_type = int
    elif db_type == "real" or db_type == "double precision":
        py_type = float
    elif db_type == "date":
        py_type = datetime.date
    elif db_type == "time without time zone":
        py_type = datetime.time
    elif db_type == "timestamp without time zone":
        py_type = datetime.datetime
    else:
        raise RuntimeError(f"unrecognized database type: {db_type}")

    if nullable:
        return Optional[py_type]
    else:
        return py_type


@dataclass
class ColumnSchema:
    "Metadata associated with a database table column."

    name: str
    data_type: type
    default: Optional[Any]
    description: str
    references: Optional[ForeignKey] = None


@dataclass
class TableSchema:
    "Metadata associated with a database table."

    name: str
    description: str
    columns: Dict[str, ColumnSchema]


@dataclass
class CatalogSchema:
    "Metadata associated with a database (a.k.a. catalog)."

    name: str
    tables: Dict[str, TableSchema]


async def get_table_schema(
    conn: DatabaseClient, db_schema: str, db_table: str
) -> TableSchema:
    "Retrieves metadata for a table in the current catalog."

    query = """
        SELECT
            dsc.description
        FROM
            pg_catalog.pg_class cls
                INNER JOIN pg_catalog.pg_namespace ns ON cls.relnamespace = ns.oid
                INNER JOIN pg_catalog.pg_description dsc ON cls.oid = dsc.objoid
        WHERE
            ns.nspname = $1 AND cls.relname = $2 AND dsc.objsubid = 0
    """
    description = await conn.typed_fetch_value(str, query, db_schema, db_table)

    query = """
        WITH
            column_description AS (
                SELECT
                    dsc.objsubid,
                    dsc.description
                FROM
                    pg_catalog.pg_class cls
                        INNER JOIN pg_catalog.pg_namespace ns ON cls.relnamespace = ns.oid
                        INNER JOIN pg_catalog.pg_description dsc ON cls.oid = dsc.objoid
                WHERE
                    ns.nspname = $1 AND cls.relname = $2
            )
        SELECT
            column_name AS name,
            CASE WHEN is_nullable = 'YES' THEN 1 WHEN is_nullable = 'NO' THEN 0 ELSE NULL END AS is_nullable,
            data_type,
			column_default,
			character_maximum_length,
			is_identity,
            description
        FROM
            information_schema.columns
                LEFT JOIN column_description ON objsubid = ordinal_position
        WHERE
            table_catalog = CURRENT_CATALOG AND table_schema = $1 AND table_name = $2
        ORDER BY
            ordinal_position
    """
    columns = await conn.raw_fetch(query, db_schema, db_table)
    column_schemas = {}
    for column in columns:
        typ = _db_type_to_py_type(column["data_type"], column["is_nullable"])
        column_schema = ColumnSchema(
            name=column["name"],
            data_type=typ,
            default=optional_cast(typ, column["column_default"]),
            description=column["description"],
        )
        column_schemas[column_schema.name] = column_schema

    table_schema = TableSchema(
        name=db_table, description=description, columns=column_schemas
    )
    await set_foreign_keys(conn, db_schema, table_schema)
    return table_schema


async def get_catalog_schema(conn: DatabaseClient, db_schema: str) -> CatalogSchema:
    "Retrieves metadata for the current catalog."

    query = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_catalog = CURRENT_CATALOG AND table_schema = $1
    """
    tables = await conn.typed_fetch_column(str, query, db_schema)
    table_schemas = [await get_table_schema(conn, db_schema, table) for table in tables]
    table_schema_map = dict((table.name, table) for table in table_schemas)
    return CatalogSchema(name=db_schema, tables=table_schema_map)


@dataclass
class Constraint:
    contraint_name: str
    child_column: str
    parent_table: str
    parent_column: str


async def set_foreign_keys(
    conn: DatabaseClient, db_schema: str, table_schema: TableSchema
) -> None:
    "Binds table relations associating foreign keys with primary keys."

    query = """
        SELECT 
            conname AS contraint_name,
            att2.attname AS child_column, 
            cls.relname AS parent_table,
            att1.attname AS parent_column
        FROM
            (SELECT 
                UNNEST(con.conkey) AS parent, 
                UNNEST(con.confkey) AS child, 
                con.confrelid, 
                con.conrelid,
                con.conname
            FROM 
                pg_class cls
                    INNER JOIN pg_namespace AS ns ON cls.relnamespace = ns.oid
                    INNER JOIN pg_constraint AS con ON con.conrelid = cls.oid
            WHERE
                cls.relname = $2 AND ns.nspname = $1 AND con.contype = 'f'
            ) AS reference_constraint
                INNER JOIN pg_attribute AS att1 ON
                    att1.attrelid = reference_constraint.confrelid AND att1.attnum = reference_constraint.child
                INNER JOIN pg_class AS cls ON
                    cls.oid = reference_constraint.confrelid
                INNER JOIN pg_attribute AS att2 ON
                    att2.attrelid = reference_constraint.conrelid AND att2.attnum = reference_constraint.parent
    """
    constraints = await conn.typed_fetch(
        Constraint, query, db_schema, table_schema.name
    )
    for constraint in constraints:
        column = table_schema.columns[constraint.child_column]
        if column.references is not None:
            raise RuntimeError(
                f"column {column.name} already has a foreign key constraint"
            )

        column.references = ForeignKey(
            name=constraint.contraint_name,
            primary_table=constraint.parent_table,
            primary_column=constraint.parent_column,
        )


def column_to_field(
    column: ColumnSchema,
) -> Union[Tuple[str, Type], Tuple[str, Type, Field]]:
    if keyword.iskeyword(column.name):
        field_name = f"{column.name}_"  # PEP 8: single trailing underscore to avoid conflicts with Python keyword
    else:
        field_name = column.name

    if column.references is None:
        return (field_name, column.data_type)
    else:
        return (
            field_name,
            column.data_type,
            dataclasses.field(metadata={"references": column.references}),
        )


def table_to_dataclass(table: TableSchema) -> Dataclass:
    "Generates a dataclass type corresponding to a table schema."

    fields = [column_to_field(column) for column in table.columns.values()]
    if keyword.iskeyword(table.name):
        class_name = f"{table.name}_"  # PEP 8: single trailing underscore to avoid conflicts with Python keyword
    else:
        class_name = table.name

    typ = dataclasses.make_dataclass(class_name, fields)
    typ.__doc__ = table.description
    return typ


def catalog_to_dataclasses(catalog: CatalogSchema) -> List[Dataclass]:
    "Generates a list of dataclass types corresponding to a catalog schema."

    return [table_to_dataclass(table) for table in catalog.tables.values()]


def is_type_optional(typ: Type) -> bool:
    "True if the type annotation corresponds to an optional type (e.g. Optional[T] or Union[T1,T2,None])."

    if typing.get_origin(typ) is Union:  # Optional[T] is represented as Union[T, None]
        return type(None) in typing.get_args(typ)

    return False


def unwrap_optional_type(typ: Type[Optional[T]]) -> Type[T]:
    "For an optional type Optional[T], retrieves the underlying type T."

    # Optional[T] is represented internally as Union[T, None]
    if typing.get_origin(typ) is not Union:
        raise ValueError("optional type must have un-subscripted type of Union")

    # will automatically unwrap Union[T] into T
    return Union[
        tuple(filter(lambda item: item is not type(None), typing.get_args(typ)))
    ]


def dataclasses_to_stream(types: List[Dataclass], target: TextIO):
    "Generates Python code corresponding to a dataclass type."

    print("# This source file has been generated by a tool, do not edit", file=target)
    print("from dataclasses import dataclass, field", file=target)
    print("from datetime import date, datetime, time", file=target)
    print("from typing import Optional", file=target)
    print("from pylinsql.schema import *", file=target)
    print(file=target)

    for typ in types:
        print(file=target)
        print("@dataclass", file=target)
        print(f"class {typ.__name__}:", file=target)
        if typ.__doc__:
            print(f"    {repr(typ.__doc__)}", file=target)
            print(file=target)
        for field in dataclasses.fields(typ):
            if is_type_optional(field.type):
                inner_type = unwrap_optional_type(field.type)
                type_name = f"Optional[{inner_type.__name__}]"
            else:
                type_name = field.type.__name__
            if field.metadata:
                initializer = f" = field(metadata = {repr(dict(field.metadata))})"
            else:
                initializer = ""
            print(f"    {field.name}: {type_name}{initializer}", file=target)
        print(file=target)


def dataclasses_to_code(types: List[Dataclass]) -> str:
    f = io.StringIO()
    dataclasses_to_stream(types, f)
    return f.getvalue()
