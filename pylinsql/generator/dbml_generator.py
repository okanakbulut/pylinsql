"""
Generates Database Markup Language (DBML) documents from a Python module definition.

:seealso: https://www.dbml.org/docs/
"""

import dataclasses
import re
import types
from typing import List, TextIO

from pylinsql.generator.conversion import python_to_sql_type
from pylinsql.generator.database_traits import (
    get_foreign_key,
    get_primary_key,
    is_table_type,
)
from pylinsql.generator.inspection import entity_classes
from strong_typing.auxiliary import int16, int32, int64
from strong_typing.inspection import (
    is_dataclass_type,
    is_type_enum,
    is_type_optional,
    unwrap_optional_type,
)


def dbml_identifier(text: str) -> str:
    if re.match(r"^[A-Za-z_]+(?:[(][A-Za-z0-9_]+[)])?$", text):
        # examples: bigint, varchar(255)
        return text
    else:
        string = text.replace('"', '\\"')
        return f'"{string}"'


def enum_to_stream(cls: type, target: TextIO) -> None:
    print(f"enum {cls.__name__} {{", file=target)
    for m in cls.__members__.values():
        print(f"    {dbml_identifier(m.value)}", file=target)
    print(f"}}", file=target)


def table_to_stream(cls: type, target: TextIO) -> None:
    indexes: List[str] = []
    primary_key_column = None

    primary_key = get_primary_key(cls)
    if primary_key is not None:
        if isinstance(primary_key.column, str):
            primary_key_column = primary_key.column
        elif isinstance(primary_key.column, list):
            column_list = ", ".join(dbml_identifier(id) for id in primary_key.column)
            indexes.append(f"({column_list}) [primary key]")

    # table definition
    print(f"Table {cls.__name__} {{", file=target)

    for field in dataclasses.fields(cls):
        settings: List[str] = []

        # column type
        if field.name == primary_key_column:
            field_type = field.type
            settings.append("primary key")
            if field.type is int16 or field.type is int32 or field.type is int64:
                settings.append("increment")
        elif is_type_optional(field.type):
            field_type = unwrap_optional_type(field.type)
            settings.append("null")
        else:
            field_type = field.type
            settings.append("not null")

        # column definition
        if is_type_enum(field_type) or is_table_type(field_type):
            python_type = field_type.__name__
        elif is_dataclass_type(field_type):
            python_type = "jsonb"
        else:
            python_type = python_to_sql_type(field_type, compact=True, custom=False)
        sql_type = dbml_identifier(python_type)
        field_name = dbml_identifier(field.name)
        if settings:
            property_list = ", ".join(settings)
            print(f"    {field_name} {sql_type} [{property_list}]", file=target)
        else:
            print(f"    {field_name} {sql_type}", file=target)

    if indexes:
        print("    Indexes {", file=target)
        for index in indexes:
            print(f"        {index}", file=target)
        print("    }", file=target)

    print(f"}}", file=target)

    # many-to-one relationships
    fk_sql_table = dbml_identifier(cls.__name__)
    for field in dataclasses.fields(cls):
        foreign_key = get_foreign_key(field)
        if foreign_key:
            fk_name = dbml_identifier(foreign_key.name)
            fk_sql_column = dbml_identifier(field.name)
            pk_sql_table = dbml_identifier(foreign_key.references.table)
            pk_sql_column = dbml_identifier(foreign_key.references.column)
            print(
                f"Ref {fk_name}: {fk_sql_table}.{fk_sql_column} > {pk_sql_table}.{pk_sql_column}",
                file=target,
            )


def module_to_sql_stream(module: types.ModuleType, target: TextIO) -> None:
    classes = entity_classes(module)

    enumerations = [cls for cls in classes.values() if is_type_enum(cls)]
    for cls in enumerations:
        enum_to_stream(cls, target)

    tables = [cls for cls in classes.values() if is_table_type(cls)]
    for table in tables:
        table_to_stream(table, target)
