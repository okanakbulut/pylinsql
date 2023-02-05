"""
Generates SQL DDL commands from a Python module definition.
"""

import dataclasses
import types
from typing import Dict, List, Optional, TextIO, TypeVar

from pylinsql.generator.conversion import (
    python_to_sql_type,
    sql_quoted_id,
    sql_quoted_str,
)
from pylinsql.generator.database_traits import (
    get_discriminated_key,
    get_foreign_key,
    get_primary_key,
    is_composite_type,
    is_table_type,
)
from pylinsql.generator.inspection import entity_classes
from pylinsql.generator.schema import DataClass
from strong_typing.auxiliary import int16, int32, int64
from strong_typing.docstring import Docstring, parse_type
from strong_typing.inspection import (
    is_dataclass_type,
    is_type_enum,
    is_type_optional,
    unwrap_optional_type,
)

T = TypeVar("T")


def module_to_sql_stream(module: types.ModuleType, target: TextIO) -> None:
    classes = entity_classes(module)

    enumerations = [cls for cls in classes.values() if is_type_enum(cls)]
    for cls in enumerations:
        enum_values = ", ".join(
            sql_quoted_str(m.value) for m in cls.__members__.values()
        )
        print(f"CREATE TYPE {cls.__name__} AS ENUM ({enum_values});", file=target)
    if enumerations:
        print(file=target)

    composite_types = [cls for cls in classes.values() if is_composite_type(cls)]
    for cls in composite_types:
        SQLConverter(cls).write_type(target)
    if composite_types:
        print(file=target)

    converters = [SQLConverter(cls) for cls in classes.values() if is_table_type(cls)]
    for converter in converters:
        converter.write_table(target)
    for converter in converters:
        converter.write_constraints(target)


class ForeignKeyDependencyResolver:
    "Discovers data classes referenced via foreign key definitions."

    def __init__(self, classes: Dict[str, type]):
        self.classes = classes

    def find(self, cls: type) -> List[type]:
        result = []

        if is_dataclass_type(cls):
            for field in dataclasses.fields(cls):
                foreign_key = get_foreign_key(field)
                if foreign_key is not None:
                    typ = self.classes.get(foreign_key.references.table)
                    if typ is not None:
                        result.append(typ)

        return result


def class_to_sql_stream(cls: DataClass[T], target: TextIO) -> None:
    SQLConverter(cls).write_table(target)


class SQLConverter:
    cls: type
    docs: Docstring

    def __init__(self, cls: T):
        self.cls = cls
        self.docs = parse_type(cls)

    def _get_field_description(self, field: dataclasses.Field) -> Optional[str]:
        description = field.metadata.get("description")
        if description is not None:
            return description

        param = self.docs.params.get(field.name)
        if param is not None:
            return param.description

        return None

    def write_comments(self, sql_object_type: str, target: TextIO) -> None:
        class_sql_name = sql_quoted_id(self.cls.__name__)
        comments: List[str] = []

        for field in dataclasses.fields(self.cls):
            description = self._get_field_description(field)
            if description is not None:
                comments.append(
                    f"COMMENT ON COLUMN {class_sql_name}.{sql_quoted_id(field.name)} IS {sql_quoted_str(description)};"
                )

        description = self.docs.full_description
        if description is not None:
            print(
                f"COMMENT ON {sql_object_type} {class_sql_name} IS {sql_quoted_str(description)};",
                file=target,
            )
        for comment in comments:
            print(comment, file=target)
        if comments:
            print(file=target)

    def write_type(self, target: TextIO) -> None:
        class_sql_name = sql_quoted_id(self.cls.__name__)
        defs: List[str] = []

        for field in dataclasses.fields(self.cls):
            field_sql_name = sql_quoted_id(field.name)

            # no constraints allowed on composite types (including NOT NULL)
            if is_type_optional(field.type):
                sql_type = python_to_sql_type(unwrap_optional_type(field.type))
            else:
                sql_inner_type = python_to_sql_type(field.type)
                sql_type = f"{sql_inner_type}"
            defs.append(f"{field_sql_name} {sql_type}")

        print(f"CREATE TYPE {class_sql_name} AS (", file=target)
        print(",\n".join(defs), file=target)
        print(f");\n", file=target)

        self.write_comments("TYPE", target)

    def write_table(self, target: TextIO) -> None:
        class_sql_name = sql_quoted_id(self.cls.__name__)

        defs: List[str] = []
        constraints: List[str] = []

        primary_key_column = None
        primary_key = get_primary_key(self.cls)
        if primary_key is not None:
            if isinstance(primary_key.column, str):
                column_list = sql_quoted_id(primary_key.column)
                primary_key_column = primary_key.column
            elif isinstance(primary_key.column, list):
                column_list = ", ".join(sql_quoted_id(id) for id in primary_key.column)

            constraints.append(
                f'CONSTRAINT "{primary_key.name}" PRIMARY KEY ({column_list})'
            )

        for field in dataclasses.fields(self.cls):
            field_sql_name = sql_quoted_id(field.name)

            if field.name == primary_key_column:
                sql_inner_type = python_to_sql_type(field.type)
                if field.type is int16 or field.type is int32 or field.type is int64:
                    sql_type = f"{sql_inner_type} GENERATED BY DEFAULT AS IDENTITY"
                else:
                    sql_type = sql_inner_type
            elif is_type_optional(field.type):
                sql_type = python_to_sql_type(unwrap_optional_type(field.type))
            else:
                sql_inner_type = python_to_sql_type(field.type)
                sql_type = f"{sql_inner_type} NOT NULL"
            defs.append(f"{field_sql_name} {sql_type}")

        defs.extend(constraints)
        print(f"CREATE TABLE {class_sql_name}(", file=target)
        print(",\n".join(defs), file=target)
        print(f");\n", file=target)

        self.write_comments("TABLE", target)

    def write_constraints(self, target: TextIO) -> None:
        class_sql_name = sql_quoted_id(self.cls.__name__)

        constraints: List[str] = []
        for field in dataclasses.fields(self.cls):
            field_sql_name = sql_quoted_id(field.name)

            foreign_key = get_foreign_key(field)
            if foreign_key:
                fk_sql_name = sql_quoted_id(foreign_key.name)
                pk_sql_table = sql_quoted_id(foreign_key.references.table)
                pk_sql_column = sql_quoted_id(foreign_key.references.column)
                constraints.append(
                    f"ADD CONSTRAINT {fk_sql_name} FOREIGN KEY ({field_sql_name}) REFERENCES {pk_sql_table}({pk_sql_column})"
                )

        if constraints:
            print(f"ALTER TABLE {class_sql_name}", file=target)
            print(",\n".join(constraints), file=target)
            print(f";\n", file=target)

        for field in dataclasses.fields(self.cls):
            field_sql_name = sql_quoted_id(field.name)

            discriminated_key = get_discriminated_key(field)
            if discriminated_key:
                dk_sql_name = sql_quoted_id(discriminated_key.name)
                dk_sql_column = sql_quoted_id(discriminated_key.discriminator)
                print(
                    f"CREATE INDEX {dk_sql_name} ON {class_sql_name} ({dk_sql_column}, {field_sql_name});\n",
                    file=target,
                )
