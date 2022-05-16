"""
Provides information on database classes defined in a Python module.
"""

import dataclasses
import inspect
import linecache
import re
import types
from typing import Dict, List, Set

from strong_typing.inspection import is_dataclass_type, is_type_enum

from .schema import DiscriminatedKey, ForeignKey, Reference


def _classes_in_source(module: types.ModuleType) -> Dict[str, int]:
    "Retrieve a dictionary of key/value pairs mapping class names in a module to source code line numbers."

    file = inspect.getsourcefile(module)
    if file:
        # invalidate cache if needed
        linecache.checkcache(file)
    else:
        file = inspect.getfile(module)

    lines = linecache.getlines(file, module.__dict__)
    if not lines:
        raise OSError("could not get source code")

    result: Dict[str, int] = {}
    for lineno, line in enumerate(lines, start=1):
        m = re.match(r"^class ([A-Za-z_][A-Za-z0-9_]*)", line)
        if m:
            result[m.group(1)] = lineno

    return result


def _sort_classes_by_lineno(module: types.ModuleType, classes: List[type]) -> None:
    # keep the order of classes as they are defined in the source file
    class_lineno = _classes_in_source(module)
    classes.sort(key=lambda cls: class_lineno.get(cls.__name__, 0))


def entity_classes(module: types.ModuleType) -> Dict[str, type]:
    if not inspect.ismodule(module):
        raise TypeError(f"expected Python module but got: {module}")

    # skip types that are not data or enumeration classes and types imported from other modules
    classes = [
        cls
        for _, cls in inspect.getmembers(
            module,
            lambda cls: (dataclasses.is_dataclass(cls) or is_type_enum(cls))
            and cls.__module__ == module.__name__,
        )
    ]

    _sort_classes_by_lineno(module, classes)
    return {cls.__name__: cls for cls in classes}


class _KeyValidator:
    entities: Dict[str, type]
    field_names: Dict[str, List[str]]
    key_names: Set[str]
    verbose: bool

    def __init__(self, module: types.ModuleType, verbose: bool = True) -> None:
        self.entities = entity_classes(module)
        self.field_names = {
            class_name: [f.name for f in dataclasses.fields(class_type)]
            for class_name, class_type in self.entities.items()
            if dataclasses.is_dataclass(class_type)
        }
        self.key_names = set()
        self.verbose = verbose

    def _validate_unique(self, key_name: str) -> bool:
        if key_name in self.key_names:
            print(f"{key_name} is not unique")
            return False
        else:
            self.key_names.add(key_name)
            return True

    def _validate_reference(self, key_name: str, reference: Reference) -> bool:
        if reference.table not in self.entities:
            if self.verbose:
                print(f"{key_name} references non-existent table `{reference.table}`")
            return False

        if reference.column not in self.field_names[reference.table]:
            if self.verbose:
                print(
                    f"{key_name} references non-existent field `{reference.column}` in `{reference.table}`"
                )
            return False

        return True

    def validate(self) -> bool:
        result = True
        for entity in self.entities.values():
            if not is_dataclass_type(entity):
                continue

            for field in dataclasses.fields(entity):
                data = field.metadata.get("foreign_key")
                if data is None:
                    continue

                if isinstance(data, ForeignKey):
                    f_key: ForeignKey = data

                    if not self._validate_unique(f_key.name):
                        result = False

                    if not self._validate_reference(
                        f"foreign key {f_key.name}", f_key.references
                    ):
                        result = False

                elif isinstance(data, DiscriminatedKey):
                    d_key: DiscriminatedKey = data

                    if not self._validate_unique(d_key.name):
                        result = False

                    for ref in d_key.references:
                        if not self._validate_reference(
                            f"discriminated key {d_key.name}", ref
                        ):
                            result = False

        return result


def validate(module: types.ModuleType) -> bool:
    "Validates a module generated from a PostgreSQL schema for correctness and consistency."

    return _KeyValidator(module).validate()
