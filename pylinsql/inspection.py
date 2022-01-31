import dataclasses
import inspect
import types
from typing import Dict, List, Type

from strong_typing.inspection import is_dataclass_type, is_type_enum

from .schema import DiscriminatedKey, ForeignKey, Reference


def entity_classes(module: types.ModuleType) -> Dict[str, Type]:
    if not inspect.ismodule(module):
        raise TypeError(f"expected Python module but got: {module}")

    # skip types that are not data or enumeration classes and types imported from other modules
    return {
        cls.__name__: cls
        for _, cls in inspect.getmembers(
            module,
            lambda cls: (dataclasses.is_dataclass(cls) or is_type_enum(cls))
            and cls.__module__ == module.__name__,
        )
    }


class _KeyValidator:
    entities: Dict[str, Type]
    field_names: Dict[str, List[str]]

    def __init__(self, module: types.ModuleType) -> None:
        self.entities = entity_classes(module)
        self.field_names = {
            class_name: [f.name for f in dataclasses.fields(class_type)]
            for class_name, class_type in self.entities.items()
            if dataclasses.is_dataclass(class_type)
        }

    def _validate_reference(self, key_name: str, reference: Reference) -> bool:
        if reference.table not in self.entities:
            print(f"{key_name} references non-existent table `{reference.table}`")
            return False

        if reference.column not in self.field_names[reference.table]:
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
                    if not self._validate_reference(
                        f"foreign key {f_key.name}", f_key.references
                    ):
                        result = False

                elif isinstance(data, DiscriminatedKey):
                    d_key: DiscriminatedKey = data
                    for ref in d_key.references:
                        if not self._validate_reference(
                            f"discriminated key {d_key.name}", ref
                        ):
                            result = False

        return result


def validate(module: types.ModuleType) -> bool:
    return _KeyValidator(module).validate()
