import dataclasses
from dataclasses import dataclass
from typing import List, Union


class _CompactDataClass:
    "A data class whose repr() uses positional rather than keyword arguments."

    def __repr__(self) -> str:
        arglist = ", ".join(
            repr(getattr(self, field.name)) for field in dataclasses.fields(self)
        )
        return f"{self.__class__.__name__}({arglist})"


@dataclass(frozen=True, repr=False)
class Reference(_CompactDataClass):
    "Captures a set of columns in a table referenced by a foreign key constraint."

    table: str
    column: Union[str, List[str]]


@dataclass(frozen=True, repr=False)
class PrimaryKey(_CompactDataClass):
    "Identifies a set of columns in a table as part of the primary key."

    name: str
    column: Union[str, List[str]]


@dataclass(frozen=True, repr=False)
class ForeignKey(_CompactDataClass):
    "Declares a foreign key in a table."

    name: str
    references: Reference
