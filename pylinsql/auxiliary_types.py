import dataclasses
from dataclasses import dataclass
from typing import Optional

from typing_extensions import Annotated


class CompactDataClass:
    "A data class whose repr() uses positional rather than keyword arguments."

    def __repr__(self) -> str:
        arglist = ", ".join(
            repr(getattr(self, field.name)) for field in dataclasses.fields(self)
        )
        return f"{self.__class__.__name__}({arglist})"


@dataclass(frozen=True, repr=False)
class MaxLength(CompactDataClass):
    value: int


@dataclass(frozen=True, repr=False)
class Precision(CompactDataClass):
    significant_digits: int
    decimal_digits: Optional[int] = 0


@dataclass(frozen=True, repr=False)
class Storage(CompactDataClass):
    bytes: int


int16 = Annotated[int, Storage(2)]
int32 = Annotated[int, Storage(4)]
int64 = Annotated[int, Storage(8)]
