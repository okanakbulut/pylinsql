from dataclasses import dataclass
import dataclasses


class _CompactDataclass:
    "A data class whose repr() uses positional rather than keyword arguments."

    def __repr__(self) -> str:
        arglist = ", ".join(
            repr(getattr(self, field.name)) for field in dataclasses.fields(self)
        )
        return f"{self.__class__.__name__}({arglist})"


@dataclass(frozen=True, repr=False)
class Reference(_CompactDataclass):
    table: str
    column: str


@dataclass(frozen=True, repr=False)
class ForeignKey(_CompactDataclass):
    name: str
    references: Reference
