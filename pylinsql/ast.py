"""
Abstract syntax tree synthesized from a Python Boolean lambda expression or the conditional part of a generator expression.

This module is used internally.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, List, Optional

from .core import *


class Expression:
    "An abstract syntax node synthesized from a Python Boolean lambda expression or the conditional part of a generator expression."

    def __str__(self):
        raise NotImplementedError("abstract node")


@dataclass(frozen=True)
class TopLevelExpression(Expression):
    precedence: ClassVar[int] = 0


@dataclass(frozen=True)
class Constant(Expression):
    precedence: ClassVar[int] = 15

    value: Any

    def negate(self) -> Constant:
        if self.value is True or self.value is False:
            return Constant(not self.value)
        else:
            raise TypeError(f"cannot negate non-Boolean type: {type(self.value)}")

    def __str__(self):
        return repr(self.value)


@dataclass(frozen=True)
class SequenceExpression(Expression):
    precedence: ClassVar[int] = 15

    exprs: List[Expression]

    def to_string(self, open: str, close: str):
        parts = ", ".join(str(e) for e in self.exprs)
        return f"{open}{parts}{close}"


@dataclass(frozen=True)
class TupleExpression(SequenceExpression):
    def __str__(self):
        return self.to_string("(", ")")


@dataclass(frozen=True)
class ListExpression(SequenceExpression):
    def __str__(self):
        return self.to_string("[", "]")


@dataclass(frozen=True)
class ClosureRef(Expression):
    precedence: ClassVar[int] = 15

    name: str

    def __str__(self):
        return str(self.name)


@dataclass(frozen=True)
class LocalRef(Expression):
    precedence: ClassVar[int] = 15

    name: str

    def __str__(self):
        return str(self.name)


@dataclass(frozen=True)
class GlobalRef(Expression):
    precedence: ClassVar[int] = 15

    name: str

    def __str__(self):
        return str(self.name)


@dataclass(frozen=True)
class AttributeAccess(Expression):
    precedence: ClassVar[int] = 14

    base: Expression
    attr_name: str

    def __str__(self):
        return f"{self.base}.{self.attr_name}"


@dataclass(frozen=True)
class IndexAccess(Expression):
    precedence: ClassVar[int] = 13

    base: Expression
    index: int

    def __str__(self):
        return f"{self.base}[{self.index}]"


@dataclass(frozen=True)
class FunctionCall(Expression):
    precedence: ClassVar[int] = 13

    base: Expression
    args: List[Expression]

    def negate(self) -> Expression:
        return Negation(self)

    def get_function_name(self) -> Optional[str]:
        if isinstance(self.base, GlobalRef):
            return self.base.name
        else:
            return None

    def is_dispatchable(self, fn: Callable) -> bool:
        "True if this function expression can be used to invoke the given signature."

        name = self.get_function_name()
        if name:
            return name == fn.__name__ and len(self.args) == len(
                inspect.signature(fn).parameters
            )
        else:
            return False

    def __str__(self):
        args = ", ".join(str(arg) for arg in self.args)
        return f"{self.base}({args})"


class Dispatcher:
    function_mapping: Dict[str, Callable]

    def __init__(self, functions: List[Callable]):
        self.function_mapping = {fn.__name__: fn for fn in functions}

    def _get(self, name: str) -> Optional[Callable]:
        return self.function_mapping.get(name, None)

    def get(self, call: FunctionCall) -> Optional[Callable]:
        name = call.get_function_name()
        if not name:
            return None

        fn = self._get(name)
        if not fn or not call.is_dispatchable(fn):
            return None

        return fn


@dataclass(frozen=True)
class UnaryExpression(Expression):
    expr: Expression

    def __str__(self):
        return f"{self.op}{self.expr}"


@dataclass(frozen=True)
class BinaryExpression(Expression):
    left: Expression
    right: Expression

    def __str__(self):
        return f"{self.left} {self.op} {self.right}"


@dataclass(frozen=True)
class Exponentiation(BinaryExpression):
    precedence: ClassVar[int] = 12
    op: ClassVar[str] = "**"


@dataclass(frozen=True)
class UnaryPlus(UnaryExpression):
    precedence: ClassVar[int] = 11
    op: ClassVar[str] = "+"


@dataclass(frozen=True)
class UnaryMinus(UnaryExpression):
    precedence: ClassVar[int] = 11
    op: ClassVar[str] = "-"


@dataclass(frozen=True)
class BitwiseNot(UnaryExpression):
    precedence: ClassVar[int] = 11
    op: ClassVar[str] = "~"


@dataclass(frozen=True)
class Multiplication(BinaryExpression):
    precedence: ClassVar[int] = 10
    op: ClassVar[str] = "*"


@dataclass(frozen=True)
class Division(BinaryExpression):
    precedence: ClassVar[int] = 10
    op: ClassVar[str] = "/"


@dataclass(frozen=True)
class Addition(BinaryExpression):
    precedence: ClassVar[int] = 9
    op: ClassVar[str] = "+"


@dataclass(frozen=True)
class Subtraction(BinaryExpression):
    precedence: ClassVar[int] = 9
    op: ClassVar[str] = "-"


@dataclass(frozen=True)
class BitwiseLeftShift(BinaryExpression):
    precedence: ClassVar[int] = 8
    op: ClassVar[str] = "<<"


@dataclass(frozen=True)
class BitwiseRightShift(BinaryExpression):
    precedence: ClassVar[int] = 8
    op: ClassVar[str] = ">>"


@dataclass(frozen=True)
class BitwiseAnd(BinaryExpression):
    precedence: ClassVar[int] = 7
    op: ClassVar[str] = "&"


@dataclass(frozen=True)
class BitwiseXor(BinaryExpression):
    precedence: ClassVar[int] = 6
    op: ClassVar[str] = "^"


@dataclass(frozen=True)
class BitwiseOr(BinaryExpression):
    precedence: ClassVar[int] = 5
    op: ClassVar[str] = "|"


def _negate_comparison_operator():
    ops = [
        ("==", "!="),
        ("<", ">="),
        ("<=", ">"),
        ("in", "not in"),
        ("is", "is not"),
    ]
    op_map = {}
    op_map.update((op[0], op[1]) for op in ops)
    op_map.update((op[1], op[0]) for op in ops)
    return op_map


@dataclass(frozen=True)
class Comparison(Expression):
    precedence: ClassVar[int] = 4

    negate_op: ClassVar[Dict[str, str]] = _negate_comparison_operator()

    op: str
    left: Expression
    right: Expression

    def negate(self) -> Expression:
        return Comparison(self.negate_op[self.op], self.left, self.right)

    def __str__(self):
        return f"{self.left} {self.op} {self.right}"


@dataclass(frozen=True)
class Negation(Expression):
    precedence: ClassVar[int] = 3

    expr: Expression

    def negate(self) -> Expression:
        return self.expr

    def __str__(self):
        return f"not {self.expr}"


@dataclass(frozen=True)
class BooleanExpression(Expression):
    exprs: List[Expression]

    def to_string(self, adjoiner: str):
        parts = f" {adjoiner} ".join(str(e) for e in self.exprs)
        return f"({parts})"


@dataclass(frozen=True)
class Conjunction(BooleanExpression):
    precedence: ClassVar[int] = 2

    def __str__(self):
        return self.to_string("and")


@dataclass(frozen=True)
class Disjunction(BooleanExpression):
    precedence: ClassVar[int] = 1

    def __str__(self):
        return self.to_string("or")
