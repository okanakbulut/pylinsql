"""
Constructs a PostgreSQL DML/DQL statement from an abstract syntax tree expression.

This module is used internally.
"""

from __future__ import annotations

import builtins
import dataclasses
import enum
import functools
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from .ast import *
from .base import is_dataclass_instance
from .core import *

_aggregate_functions = Dispatcher([avg, count, max, min, sum])
_conditional_aggregate_functions = Dispatcher(
    [avg_if, count_if, max_if, min_if, sum_if]
)
_datetime_functions = Dispatcher([year, month, day, hour, minute, second])
_join_functions = Dispatcher([full_join, inner_join, left_join, right_join])
_order_functions = Dispatcher([asc, desc])


@enum.unique
class _JoinType(enum.Enum):
    InnerJoin = inner_join.__name__
    LeftJoin = left_join.__name__
    RightJoin = right_join.__name__
    FullJoin = full_join.__name__


@dataclass
class _EntityJoin:
    join_type: _JoinType
    left_entity: str
    left_attr: str
    right_entity: str
    right_attr: str

    def swap(self) -> _EntityJoin:
        if self.join_type is _JoinType.LeftJoin:
            swap_type = _JoinType.RightJoin
        elif self.join_type is _JoinType.RightJoin:
            swap_type = _JoinType.LeftJoin
        else:
            swap_type = self.join_type
        return _EntityJoin(
            swap_type,
            self.right_entity,
            self.right_attr,
            self.left_entity,
            self.left_attr,
        )

    def as_join(self, entity_aliases):
        if self.join_type is _JoinType.InnerJoin:
            join_type = "INNER"
        elif self.join_type is _JoinType.LeftJoin:
            join_type = "LEFT"
        elif self.join_type is _JoinType.RightJoin:
            join_type = "RIGHT"
        elif self.join_type is _JoinType.FullJoin:
            join_type = "FULL"
        return f"{join_type} JOIN {entity_aliases[self.right_entity]} ON {self.left_entity}.{self.left_attr} = {self.right_entity}.{self.right_attr}"


class _EntityJoinCollection:
    entity_joins: Dict[Tuple[str, str], _EntityJoin]

    def __init__(self):
        self.entity_joins = {}

    def __str__(self) -> str:
        return "\n".join(str(e) for e in self.entity_joins.values())

    def __bool__(self) -> bool:
        return len(self.entity_joins) > 0

    def add(
        self,
        join_type: _JoinType,
        left_entity: str,
        left_attr: str,
        right_entity: str,
        right_attr: str,
    ):
        min_entity = builtins.min(left_entity, right_entity)
        max_entity = builtins.max(left_entity, right_entity)
        self.entity_joins[(min_entity, max_entity)] = _EntityJoin(
            join_type, left_entity, left_attr, right_entity, right_attr
        )

    def pop(self, left_entity: str, right_entity: str) -> Optional[_EntityJoin]:
        min_entity = builtins.min(left_entity, right_entity)
        max_entity = builtins.max(left_entity, right_entity)
        entity_join = self.entity_joins.pop((min_entity, max_entity), None)
        if entity_join:
            if left_entity != entity_join.left_entity:
                return entity_join.swap()
            else:
                return entity_join
        else:
            return None


@enum.unique
class _OrderType(enum.Enum):
    Ascending = asc.__name__
    Descending = desc.__name__


class _QueryExtractor:
    def _boolean_simplify(self, bool_expr: BooleanExpression) -> Expression:
        parts = list(filter(None, (self.visit(expr) for expr in bool_expr.exprs)))
        if len(parts) > 1:
            return bool_expr
        elif len(parts) == 1:
            return parts[0]
        else:
            return None


class _QueryValidator(_QueryExtractor):
    """
    Validates if a Python generator expression is a valid query expression.
    """

    @functools.singledispatchmethod
    def visit(self, arg):
        return arg

    @visit.register
    def _(self, bool_expr: BooleanExpression):
        return self._boolean_simplify(bool_expr)

    @visit.register
    def _(self, call: FunctionCall):
        fn = _order_functions.get(call)
        if fn:
            raise TypeError(
                f"order function {fn.__name__} can only be used as a top-level wrapper in the target expression part of the Python generator expression"
            )

        return call


class _JoinExtractor(_QueryExtractor):
    """
    Extracts the join part from a Python generator expression to be used in a SQL FROM clause.
    """

    entity_joins: _EntityJoinCollection

    def __init__(self):
        self.entity_joins = _EntityJoinCollection()

    @functools.singledispatchmethod
    def visit(self, arg):
        return arg

    @visit.register
    def _(self, bool_expr: BooleanExpression):
        return self._boolean_simplify(bool_expr)

    def _join_expr(self, join_type: _JoinType, left: Expression, right: Expression):
        if not (
            isinstance(left, AttributeAccess)
            and isinstance(right, AttributeAccess)
            and isinstance(left.base, LocalRef)
            and isinstance(right.base, LocalRef)
        ):
            raise TypeError(
                "join expressions must adhere to the format: join(entity1.attr1, entity2.attr2)"
            )

        self.entity_joins.add(
            join_type,
            left.base.name,
            left.attr_name,
            right.base.name,
            right.attr_name,
        )

    @visit.register
    def _(self, call: FunctionCall):
        fn = _join_functions.get(call)
        if fn:
            return self._join_expr(_JoinType(fn.__name__), call.args[0], call.args[1])

        return call


class _ConditionExtractor(_QueryExtractor):
    """
    Extracts the conditional part from a Python generator expression to be used in a SQL WHERE or HAVING clause.

    This class takes the abstract syntax tree of a Python generator expression such as
    ```
    ( p for p in entity(Person) if p.given_name == "John" and min(p.birth_year) >= 1980 )
    ```
    and (depending on initialization options) extracts `p.given_name == "John"`, which goes into the WHERE clause, or
    `min(p.birth_year) >= 1980`, which goes into the HAVING clause.

    :param vars: Variables in the abstract syntax tree that correspond to entities.
    :param aggregation: Whether the operation is to gather WHERE terms (False) or HAVING terms (True).
    """

    def __init__(self, local_vars: List[str], aggregation: bool):
        self.local_vars = local_vars
        self.aggregation = aggregation
        self.in_aggregation_fn = False

    @functools.singledispatchmethod
    def visit(self, arg: Expression) -> Expression:
        return arg

    @visit.register
    def _(self, bool_expr: BooleanExpression):
        return self._boolean_simplify(bool_expr)

    @visit.register
    def _(self, unary_expr: UnaryExpression):
        inner_expr = self.visit(unary_expr.expr)
        if inner_expr:
            return type(unary_expr)(inner_expr)
        else:
            return None

    @visit.register
    def _(self, binary_expr: BinaryExpression):
        left_expr = self.visit(binary_expr.left)
        right_expr = self.visit(binary_expr.right)
        if left_expr and right_expr:
            return type(binary_expr)(left_expr, right_expr)
        else:
            return None

    @visit.register
    def _(self, comp: Comparison):
        left = self.visit(comp.left)
        right = self.visit(comp.right)
        if left and right:
            return Comparison(comp.op, left, right)
        else:
            return None

    @visit.register
    def _(self, call: FunctionCall):
        fn = _aggregate_functions.get(call)
        if fn:
            return self._get_aggregation_func(call)

        fn = _conditional_aggregate_functions.get(call)
        if fn:
            return self._get_aggregation_func(call)

        if not self.aggregation:
            return FunctionCall(
                self.visit(call.base), [self.visit(arg) for arg in call.args]
            )

        return None

    def _get_aggregation_func(self, call: FunctionCall) -> Optional[FunctionCall]:
        if self.aggregation:
            self.in_aggregation_fn = True
            args = [self.visit(arg) for arg in call.args]
            self.in_aggregation_fn = False

            return FunctionCall(self.visit(call.base), args)
        else:
            return None

    @visit.register
    def _(self, attr: AttributeAccess):
        base = self.visit(attr.base)
        if base:
            return AttributeAccess(base, attr.attr_name)
        else:
            return None

    @visit.register
    def _(self, arg: ClosureRef):
        return arg

    @visit.register
    def _(self, arg: GlobalRef):
        return arg

    @visit.register
    def _(self, arg: LocalRef):
        if arg.name in self.local_vars and (
            self.aggregation
            and not self.in_aggregation_fn
            or not self.aggregation
            and self.in_aggregation_fn
        ):
            return None

        return arg

    @visit.register
    def _(self, arg: Constant):
        return arg


_query_parameters = [p_1, p_2, p_3, p_4, p_5, p_6, p_7, p_8, p_9]


class _QueryVisitor:
    parameters: Set[_QueryParameter]
    stack: List[Expression]

    def __init__(self, closure_vars: Dict[str, Any]):
        self.closure_vars = closure_vars
        self.parameters = set()
        self.stack = [TopLevelExpression()]

    def visit(self, arg: Expression) -> str:
        self.stack.append(arg)
        expr = self._visit(arg)
        self.stack.pop()

        if arg.precedence < self.stack[-1].precedence:
            return f"({expr})"
        else:
            return expr

    @functools.singledispatchmethod
    def _visit(self, arg):
        raise NotImplementedError(
            f"unrecognized expression: {arg} (of type {type(arg)})"
        )

    def _sql_where_expr(self, adjoiner: str, exprs: List[Expression]) -> Optional[str]:
        parts = list(filter(None, (self.visit(expr) for expr in exprs)))
        if len(parts) > 1:
            return f" {adjoiner} ".join(parts)
        elif len(parts) == 1:
            return parts[0]
        else:
            return None

    def _sql_unary_expr(self, op: str, unary_expr: UnaryExpression) -> str:
        expr = self.visit(unary_expr.expr)
        return f"{op} {expr}"

    def _sql_binary_expr(self, op: str, binary_expr: BinaryExpression) -> str:
        left = self.visit(binary_expr.left)
        right = self.visit(binary_expr.right)
        return f"{left} {op} {right}"

    @_visit.register
    def _(self, conj: Conjunction):
        return self._sql_where_expr("AND", conj.exprs)

    @_visit.register
    def _(self, disj: Disjunction):
        return self._sql_where_expr("OR", disj.exprs)

    @_visit.register
    def _(self, neg: Negation):
        return self._sql_unary_expr("NOT ", neg)

    @_visit.register
    def _(self, expr: UnaryPlus):
        return self._sql_unary_expr("+", expr)

    @_visit.register
    def _(self, expr: UnaryMinus):
        return self._sql_unary_expr("-", expr)

    @_visit.register
    def _(self, expr: Exponentiation):
        return self._sql_binary_expr("^", expr)

    @_visit.register
    def _(self, expr: Multiplication):
        return self._sql_binary_expr("*", expr)

    @_visit.register
    def _(self, expr: Division):
        return self._sql_binary_expr("/", expr)

    @_visit.register
    def _(self, expr: Addition):
        return self._sql_binary_expr("+", expr)

    @_visit.register
    def _(self, expr: Subtraction):
        return self._sql_binary_expr("-", expr)

    @_visit.register
    def _(self, expr: BitwiseNot):
        return self._sql_unary_expr("~", expr)

    @_visit.register
    def _(self, expr: BitwiseAnd):
        return self._sql_binary_expr("&", expr)

    @_visit.register
    def _(self, expr: BitwiseXor):
        return self._sql_binary_expr("#", expr)

    @_visit.register
    def _(self, expr: BitwiseOr):
        return self._sql_binary_expr("|", expr)

    @_visit.register
    def _(self, comp: Comparison):
        if isinstance(comp.right, Constant) and comp.right.value is None:
            left = self.visit(comp.left)
            if comp.op == "is":
                return f"{left} IS NULL"
            elif comp.op == "is not":
                return f"{left} IS NOT NULL"
        elif comp.op in ["in", "not in"]:
            left = self.visit(comp.left)
            right = self.visit(comp.right)
            if comp.op == "in":
                return f"{left} IN {right}"
            elif comp.op == "not in":
                return f"{left} NOT IN {right}"
        else:
            left = self.visit(comp.left)
            right = self.visit(comp.right)
            binary_ops = {
                "==": "=",
                "!=": "<>",
                "<": "<",
                "<=": "<=",
                ">=": ">=",
                ">": ">",
            }
            if comp.op in binary_ops:
                op = binary_ops[comp.op]
                return f"{left} {op} {right}"

        raise TypeError(f"illegal comparison: {comp}")

    @_visit.register
    def _(self, call: FunctionCall):
        fn = _aggregate_functions.get(call)
        if fn:
            args = ", ".join([self.visit(arg) for arg in call.args])
            func = call.base.name.upper()
            return f"{func}({args})"

        fn = _conditional_aggregate_functions.get(call)
        if fn:
            self.stack.append(TopLevelExpression())
            expr, cond = [self.visit(arg) for arg in call.args]
            self.stack.pop()
            func = call.base.name.replace("_if", "").upper()
            return f"{func}({expr}) FILTER (WHERE {cond})"

        fn = _datetime_functions.get(call)
        if fn:
            arg = self.visit(call.args[0])
            return f"EXTRACT({call.base.name.upper()} FROM {arg})"

        if call.is_dispatchable(now):
            return "CURRENT_TIMESTAMP"

        raise ValueError(f"unrecognized function call: {call}")

    @_visit.register
    def _(self, arg: AttributeAccess):
        base = self.visit(arg.base)
        return f"{base}.{arg.attr_name}"

    @_visit.register
    def _(self, arg: ClosureRef):
        value = self.closure_vars[arg.name]
        if isinstance(value, Query):
            return f"({value.sql})"
        else:
            return value

    @_visit.register
    def _(self, arg: GlobalRef):
        for param in _query_parameters:
            if arg.name == param.name:
                self.parameters.add(param)
                return param
        return arg.name

    @_visit.register
    def _(self, arg: LocalRef):
        return arg.name

    @_visit.register
    def _(self, arg: TupleExpression):
        self.stack.append(TopLevelExpression())
        value = ", ".join(self.visit(expr) for expr in arg.exprs)
        self.stack.pop()
        return value

    @_visit.register
    def _(self, arg: Constant):
        if isinstance(arg.value, str):
            return "'" + arg.value.replace("'", "''") + "'"
        else:
            return str(arg.value)


@dataclass
class Context:
    local_vars: List[str]
    closure_vars: Dict[str, Any]


class _SelectExtractor(_QueryExtractor):
    query_visitor: _QueryVisitor
    local_vars: List[str]
    select: List[str]
    group_by: List[str]
    order_by: List[str]
    has_aggregate: bool

    def __init__(self, query_visitor: _QueryVisitor, local_vars: List[str]):
        self.query_visitor = query_visitor
        self.local_vars = local_vars
        self.select = []
        self.group_by = []
        self.order_by = []
        self.has_aggregate = False

    def visit(self, expr: Expression) -> Expression:
        self._visit(expr)

    def _visit_expr(self, expr: Expression) -> str:
        item = self.query_visitor.visit(expr)
        if self._is_aggregate(expr):
            self.has_aggregate = True
        else:
            self.group_by.append(item)
        self.select.append(item)
        return item

    @functools.singledispatchmethod
    def _visit(self, expr: Expression):
        self._visit_expr(expr)

    @_visit.register
    def _(self, call: FunctionCall):
        fn = _order_functions.get(call)
        if fn:
            item = self._visit_expr(call.args[0])
            order = _OrderType(fn.__name__).value.upper()
            self.order_by.append(f"{item} {order}")
        else:
            self._visit_expr(call)

    @_visit.register
    def _(self, ref: LocalRef):
        self.select.append("*")

    @_visit.register
    def _(self, tup: TupleExpression):
        for expr in tup.exprs:
            self._visit(expr)

    def _is_aggregate(self, expr):
        "True if an expression in a SELECT clause is an aggregation expression."

        return (
            _ConditionExtractor(self.local_vars, aggregation=True).visit(expr)
            is not None
        )


@dataclass
class QueryBuilderArgs:
    source: EntityProxy
    context: Context
    cond_expr: Expression
    yield_expr: Expression


class QueryBuilder:
    def select(self, qba: QueryBuilderArgs) -> str:
        _QueryValidator().visit(qba.cond_expr)
        query_visitor = _QueryVisitor(qba.context.closure_vars)

        # extract JOIN clause from "if" part of generator expression
        join_simplifier = _JoinExtractor()
        cond_expr = join_simplifier.visit(qba.cond_expr)
        entity_joins = join_simplifier.entity_joins

        # construct JOIN expression "a JOIN b ON a.foreign_key = b.primary_key JOIN ..."
        entity_aliases = {
            var: f'"{typ.__name__}" AS {var}'
            for typ, var in zip(qba.source.types, qba.context.local_vars)
        }
        remaining_entities = qba.context.local_vars.copy()
        sql_join = []
        while remaining_entities:
            first = remaining_entities.pop(0)
            joined_entities = set([first])
            sql_join_group = [entity_aliases[first]]

            while True:
                entity_join = self._match_entities(
                    entity_joins, joined_entities, remaining_entities
                )
                if not entity_join:
                    break

                joined_entities.add(entity_join.right_entity)
                remaining_entities.remove(entity_join.right_entity)

                sql_join_group.append(entity_join.as_join(entity_aliases))

            sql_join.append(" ".join(sql_join_group))

        # construct WHERE expression
        where_expr = _ConditionExtractor(
            qba.context.local_vars, aggregation=False
        ).visit(cond_expr)
        sql_where = query_visitor.visit(where_expr) if where_expr else None

        # construct HAVING expression
        having_expr = _ConditionExtractor(
            qba.context.local_vars, aggregation=True
        ).visit(cond_expr)
        sql_having = query_visitor.visit(having_expr) if having_expr else None

        # construct SELECT expression
        select_visitor = _SelectExtractor(query_visitor, qba.context.local_vars)
        select_visitor.visit(qba.yield_expr)
        sql_group = select_visitor.group_by if select_visitor.has_aggregate else None
        sql_select = select_visitor.select
        sql_order = select_visitor.order_by

        sql_parts = ["SELECT"]
        sql_parts.append(", ".join(sql_select))
        if sql_join:
            sql_parts.extend(["FROM", ", ".join(sql_join)])
        if sql_where:
            sql_parts.extend(["WHERE", sql_where])
        if sql_group:
            sql_parts.extend(["GROUP BY", ", ".join(sql_group)])
        if sql_having:
            sql_parts.extend(["HAVING", sql_having])
        if sql_order:
            sql_parts.extend(["ORDER BY", ", ".join(sql_order)])
        return " ".join(sql_parts)

    def insert_or_select(self, qba: QueryBuilderArgs, insert_obj: DataClass) -> str:
        if not is_dataclass_instance(insert_obj):
            raise TypeError(f"{insert_obj} must be a dataclass instance")

        _QueryValidator().visit(qba.cond_expr)
        query_visitor = _QueryVisitor(qba.context.closure_vars)

        # check JOIN clause in "if" part of generator expression
        join_simplifier = _JoinExtractor()
        cond_expr = join_simplifier.visit(qba.cond_expr)
        if join_simplifier.entity_joins:
            raise ValueError(
                "no join conditions are allowed in an insert or select query"
            )

        # construct FROM expression
        if len(qba.source.types) != 1:
            raise ValueError(
                "a single target entity is required for an insert or select query"
            )
        entity_type = qba.source.types[0]
        entity_var = qba.context.local_vars[0]
        if not isinstance(insert_obj, entity_type):
            raise TypeError(
                f"object to insert has wrong type: {type(insert_obj)}, expected: {entity_type}"
            )
        sql_from = f'"{entity_type.__name__}" AS {entity_var}'

        # construct WHERE expression
        where_expr = _ConditionExtractor(
            qba.context.local_vars, aggregation=False
        ).visit(cond_expr)
        sql_where = query_visitor.visit(where_expr) if where_expr else None

        # check HAVING expression
        if _ConditionExtractor(qba.context.local_vars, aggregation=True).visit(
            cond_expr
        ):
            raise ValueError(
                "no aggregation functions are allowed in an insert or select query"
            )

        # construct SELECT expression
        select_visitor = _SelectExtractor(query_visitor, qba.context.local_vars)
        select_visitor.visit(qba.yield_expr)
        if select_visitor.has_aggregate:
            raise ValueError(
                "no aggregation functions are allowed in an insert or select query"
            )

        sql_select = select_visitor.select
        sql_order = select_visitor.order_by

        sql_select_column_names = ", ".join(sql_select)
        select_parts = ["SELECT", sql_select_column_names, "FROM", sql_from]
        if sql_where:
            select_parts.extend(["WHERE", sql_where])
        if sql_order:
            select_parts.extend(["ORDER BY", ", ".join(sql_order)])
        select_query = " ".join(select_parts)

        if query_visitor.parameters:
            offset = builtins.max(param.index for param in query_visitor.parameters) + 1
        else:
            offset = 1

        fields = dataclasses.fields(insert_obj)
        insert_names = [
            field.name
            for field in fields
            if getattr(insert_obj, field.name) is not DEFAULT
        ]
        sql_insert_names = ", ".join(insert_names)
        sql_insert_placeholders = ", ".join(
            f"${index + offset}" for index in range(len(insert_names))
        )
        insert_query = f"INSERT INTO {sql_from} ({sql_insert_names}) SELECT {sql_insert_placeholders} WHERE NOT EXISTS (SELECT * FROM select_query) RETURNING {sql_select_column_names}"

        return f"WITH select_query AS ({select_query}), insert_query AS ({insert_query}) SELECT * FROM select_query UNION ALL SELECT * FROM insert_query"

    def _match_entities(
        self, entity_joins, joined_entities: List[str], remaining_entities: List[str]
    ) -> Optional[_EntityJoin]:
        """
        Pairs up entities with one another along a join expression.

        :joined_entities: Entities already joined by previous INNER, LEFT and RIGHT joins.
        :remaining_entities: Entities to be paired up with previously joined entities.
        :returns: A new pair not already in the joined entities set (or None).
        """

        for left in joined_entities:
            for right in remaining_entities:
                entity_join = entity_joins.pop(left, right)
                if entity_join:
                    return entity_join
        return None
