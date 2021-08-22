"""
Construct a SQL query from a Python expression.
"""

from __future__ import annotations

import functools
import inspect
import sys
from typing import Generator, List, Type

from .builder import Context, QueryBuilder, QueryBuilderArgs
from .core import EntityProxy, Query
from .decompiler import CodeExpression, CodeExpressionAnalyzer


def get_entity_types(sql_generator_expr: Generator) -> List[Type]:
    if not inspect.isgenerator(sql_generator_expr):
        raise TypeError(
            f"expected a SQL generator expression but got: {type(sql_generator_expr)}"
        )

    entity = sql_generator_expr.gi_frame.f_locals[".0"]
    if not isinstance(entity, EntityProxy):
        raise TypeError("invalid SQL generator expression")

    return entity.types


@functools.lru_cache
def _analyze_expression(sql_generator_expr: Generator) -> CodeExpression:
    code_analyzer = CodeExpressionAnalyzer(sql_generator_expr)
    try:
        return code_analyzer.get_expression()
    except AttributeError as e:
        path = sql_generator_expr.gi_frame.f_code.co_filename
        lineno = sql_generator_expr.gi_frame.f_code.co_firstlineno
        raise RuntimeError(
            f'error parsing expression in file "{path}", line {lineno}'
        ) from e


def _query_builder_args(sql_generator_expr: Generator) -> QueryBuilderArgs:
    if not inspect.isgenerator(sql_generator_expr):
        raise TypeError(
            f"expected a SQL generator expression but got: {type(sql_generator_expr)}"
        )

    # obtain AST representation of generator expression
    code_expression = _analyze_expression(sql_generator_expr)

    # get reference to caller's frame
    caller = sys._getframe(2)
    closure_vars = caller.f_locals

    # build query context
    context = Context(code_expression.local_vars, closure_vars)
    source_arg = sql_generator_expr.gi_frame.f_locals[".0"]

    # build SQL query
    return QueryBuilderArgs(
        source_arg,
        context,
        code_expression.conditional_expr,
        code_expression.yield_expr,
    )


def select(sql_generator_expr: Generator) -> Query:
    qba = _query_builder_args(sql_generator_expr)
    builder = QueryBuilder()
    sql = builder.select(qba)
    return Query(sql)


def insert_or_select(insert_obj: T, sql_generator_expr: Generator) -> Query:
    qba = _query_builder_args(sql_generator_expr)
    builder = QueryBuilder()
    sql = builder.insert_or_select(qba, insert_obj)
    return Query(sql)
