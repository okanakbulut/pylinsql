"""
Abstract syntax node in a tree synthesized from a Python Boolean lambda expression or the conditional part of
a generator expression.

This module is used internally.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from dis import Instruction
from typing import Any, ClassVar, List, Optional

from .ast import Conjunction, Disjunction, Expression, Stack
from .evaluator import Evaluator


class AbstractNode:
    "An abstract node in the control flow graph."

    # symbolic expression that constitutes the target condition
    expr: Expression

    inverted: bool = False

    # target node if condition evaluates to true (green edge)
    on_true: AbstractNode = None
    # target node if condition evaluates to false (red edge)
    on_false: AbstractNode = None
    # nodes with outgoing edges (true or false) pointing to this node
    origins: List[AbstractNode]

    def __init__(self, expr):
        self.expr = expr
        self.origins = []

    def __repr__(self) -> str:
        return f"{__class__.__name__}({self.expr}, {self.inverted})"

    def is_origin_consistent(self) -> bool:
        "Checks if all incoming edges are exclusively true (green) or exclusively false (red)."

        origins_true = all(self == caller.on_true for caller in self.origins)
        origins_false = all(self == caller.on_false for caller in self.origins)
        return origins_true or origins_false

    def get_origin_true(self) -> List[AbstractNode]:
        "Returns all originating nodes whose true (green) edge is incoming to this node."

        return [origin for origin in self.origins if origin.on_true is self]

    def get_origin_false(self) -> List[AbstractNode]:
        "Returns all originating nodes whose false (red) edge is incoming to this node."

        return [origin for origin in self.origins if origin.on_false is self]

    def get_roots(self) -> List[AbstractNode]:

        result = []

        def recursive_helper(node):
            if not node.origins:
                if node not in result:
                    result.append(node)
            else:
                for origin in node.origins:
                    recursive_helper(origin)

        recursive_helper(self)
        return result

    def remove_origins(self, nodes: Optional[List[AbstractNode]] = None) -> None:
        "Deletes all edges incoming to this node from a given set of nodes."

        if nodes is not None:
            for origin in self.get_origin_true():
                if origin in nodes:
                    origin.set_on_true(None)
            for origin in self.get_origin_false():
                if origin in nodes:
                    origin.set_on_false(None)
        else:
            for origin in self.get_origin_true():
                origin.set_on_true(None)
            for origin in self.get_origin_false():
                origin.set_on_false(None)

    def set_target(
        self, true_node: Optional[AbstractNode], false_node: Optional[AbstractNode]
    ) -> None:
        "Binds outgoing edges of a node."

        self.set_on_true(true_node)
        self.set_on_false(false_node)

    def set_on_true(self, node: Optional[AbstractNode]) -> None:
        "Binds the true (green) edge of the node."

        if self.on_true is not None:
            self.on_true.origins.remove(self)
        self.on_true = node
        if node is not None:
            node.origins.append(self)

    def set_on_false(self, node: Optional[AbstractNode]) -> None:
        "Binds the false (red) edge of the node."

        if self.on_false is not None:
            self.on_false.origins.remove(self)
        self.on_false = node
        if node is not None:
            node.origins.append(self)

    def redirect(self, source: AbstractNode, target: AbstractNode) -> None:
        "Redirect edges targeting the given node to another node."

        if self.on_true is source:
            self.set_on_true(target)
        if self.on_false is source:
            self.set_on_false(target)

    def redirect_origins(self, target: AbstractNode) -> None:
        "Redirect edges targeting this node to another node."

        for origin in self.origins:
            origin.redirect(self, target)

    def seize_origins(self, node: AbstractNode) -> None:
        """
        Captures all incoming edges of another node such that all edges that were previously entering that node
        are now targeting this node.
        """

        for origin in node.origins:
            origin.redirect(node, self)

    def twist(self) -> None:
        "Swaps true (green) and false (red) edges with each another."

        self.inverted = not self.inverted
        self.on_false, self.on_true = self.on_true, self.on_false

    def topological_sort(self) -> List[AbstractNode]:
        "Produces a topological sort of all descendant nodes starting from this node."

        result = []
        seen = set()

        def recursive_helper(node):
            if node.on_true is not None and node.on_true not in seen:
                seen.add(node.on_true)
                recursive_helper(node.on_true)
            if node.on_false is not None and node.on_false not in seen:
                seen.add(node.on_false)
                recursive_helper(node.on_false)
            result.append(node)

        recursive_helper(self)
        result.reverse()
        return result

    def traverse_top_down(self) -> List[AbstractNode]:
        "Produces a depth-first traversal of nodes starting from this node, choosing true (green) edges first."

        result = []

        def recursive_helper(node):
            result.append(node)
            if node.on_true is not None and node.on_true not in result:
                recursive_helper(node.on_true)
            if node.on_false is not None and node.on_false not in result:
                recursive_helper(node.on_false)

        recursive_helper(self)
        return result

    def traverse_bottom_up(self) -> List[AbstractNode]:
        "Produces a depth-first traversal of nodes starting from this node and following origins."

        result = []

        def recursive_helper(node):
            result.append(node)
            for origin in self.origins:
                if origin not in result:
                    recursive_helper(origin)

        recursive_helper(self)
        return result


@dataclass(frozen=True)
class NodeInstructions:
    "Instructions encapsulated by a simple node."

    instructions: List[Instruction]

    def __repr__(self) -> str:
        addr = self.instructions[0].offset
        head = self.instructions[0].opname
        if len(self.instructions) > 1:
            last = self.instructions[-1].opname
            return f"'{addr} {head}..{last}'"
        else:
            return f"'{addr} {head}'"


@dataclass(frozen=True)
class NodeBooleanExpression:
    "A Boolean expression that a composite node represents."

    nodes: List[AbstractNode]

    @classmethod
    def expression(cls, exprs: List[Expression]) -> Expression:
        ...

    flag: ClassVar[bool] = None


@dataclass(frozen=True)
class NodeConjunction(NodeBooleanExpression):
    @classmethod
    def expression(cls, exprs: List[Expression]) -> Expression:
        return Conjunction(exprs)

    flag: ClassVar[bool] = True


@dataclass(frozen=True)
class NodeDisjunction(NodeBooleanExpression):
    @classmethod
    def expression(cls, exprs: List[Expression]) -> Expression:
        return Disjunction(exprs)

    flag: ClassVar[bool] = False


@dataclass(frozen=True)
class NodeSequence:
    nodes: List[AbstractNode]


def print_nodes(nodes: List[AbstractNode]) -> None:
    for node in nodes:
        print(node)
        print(f"  true:  {node.on_true}")
        print(f"  false: {node.on_false}")


class ConditionExpressionChecker:
    "Verifies if a set of nodes corresponds to a stand-alone conditional expression (e.g. as a function call argument)."

    head_node: AbstractNode
    target_node: AbstractNode

    def matches(self, nodes: List[AbstractNode]) -> bool:
        if not nodes:
            return False

        # head node intercepts all incoming edges
        head_node = None
        for node in nodes:
            for origin in node.origins:
                if origin not in nodes:
                    head_node = node
                    break

        # condition is terminated at a single target node
        target_node = None
        for node in nodes:
            # check if set has a single output
            if node.on_true is not None and node.on_true not in nodes:
                if target_node is None:
                    target_node = node.on_true
                elif node.on_true is not target_node:
                    return False
            if node.on_false is not None and node.on_false not in nodes:
                if target_node is None:
                    target_node = node.on_false
                elif node.on_false is not target_node:
                    return False

        # check if all edges to the target node originate from the set
        for origin in target_node.origins:
            if origin not in nodes:
                return False

        # check if inner nodes have no external origins
        for node in nodes:
            if node is not head_node:
                for origin in node.origins:
                    if origin not in nodes:
                        return False

        self.head_node = head_node
        self.target_node = target_node
        return True


class LoopConditionChecker:
    "Verifies if a set of nodes corresponds to a loop statement."

    # iterator statement at the head of the loop
    iterator_node: AbstractNode
    # statements in the body of the loop
    body_node: AbstractNode
    # node immediately following the loop
    exit_node: AbstractNode

    def matches(self, nodes: List[AbstractNode]) -> bool:
        "True if the set of nodes corresponds to a loop statement, including initializer, condition and body."

        if not nodes:
            return False

        # iterator statement
        iter_node = None
        for node in nodes:
            if node.on_true in nodes and node.on_false not in nodes:
                iter_node = node
                break

        # loop body
        body_node = None
        for node in nodes:
            # unconditional jump to head at the end of loop body
            if node.on_true is node.on_false and node.on_true is iter_node:
                body_node = node
                break

        # check if nodes have no incoming edges from outside the loop (except iterator)
        for node in nodes:
            if node is iter_node:
                continue

            # jump to outside the loop
            if node.on_true not in nodes:
                return False
            if node.on_false not in nodes:
                return False

            # jump from outside of the loop
            for origin in node.origins:
                if origin not in nodes:
                    return False

        self.iterator_node = iter_node
        self.body_node = body_node
        self.exit_node = iter_node.on_false
        return True


class NodeVisitor:
    """
    Evaluates a graph of simple and composite nodes.

    A simple node corresponds to a basic block, and encapsulates a series of instructions, usually terminated
    with a jump instruction.

    A composite node is formed by merging other nodes with conjunction, disjunction or sequence semantics.
    """

    evaluator: Evaluator

    # expression that is produced by a YIELD_VALUE instruction
    yield_expr: Optional[Expression]
    # expression that is returned by a RETURN_VALUE instruction
    return_expr: Optional[Expression]

    stack: Stack

    def __init__(self, evaluator) -> None:
        self.evaluator = evaluator
        self.yield_expr = None
        self.return_expr = None

        # stack is empty for function block entry point
        self.stack = []

    def visit(self, node: AbstractNode, jump_cond: bool = True) -> Expression:
        expr = self._visit(node.expr, jump_cond)
        if expr is not None and node.inverted:
            return expr.negate()
        else:
            return expr

    @functools.singledispatchmethod
    def _visit(self, expr: Any, jump_cond: bool) -> Expression:
        raise NotImplementedError(f"unrecognized node expression type: {type(expr)}")

    @_visit.register
    def _(self, block: NodeInstructions, jump_cond: bool) -> Expression:
        result = self.evaluator.process_block(block.instructions, self.stack, jump_cond)

        self.stack = result.stack
        if result.yield_expr:
            if self.yield_expr is None:
                self.yield_expr = result.yield_expr
            elif self.yield_expr != result.yield_expr:
                raise NotImplementedError(
                    "multiple 'yield' statements in an execution graph"
                )
        if result.return_expr:
            if self.return_expr is None:
                self.return_expr = result.return_expr
            elif self.return_expr != result.return_expr:
                raise NotImplementedError(
                    "multiple 'return' statements in an execution graph"
                )

        return result.jump_expr

    def _visit_boolean(self, boolean: NodeBooleanExpression) -> Expression:
        exprs = [self.visit(node, boolean.flag) for node in boolean.nodes[:-1]]

        # check last member of Boolean expression list to determine behavior
        expr = self.visit(boolean.nodes[-1], not boolean.flag)
        if expr is None:
            # stand-alone Boolean expression is part of a compound expression
            # e.g. expression is used as a function argument
            # merge last value from top of the stack with list of expressions gathered so far
            expr = self.stack.pop()
            exprs.append(expr)
            self.stack.append(boolean.expression(exprs))
            return None
        else:
            # Boolean expression is part of a condition statement
            # execution branches to true or false blocks based on its value
            exprs.append(expr)
            return boolean.expression(exprs)

    @_visit.register
    def _(self, conj: NodeConjunction, _: bool) -> Expression:
        return self._visit_boolean(conj)

    @_visit.register
    def _(self, disj: NodeDisjunction, _: bool) -> Expression:
        return self._visit_boolean(disj)

    @_visit.register
    def _(self, seq: NodeSequence, jump_cond: bool) -> Expression:
        result = None

        for node in seq.nodes:
            expr = self.visit(node, jump_cond)
            if result is None:
                result = expr
            elif expr is None:
                pass
            else:
                raise NotImplementedError(
                    "multiple result expressions in a single node sequence"
                )

        return result
