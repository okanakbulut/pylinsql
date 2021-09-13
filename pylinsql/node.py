"""
Abstract syntax node in a tree synthesized from a Python Boolean lambda expression or the conditional part of
a generator expression.

This module is used internally.
"""

from __future__ import annotations

from typing import List

from .ast import Expression


class AbstractNode:
    "An abstract node in the control flow graph."

    # symbolic expression that constitutes the target condition
    expr: Expression

    # target node if condition evaluates to true (green edge)
    on_true: AbstractNode = None
    # target node if condition evaluates to false (red edge)
    on_false: AbstractNode = None
    # nodes with outgoing edges (true or false) pointing to this node
    origins: List[AbstractNode]

    def __init__(self, expr=None):
        self.expr = expr
        self.origins = []

    def __repr__(self) -> str:
        return f"{__class__.__name__}({self.expr})"

    def print(self, indent=0) -> None:
        if self.expr is not None:
            expr = self.expr
        else:
            expr = "<empty>"
        print(" " * indent, expr, sep="")
        indent += 4
        if self.on_true is not None:
            self.on_true.print(indent)
        if self.on_false is not None and self.on_true is not self.on_false:
            self.on_false.print(indent)

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

    def set_target(self, true_node: AbstractNode, false_node: AbstractNode) -> None:
        "Binds outgoing edges of a node."

        self.set_on_true(true_node)
        self.set_on_false(false_node)

    def set_on_true(self, node: AbstractNode) -> None:
        "Binds the true (green) edge of the node."

        if self.on_true is not None:
            self.on_true.origins.remove(self)
        self.on_true = node
        if node is not None:
            node.origins.append(self)

    def set_on_false(self, node: AbstractNode) -> None:
        "Binds the false (red) edge of the node."

        if self.on_false is not None:
            self.on_false.origins.remove(self)
        self.on_false = node
        if node is not None:
            node.origins.append(self)

    def seize_origins(self, node: AbstractNode) -> None:
        """
        Captures all incoming edges of another node such that all edges that were previously entering that node
        are now targeting this node.
        """

        for origin in node.origins:
            if origin.on_true is node:
                origin.set_on_true(self)
            elif origin.on_false is node:
                origin.set_on_false(self)

    def twist(self) -> None:
        "Swaps true (green) and false (red) edges with each another."

        if self.expr is not None:
            self.expr = self.expr.negate()
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


def is_expr_condition(nodes: List[AbstractNode]) -> bool:
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

    if len(nodes[0].origins) > 1:
        return False
    for node in nodes[1:]:
        for origin in node.origins:
            if origin not in nodes:
                return False

    return True


def is_loop_condition(nodes: List[AbstractNode]) -> bool:
    target_true = None
    target_false = None
    for node in nodes:
        # check if set has a single output
        if node.on_true is not None and node.on_true not in nodes:
            if target_true is None:
                target_true = node.on_true
            elif node.on_true is not target_true:
                return False
        if node.on_false is not None and node.on_false not in nodes:
            if target_false is None:
                target_false = node.on_false
            elif node.on_false is not target_false:
                return False

    if len(nodes[0].origins) > 1:
        return False
    for node in nodes[1:]:
        for origin in node.origins:
            if origin not in nodes:
                return False

    return True