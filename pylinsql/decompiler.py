"""
Reconstruct the original Python code from a Python generator expression.

This module is used internally.
"""

from __future__ import annotations

import dis
import inspect
import itertools
from dataclasses import dataclass
from types import CodeType
from typing import Any, Callable, Generator, List, Tuple, Union

from .ast import *
from .base import is_lambda


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


@dataclass(frozen=True)
class _IteratorValue:
    "Placeholder for iterator value pushed to the stack by the FOR_ITER instruction."

    pass


class _Disassembler:
    codeobject: CodeType
    stack: List[Expression]
    variables: List[str]
    expr: Expression

    # instruction index on jump condition True
    on_true: int
    # instruction index on jump condition False
    on_false: int

    def __init__(self, codeobject):
        self.codeobject = codeobject
        self.stack = []
        self.variables = []

    def process_block(self, instructions: List[dis.Instruction]):
        "Process a single basic block, ending with a (conditional) jump."

        self.expr = None
        self.on_true = None
        self.on_false = None

        for instruction in instructions:
            fn = getattr(self, instruction.opname)
            fn(instruction.arg)

        return self.expr, self.on_true, self.on_false

    def LOAD_ATTR(self, name_index):
        base = self.stack.pop()
        self.stack.append(AttributeAccess(base, self.codeobject.co_names[name_index]))

    def LOAD_CONST(self, const_index):
        self.stack.append(Constant(self.codeobject.co_consts[const_index]))

    def LOAD_FAST(self, var_num):
        self.stack.append(LocalRef(self.codeobject.co_varnames[var_num]))

    def LOAD_GLOBAL(self, name_index):
        self.stack.append(GlobalRef(self.codeobject.co_names[name_index]))

    def LOAD_DEREF(self, i):
        if i < len(self.codeobject.co_cellvars):
            name = self.codeobject.co_cellvars[i]
        else:
            name = self.codeobject.co_freevars[i - len(self.codeobject.co_cellvars)]
        self.stack.append(ClosureRef(name))

    def STORE_FAST(self, var_num):
        self.stack.pop()
        self.variables.append(self.codeobject.co_varnames[var_num])

    def CALL_FUNCTION(self, argc):
        args = []
        for _ in range(argc):
            args.append(self.stack.pop())
        args.reverse()
        func = self.stack.pop()
        self.stack.append(FunctionCall(func, args))

    def _unary_op(self, cls):
        expr = self.stack.pop()
        self.stack.append(cls(expr))

    UNARY_POSITIVE = lambda self, _: self._unary_op(UnaryPlus)
    UNARY_NEGATIVE = lambda self, _: self._unary_op(UnaryMinus)
    UNARY_INVERT = lambda self, _: self._unary_op(BitwiseNot)

    def _binary_op(self, cls):
        right = self.stack.pop()
        left = self.stack.pop()
        self.stack.append(cls(left, right))

    BINARY_POWER = lambda self, _: self._binary_op(Exponentiation)
    BINARY_MULTIPLY = lambda self, _: self._binary_op(Multiplication)
    BINARY_TRUE_DIVIDE = lambda self, _: self._binary_op(Division)
    BINARY_ADD = lambda self, _: self._binary_op(Addition)
    BINARY_SUBTRACT = lambda self, _: self._binary_op(Subtraction)
    BINARY_LSHIFT = lambda self, _: self._binary_op(BitwiseLeftShift)
    BINARY_RSHIFT = lambda self, _: self._binary_op(BitwiseRightShift)
    BINARY_AND = lambda self, _: self._binary_op(BitwiseAnd)
    BINARY_XOR = lambda self, _: self._binary_op(BitwiseXor)
    BINARY_OR = lambda self, _: self._binary_op(BitwiseOr)

    def _compare_op(self, op: str, invert: bool):
        right = self.stack.pop()
        left = self.stack.pop()
        comp = Comparison(op, left, right)
        if invert:
            self.stack.append(comp.negate())
        else:
            self.stack.append(comp)

    COMPARE_OP = lambda self, opname: self._compare_op(dis.cmp_op[opname], False)

    # new in version 3.9
    CONTAINS_OP = lambda self, invert: self._compare_op("in", invert)
    IS_OP = lambda self, invert: self._compare_op("is", invert)

    def JUMP_ABSOLUTE(self, target):
        pass

    def JUMP_IF_FALSE_OR_POP(self, target):
        raise NotImplementedError()

    def JUMP_IF_TRUE_OR_POP(self, target):
        raise NotImplementedError()

    def POP_JUMP_IF_FALSE(self, target):
        self.expr = self.stack.pop()
        self.on_true = None
        self.on_false = target

    def POP_JUMP_IF_TRUE(self, target):
        self.expr = self.stack.pop()
        self.on_true = target
        self.on_false = None

    def POP_TOP(self, _):
        self.stack.pop()

    def FOR_ITER(self, _):
        self.stack.append(_IteratorValue())

    def _sequence_op(self, count, cls):
        values = []
        for _ in range(count):
            values.append(self.stack.pop())
        values.reverse()
        self.stack.append(cls(values))

    BUILD_TUPLE = lambda self, count: self._sequence_op(count, TupleExpression)
    BUILD_LIST = lambda self, count: self._sequence_op(count, ListExpression)

    def UNPACK_SEQUENCE(self, count):
        value = self.stack.pop()
        for index in range(count - 1, -1, -1):
            self.stack.append(IndexAccess(value, index))

    def RETURN_VALUE(self, _):
        self.expr = self.stack.pop()

    def YIELD_VALUE(self, _):
        self.expr = self.stack.pop()


@dataclass
class _BasicBlock:
    "An interval that contains instructions and ends with a (conditional) jump instruction."

    start_offset: int = None
    start_index: int = None
    end_index: int = None


@dataclass
class CodeExpression:
    local_vars: List[str]
    conditional_expr: Expression
    yield_expr: Expression


class CodeExpressionAnalyzer:
    code_object: CodeType
    instructions: List[dis.Instruction]

    def __init__(self, code_object: CodeType):
        self.code_object = code_object
        self.instructions = list(dis.Bytecode(self.code_object))

    @staticmethod
    def _is_jump_instruction(instruction: dis.Instruction) -> bool:
        "True if the Python instruction involves a jump with a target, e.g. JUMP_ABSOLUTE or POP_JUMP_IF_TRUE."

        return "FOR_ITER" == instruction.opname or "JUMP" in instruction.opname

    def _get_basic_blocks(self) -> List[_BasicBlock]:
        blocks = []
        block = _BasicBlock(start_index=0, start_offset=0)
        for prev_index, (prev_instr, next_instr) in enumerate(
            pairwise(self.instructions)
        ):
            next_index = prev_index + 1
            if next_instr.is_jump_target or self._is_jump_instruction(prev_instr):
                # terminate block before jump target and after jump instruction
                block.end_index = next_index
                blocks.append(block)

                # start new block
                block = _BasicBlock(
                    start_offset=next_instr.offset, start_index=next_index
                )
        block.end_index = len(self.instructions)
        blocks.append(block)
        return blocks

    def _get_abstract_nodes(self) -> Tuple[List[str], List[_AbstractNode]]:
        blocks = self._get_basic_blocks()
        disassembler = _Disassembler(self.code_object)

        node_by_offset = {}
        for index, block in enumerate(blocks):
            expr, on_true, on_false = disassembler.process_block(
                self.instructions[block.start_index : block.end_index]
            )
            if index + 1 < len(blocks):
                # automatically fall through to subsequent block
                if on_true is None:
                    on_true = blocks[index + 1].start_offset
                if on_false is None:
                    on_false = blocks[index + 1].start_offset
            node_by_offset[block.start_offset] = (
                _AbstractNode(expr),
                on_true,
                on_false,
            )

        assert not disassembler.stack

        for node, on_true, on_false in node_by_offset.values():
            on_true_node = node_by_offset[on_true][0] if on_true else None
            on_false_node = node_by_offset[on_false][0] if on_false else None
            node.set_target(on_true_node, on_false_node)

        nodes = [item[0] for item in node_by_offset.values()]

        # remove prolog and epilog from generator
        # prolog pushes the single iterable argument (a.k.a. ".0") to the stack that a generator expression receives:
        #       0 LOAD_FAST                0 (.0)
        # epilog pops the stack and returns None to indicate end of iteration
        # >>   48 LOAD_CONST               4 (None)
        #      50 RETURN_VALUE
        nodes = nodes[1:-1]

        return disassembler.variables, nodes

    def _get_condition(self, nodes: List[_AbstractNode]) -> Expression:
        # extract conditional part from generator
        true_node = _AbstractNode(Constant(True))
        false_node = _AbstractNode(Constant(False))

        # identify loop structural elements
        # loop head (a single block):
        # >>    2 FOR_ITER                18 (to 22)
        # loop body (a single block that yields values):
        #      ...
        #      14 BUILD_TUPLE              2
        #      16 YIELD_VALUE
        #      18 POP_TOP
        #      20 JUMP_ABSOLUTE            2
        # conditional nodes (several interconnected blocks that jump on conditions)
        loop_head = nodes[0]
        loop_body = nodes[-1]
        cond_nodes = nodes[1:-1]

        # redirect result statement nodes to boolean result nodes
        for node in cond_nodes:
            if node.on_true is loop_body:
                node.set_on_true(true_node)
            elif node.on_true is loop_head:
                node.set_on_true(false_node)
            if node.on_false is loop_body:
                node.set_on_false(true_node)
            elif node.on_false is loop_head:
                node.set_on_false(false_node)

        loop_head.set_target(None, None)
        cond_nodes.append(true_node)
        cond_nodes.append(false_node)
        root = cond_nodes[0]

        # align DAG edge colors such that all incoming edges are either green (true) edges or red (false) edges
        marked = set()
        for node in cond_nodes:
            edge_type = False
            for origin in node.origins:
                if origin in marked:
                    edge_type = origin.on_true is node
                    break
            for origin in node.origins:
                if edge_type != (origin.on_true is node):
                    origin.twist()
                    marked.add(origin)
        assert all(node.is_origin_consistent() for node in cond_nodes)

        # ensure that True/False result nodes are consistent with their inputs
        if not all(origin.on_true is true_node for origin in true_node.origins):
            # fix inconsistency by flipping all nodes
            for node in cond_nodes:
                node.twist()
        assert all(node.is_origin_consistent() for node in cond_nodes)

        nodes = root.topological_sort()
        nodes.remove(true_node)
        nodes.remove(false_node)

        while len(nodes) > 1:
            for start in range(len(nodes) - 1):
                # locate a span of abstract nodes that could form a single conjunction
                end = start + 1
                while end < len(nodes) and nodes[start].on_false is nodes[end].on_false:
                    end += 1

                if end - start > 1:
                    conj_expr = Conjunction([n.expr for n in nodes[start:end]])
                    conj_node = _AbstractNode(conj_expr)
                    conj_node.set_target(nodes[end - 1].on_true, nodes[start].on_false)
                    conj_node.seize_origins(nodes[start])
                    for n in nodes[start:end]:
                        n.set_target(None, None)
                    nodes[start:end] = [conj_node]
                    break

                # locate a span of abstract nodes that could form a single disjunction
                end = start + 1
                while end < len(nodes) and nodes[start].on_true is nodes[end].on_true:
                    end += 1

                if end - start > 1:
                    disj_expr = Disjunction([n.expr for n in nodes[start:end]])
                    disj_node = _AbstractNode(disj_expr)
                    disj_node.set_target(nodes[start].on_true, nodes[end - 1].on_false)
                    disj_node.seize_origins(nodes[start])
                    for n in nodes[start:end]:
                        n.set_target(None, None)
                    nodes[start:end] = [disj_node]
                    break

        return nodes[0].expr

    def get_expression(self) -> CodeExpression:
        variables, nodes = self._get_abstract_nodes()

        cond_expr = self._get_condition(nodes) if len(nodes) > 2 else None
        yield_expr = nodes[-1].expr

        return CodeExpression(variables, cond_expr, yield_expr)

    def _show_blocks(self, blocks):
        for block in blocks:
            for instr in self.instructions[block.start_index : block.end_index]:
                print(instr.opname)
            print("----")


class _AbstractNode:
    "An abstract node in the control flow graph."

    expr: Expression
    on_true: _AbstractNode = None
    on_false: _AbstractNode = None
    origins: List[_AbstractNode]

    def __init__(self, expr):
        self.expr = expr
        self.origins = []

    def print(self, indent=0):
        print(" " * indent, self.expr, sep="")
        indent += 4
        if self.on_true:
            self.on_true.print(indent)
        if self.on_false:
            self.on_false.print(indent)

    def is_origin_consistent(self):
        "Checks if all incoming edges are exclusively true (green) or exclusively false (red)."

        origins_true = all(self == caller.on_true for caller in self.origins)
        origins_false = all(self == caller.on_false for caller in self.origins)
        return origins_true or origins_false

    def set_target(self, true_node: _AbstractNode, false_node: _AbstractNode):
        "Binds outgoing edges of a node."

        self.set_on_true(true_node)
        self.set_on_false(false_node)

    def set_on_true(self, node: _AbstractNode):
        "Binds the true (green) edge of the node."

        if self.on_true:
            self.on_true.origins.remove(self)
        self.on_true = node
        if node:
            node.origins.append(self)

    def set_on_false(self, node: _AbstractNode):
        "Binds the false (red) edge of the node."

        if self.on_false:
            self.on_false.origins.remove(self)
        self.on_false = node
        if node:
            node.origins.append(self)

    def seize_origins(self, node: _AbstractNode):
        """
        Captures all incoming edges of another node such that all edges that were previously entering that node
        are now targeting this node.
        """

        for origin in node.origins:
            if origin.on_true is node:
                origin.set_on_true(self)
            elif origin.on_false is node:
                origin.set_on_false(self)

    def twist(self):
        "Swaps true (green) and false (red) edges with each another."

        self.expr = self.expr.negate()
        self.on_false, self.on_true = self.on_true, self.on_false

    def topological_sort(self):
        "Produces a topological sort of all descendant nodes starting from this node."

        result = []
        seen = set()

        def recursive_helper(node):
            if node.on_true and node.on_true not in seen:
                seen.add(node.on_true)
                recursive_helper(node.on_true)
            if node.on_false and node.on_false not in seen:
                seen.add(node.on_false)
                recursive_helper(node.on_false)
            result.append(node)

        recursive_helper(self)
        result.reverse()
        return result
