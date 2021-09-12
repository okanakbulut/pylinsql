"""
Reconstruct the original Python code from a Python generator expression.

This module is used internally.
"""

import dis
import itertools
from dataclasses import dataclass, field
from types import CodeType
from typing import Any, Iterable, List, Tuple

from .ast import *
from .node import AbstractNode


def all_equal(iterable: Iterable[T]) -> bool:
    iterator = iter(iterable)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == x for x in iterator)


def pairwise(iterable: Iterable[T]) -> Iterable[Tuple[T, T]]:
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


Stack = List[Expression]


@dataclass(frozen=True)
class _IteratorValue:
    "Placeholder for iterator value pushed to the stack by the FOR_ITER instruction."

    pass


class JumpResolver:
    _next: int

    def process(self, instr: dis.Instruction) -> Tuple[int, int]:
        self._next = instr.offset + 2  # automatically fall through to subsequent block
        fn = getattr(self, instr.opname, None)

        if fn is not None:
            return fn(instr.arg)
        else:
            return self._next, self._next

    def test(self, instr: dis.Instruction) -> bool:
        "True if the Python instruction involves a jump with a target, e.g. JUMP_ABSOLUTE or POP_JUMP_IF_TRUE."

        return getattr(self, instr.opname, None) is not None

    def JUMP_ABSOLUTE(self, target):
        return target, target

    def JUMP_FORWARD(self, delta):
        target = self._next + delta
        return target, target

    def JUMP_IF_FALSE_OR_POP(self, target):
        return self._next, target

    def JUMP_IF_TRUE_OR_POP(self, target):
        return target, self._next

    def POP_JUMP_IF_FALSE(self, target):
        return self._next, target

    def POP_JUMP_IF_TRUE(self, target):
        return target, self._next

    def FOR_ITER(self, delta):
        return self._next, self._next + delta

    def RETURN_VALUE(self, _):
        return None, None


@dataclass
class _BasicBlockExpr:
    on_true: Stack
    on_false: Stack
    # expression on which the final jump instruction in the block is evaluated
    jump_expr: Optional[Expression]
    # expression that is produced by a YIELD_VALUE instruction
    yield_expr: Optional[Expression]
    # expression that is returned by a RETURN_VALUE instruction
    return_expr: Optional[Expression]


class _Disassembler:
    codeobject: CodeType
    stack: Stack
    variables: List[str]

    _on_true: Stack
    _on_false: Stack
    _jump_expr: Expression
    _yield_expr: Expression
    _return_expr: Expression

    def __init__(self, codeobject):
        self.codeobject = codeobject
        self.stack = []
        self.variables = []

    def _reset(self):
        self._on_true = None
        self._on_false = None
        self._jump_expr = None
        self._yield_expr = None
        self._return_expr = None

    def process_block(
        self, instructions: List[dis.Instruction], stack: Stack
    ) -> _BasicBlockExpr:
        "Process a single basic block, ending with a (conditional) jump."

        self.stack = stack.copy()
        self._reset()
        for instruction in instructions:
            fn = getattr(self, instruction.opname)
            fn(instruction.arg)

        # handle fall-through from this block to the following block
        if self._on_true is None:
            self._on_true = self.stack.copy()
        if self._on_false is None:
            self._on_false = self.stack.copy()

        result = _BasicBlockExpr(
            self._on_true,
            self._on_false,
            self._jump_expr,
            self._yield_expr,
            self._return_expr,
        )
        self._reset()
        return result

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

    def _pop_func_args(self, argc: int) -> List[Expression]:
        args = []
        for _ in range(argc):
            args.append(self.stack.pop())
        args.reverse()
        return args

    def CALL_FUNCTION(self, argc):
        args = self._pop_func_args(argc)
        func = self.stack.pop()
        self.stack.append(FunctionCall(func, args))

    def CALL_FUNCTION_KW(self, argc):
        # keyword arguments with keyword names supplied in a tuple
        const: Constant = self.stack.pop()
        if not isinstance(const.value, tuple):
            raise RuntimeError("keyword argument names must be supplied in a tuple")
        names: Tuple[str, ...] = const.value
        values = self._pop_func_args(len(names))
        kwargs = {name: value for name, value in zip(names, values)}

        # positional arguments in reverse order
        pargs = []
        for _ in range(argc - len(names)):
            pargs.append(self.stack.pop())
        pargs.reverse()

        func = self.stack.pop()
        self.stack.append(FunctionCall(func, pargs, kwargs))

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

    def JUMP_FORWARD(self, delta):
        pass

    def JUMP_IF_FALSE_OR_POP(self, target):
        self._on_false = self.stack.copy()
        self._jump_expr = self.stack.pop()
        self._on_true = self.stack.copy()

    def JUMP_IF_TRUE_OR_POP(self, target):
        self._on_true = self.stack.copy()
        self._jump_expr = self.stack.pop()
        self._on_false = self.stack.copy()

    def _pop_jump(self):
        self._jump_expr = self.stack.pop()
        self._on_true = self.stack.copy()
        self._on_false = self.stack.copy()

    POP_JUMP_IF_FALSE = lambda self, target: self._pop_jump()
    POP_JUMP_IF_TRUE = lambda self, target: self._pop_jump()

    def DUP_TOP(self, _):
        self.stack.append(self.stack[-1])

    def DUP_TOP_TWO(self, _):
        item1 = self.stack[-1]
        item2 = self.stack[-2]
        self.stack.extend([item2, item1])

    def POP_TOP(self, _):
        self.stack.pop()

    def ROT_TWO(self, _):
        self.stack[-1], self.stack[-2] = self.stack[-2], self.stack[-1]

    def ROT_THREE(self, _):
        self.stack[-1], self.stack[-2], self.stack[-3] = (
            self.stack[-2],
            self.stack[-3],
            self.stack[-1],
        )

    def ROT_FOUR(self, _):
        self.stack[-1], self.stack[-2], self.stack[-3], self.stack[-4] = (
            self.stack[-2],
            self.stack[-3],
            self.stack[-4],
            self.stack[-1],
        )

    def FOR_ITER(self, _):
        self._on_false = self.stack.copy()
        self.stack.append(_IteratorValue())
        self._on_true = self.stack.copy()

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
        self._return_expr = self.stack.pop()

    def YIELD_VALUE(self, _):
        self._yield_expr = self.stack.pop()


@dataclass
class _BasicBlockSpan:
    "An interval that contains instructions and ends with a (conditional) jump instruction."

    start_offset: int = None
    start_index: int = None
    end_index: int = None


def _get_basic_blocks(instructions: List[dis.Instruction]) -> List[_BasicBlockSpan]:
    jmp = JumpResolver()

    blocks = []
    block = _BasicBlockSpan(start_index=0, start_offset=0)
    for prev_index, (prev_instr, next_instr) in enumerate(pairwise(instructions)):
        next_index = prev_index + 1
        if next_instr.is_jump_target or jmp.test(prev_instr):
            # terminate block before jump target and after jump instruction
            block.end_index = next_index
            blocks.append(block)

            # start new block
            block = _BasicBlockSpan(
                start_offset=next_instr.offset, start_index=next_index
            )
    block.end_index = len(instructions)
    blocks.append(block)
    return blocks


@dataclass
class _NodeOutputs:
    # stack passed on to the following block when condition is true
    on_true: Stack = None
    # stack passed on to the following block when condition is false
    on_false: Stack = None


@dataclass
class CodeExpression:
    local_vars: List[str]
    conditional_expr: Expression
    yield_expr: Expression


class CodeExpressionAnalyzer:
    code_object: CodeType
    instructions: List[dis.Instruction]

    _yield_expr: Expression

    def __init__(self, code_object: CodeType):
        self.code_object = code_object
        self.instructions = list(dis.Bytecode(self.code_object))
        self._yield_expr = None

    def _get_abstract_nodes(self, blocks: List[_BasicBlockSpan]) -> List[AbstractNode]:

        jmp = JumpResolver()
        nodes: List[AbstractNode] = []
        node_by_offset: Dict[Tuple[int, int]] = {}
        for block in blocks:
            node = AbstractNode()
            nodes.append(node)
            on_true, on_false = jmp.process(
                self.instructions[block.start_index : block.end_index][-1],
            )
            node_by_offset[block.start_offset] = (
                node,
                on_true,
                on_false,
            )

        for node, on_true, on_false in node_by_offset.values():
            node.set_target(
                node_by_offset[on_true][0] if on_true is not None else None,
                node_by_offset[on_false][0] if on_false is not None else None,
            )

        return nodes

    def _disassemble(
        self, blocks: List[_BasicBlockSpan], nodes: List[AbstractNode]
    ) -> List[str]:

        node_outputs: Dict[AbstractNode, _NodeOutputs] = {
            node: _NodeOutputs() for node in nodes
        }
        disassembler = _Disassembler(self.code_object)
        for block, node in zip(blocks, nodes):
            stack: Stack = None
            if not node.origins:
                # function block entry point
                stack = []
            else:
                output_on_true = (
                    node_outputs[origin].on_true for origin in node.get_origin_true()
                )
                output_on_false = (
                    node_outputs[origin].on_false for origin in node.get_origin_false()
                )
                output_stacks = itertools.chain(output_on_true, output_on_false)
                for output_stack in output_stacks:
                    if output_stack is None:
                        pass
                    elif stack is None:
                        stack = output_stack
                    elif stack != output_stack:
                        raise NotImplementedError(
                            "conditional expression in yield part of generator expression"
                        )

            result = disassembler.process_block(
                self.instructions[block.start_index : block.end_index],
                stack,
            )

            node.expr = result.jump_expr
            node_outputs[node].on_true = result.on_true
            node_outputs[node].on_false = result.on_false

            if result.yield_expr:
                if self._yield_expr is None:
                    self._yield_expr = result.yield_expr
                elif self._yield_expr != result.yield_expr:
                    raise NotImplementedError(
                        "conditional expression in yield part of generator expression"
                    )

        return disassembler.variables

    def _get_condition(self, nodes: List[AbstractNode]) -> Expression:
        # extract conditional part from generator
        true_node = AbstractNode(Constant(True))
        false_node = AbstractNode(Constant(False))

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

        # eliminate nodes that correspond to unconditional forward jumps
        for node in cond_nodes:
            if node.expr is None and node.on_true is node.on_false:
                node.on_true.seize_origins(node)
                node.set_target(None, None)
        cond_nodes = [
            node
            for node in cond_nodes
            if node.expr is None and node.on_true is None and node.on_false is None
        ]

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
            replacements = False

            for start in range(len(nodes) - 1):
                # locate a span of abstract nodes that could form a single conjunction
                end = start + 1
                while end < len(nodes) and nodes[start].on_false is nodes[end].on_false:
                    end += 1

                if end - start > 1:
                    conj_expr = Conjunction([n.expr for n in nodes[start:end]])
                    conj_node = AbstractNode(conj_expr)
                    conj_node.set_target(nodes[end - 1].on_true, nodes[start].on_false)
                    conj_node.seize_origins(nodes[start])
                    for n in nodes[start:end]:
                        n.set_target(None, None)
                    nodes[start:end] = [conj_node]
                    replacements = True
                    break

                # locate a span of abstract nodes that could form a single disjunction
                end = start + 1
                while end < len(nodes) and nodes[start].on_true is nodes[end].on_true:
                    end += 1

                if end - start > 1:
                    disj_expr = Disjunction([n.expr for n in nodes[start:end]])
                    disj_node = AbstractNode(disj_expr)
                    disj_node.set_target(nodes[start].on_true, nodes[end - 1].on_false)
                    disj_node.seize_origins(nodes[start])
                    for n in nodes[start:end]:
                        n.set_target(None, None)
                    nodes[start:end] = [disj_node]
                    replacements = True
                    break

            if not replacements:
                nodes[0].twist()

        return nodes[0].expr

    def get_expression(self) -> CodeExpression:
        # convert instructions into abstract nodes with symbolic expressions
        blocks = _get_basic_blocks(self.instructions)
        nodes = self._get_abstract_nodes(blocks)
        variables = self._disassemble(blocks, nodes)

        # remove prolog and epilog from generator
        # prolog pushes the single iterable argument (a.k.a. ".0") to the stack that a generator expression receives:
        #       0 LOAD_FAST                0 (.0)
        # epilog pops the stack and returns None to indicate end of iteration
        # >>   48 LOAD_CONST               4 (None)
        #      50 RETURN_VALUE
        nodes = nodes[1:-1]

        # extract conditional expression
        cond_expr = self._get_condition(nodes) if len(nodes) > 2 else None

        return CodeExpression(variables, cond_expr, self._yield_expr)

    def _show_blocks(self, blocks):
        for block in blocks:
            for instr in self.instructions[block.start_index : block.end_index]:
                print(instr.opname)
            print("----")
