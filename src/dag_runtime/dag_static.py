"""Bounded proofs over exact built-in types, never evaluation of user code.

An annotation, function name, or lack of an obvious write is NOT a purity proof.
Unknown protocols, exceptions, dynamic bindings, and budget exhaustion fail closed.
"""
from __future__ import annotations

import ast
import math
import sys
from dataclasses import dataclass, field, replace
from typing import Callable

from .dag_model import Characteristics, EffectKind


@dataclass(frozen=True)
class TypeFact:
    kind: str = "unknown"
    element: TypeFact | None = None
    items: tuple[TypeFact, ...] | None = None
    function_id: str | None = None
    _passive: bool = field(init=False, repr=False)
    _immutable: bool = field(init=False, repr=False)
    shape_cost: int = field(init=False, repr=False)

    def __post_init__(self):
        scalar = self.kind in {'none', 'bool', 'int', 'float', 'complex', 'str', 'bytes', 'range', 'task_payload'}
        children = self.items if self.items is not None else ((self.element,) if self.element is not None else ())
        known_contents = self.items is not None or self.element is not None
        passive = scalar or self.kind in {'function', 'builtin'} or (
            self.kind in {'tuple', 'list', 'dict', 'set'} and known_contents and all(t.passive for t in children))
        immutable = scalar or self.kind == 'tuple' and known_contents and all(t.immutable for t in children)
        # Count both stored paths because dataclass hashing visits both fields.
        # Otherwise singleton nesting stores the same child as element AND item,
        # and function-specialization key hashing could expand exponentially.
        cost = 1 + sum(t.shape_cost for t in children)
        if self.items is not None and self.element is not None:
            cost += self.element.shape_cost
        object.__setattr__(self, '_passive', passive)
        object.__setattr__(self, '_immutable', immutable)
        object.__setattr__(self, 'shape_cost', min(cost, 65))
        # A source chain x=(x,x) must not grow a recursively expanded type tree.
        # Keep compositional safety facts but drop detailed projection shape.
        if cost > 64:
            object.__setattr__(self, 'items', None)
            object.__setattr__(self, 'element', None)

    @property
    def passive(self) -> bool:
        return self._passive

    @property
    def immutable(self) -> bool:
        return self._immutable

    def describe(self) -> str:
        if self.element is not None:
            return f"{self.kind}[{self.element.kind}]"
        return self.kind

    @property
    def flat_immutable_sequence(self) -> bool:
        """Exact sequence with independently immutable contents, never an annotation."""
        return self.kind in {'list', 'tuple'} and (
            self.items is not None and all(t.immutable for t in self.items) or
            self.element is not None and self.element.immutable)


UNKNOWN = TypeFact()
INT = TypeFact("int")
BOOL = TypeFact("bool")
NONE = TypeFact("none")

# Explicit capability syntax is kept native. Dynamic unknown attribute names
# receive a namespace barrier instead; no facts about their returned value survive.
NAMESPACE_ATTRIBUTES = frozenset({'__globals__', '__dict__', 'f_globals', 'f_locals',
                                 'f_back', 'gi_frame', 'cr_frame', 'ag_frame', 'tb_frame'})


@dataclass(frozen=True)
class Proof:
    fact: TypeFact = UNKNOWN
    reasons: tuple[str, ...] = ()
    may_raise: bool = False
    unknown_calls: bool = False
    effect: EffectKind = EffectKind.PURE
    fresh_result: bool = False  # allocated by this expression; not a borrowed argument
    allocates_container: bool = False  # potential cyclic-GC/finalizer observation after unknown code
    # Optional exact/bounded integer facts used only inside bounded local-function proof.
    # They are not exported as user-visible type claims.
    int_min: int | None = None
    int_max: int | None = None
    explicit_task_contract: bool = False
    # Optional bounds for exact built-in floats.  They are an abstract proof
    # aid only; no constant folding or user-visible value prediction depends on
    # them.  Missing bounds simply mean "exact float, value range unknown".
    float_min: float | None = None
    float_max: float | None = None
    # A mutable container created inside the currently analysed function and
    # not (yet) aliased outside the simple local-name subset.  This is used only
    # to permit narrowly-modelled local mutation such as exact-list append.
    local_owned: bool = False
    # Exact cardinality for an exact built-in range when statically known.
    # This is used only to prove whether len(range(...)) fits Py_ssize_t.
    range_length: int | None = None

    @property
    def safe(self) -> bool:
        return (not self.reasons and not self.may_raise and
                self.effect in {EffectKind.PURE, EffectKind.OBJECT_LOCAL})


def uncertain(reason: str, *, unknown_calls: bool = False,
              effect: EffectKind = EffectKind.NAMESPACE, fact: TypeFact = UNKNOWN) -> Proof:
    return Proof(fact, reasons=(reason,), may_raise=True, unknown_calls=unknown_calls, effect=effect)


def combine(fact: TypeFact, proofs: list[Proof], *, fresh_result: bool = False) -> Proof:
    rank = {EffectKind.PURE: 0, EffectKind.OBJECT_LOCAL: 1, EffectKind.NAMESPACE: 2, EffectKind.ESCAPE: 3}
    return Proof(fact, tuple(dict.fromkeys(r for p in proofs for r in p.reasons)),
                 any(p.may_raise for p in proofs), any(p.unknown_calls for p in proofs),
                 max((p.effect for p in proofs), key=rank.__getitem__, default=EffectKind.PURE), fresh_result,
                 fresh_result or any(p.allocates_container for p in proofs),
                 explicit_task_contract=any(p.explicit_task_contract for p in proofs),
                 local_owned=(fresh_result and fact.kind in {'list', 'dict', 'set'}))


def _int_bounds_binary(op: ast.operator, left: Proof, right: Proof) -> tuple[int | None, int | None]:
    """Best-effort integer interval arithmetic for local-function totality proofs.

    Unknown bounds remain unknown.  These bounds are never used to infer Python
    object identity or user-defined protocol behavior; they only discharge exact
    integer zero-division checks in the already-proved built-in-int subset.
    """
    lo1, hi1, lo2, hi2 = left.int_min, left.int_max, right.int_min, right.int_max
    if isinstance(op, ast.Add):
        return ((lo1 + lo2) if lo1 is not None and lo2 is not None else None,
                (hi1 + hi2) if hi1 is not None and hi2 is not None else None)
    if isinstance(op, ast.Sub):
        return ((lo1 - hi2) if lo1 is not None and hi2 is not None else None,
                (hi1 - lo2) if hi1 is not None and lo2 is not None else None)
    if isinstance(op, ast.Mult) and None not in (lo1, hi1, lo2, hi2):
        products = (lo1 * lo2, lo1 * hi2, hi1 * lo2, hi1 * hi2)
        return min(products), max(products)
    return None, None


def _excludes_zero(proof: Proof) -> bool:
    return ((proof.int_min is not None and proof.int_min > 0) or
            (proof.int_max is not None and proof.int_max < 0))


# Keep mixed int/float proofs deliberately narrow.  Integers inside this range
# convert to binary64 exactly, so the analyzer never has to model CPython's
# OverflowError path for enormous ints or reason about conversion rounding.
_EXACT_FLOAT_INT_LIMIT = 1 << sys.float_info.mant_dig


def _int_exactly_float_convertible(proof: Proof) -> bool:
    return (
        proof.fact.kind in {'int', 'bool'}
        and proof.int_min is not None
        and proof.int_max is not None
        and -_EXACT_FLOAT_INT_LIMIT <= proof.int_min <= proof.int_max <= _EXACT_FLOAT_INT_LIMIT
    )


def _float_interval(proof: Proof) -> tuple[float, float] | None:
    """Return a finite interval usable for exact built-in float proofs.

    Exact-float type knowledge survives even when an interval is unavailable.
    Bounds are used only to discharge totality checks such as non-zero division;
    losing them therefore reduces precision rather than safety.
    """
    if proof.fact.kind == 'float':
        if proof.float_min is None or proof.float_max is None:
            return None
        if not math.isfinite(proof.float_min) or not math.isfinite(proof.float_max):
            return None
        return proof.float_min, proof.float_max
    if _int_exactly_float_convertible(proof):
        return float(proof.int_min), float(proof.int_max)
    return None


def _numeric_excludes_zero(proof: Proof) -> bool:
    if proof.fact.kind in {'int', 'bool'}:
        return _excludes_zero(proof)
    interval = _float_interval(proof)
    return bool(interval and (interval[0] > 0.0 or interval[1] < 0.0))


def _float_bounds_binary(op: ast.operator, left: Proof, right: Proof) -> tuple[float | None, float | None]:
    """Best-effort finite interval arithmetic for built-in numeric operations."""
    lhs, rhs = _float_interval(left), _float_interval(right)
    if lhs is None or rhs is None:
        return None, None
    lo1, hi1 = lhs
    lo2, hi2 = rhs
    try:
        if isinstance(op, ast.Add):
            lo, hi = lo1 + lo2, hi1 + hi2
        elif isinstance(op, ast.Sub):
            lo, hi = lo1 - hi2, hi1 - lo2
        elif isinstance(op, ast.Mult):
            values = (lo1 * lo2, lo1 * hi2, hi1 * lo2, hi1 * hi2)
            if any(math.isnan(v) for v in values):
                return None, None
            lo, hi = min(values), max(values)
        elif isinstance(op, ast.Div):
            if lo2 <= 0.0 <= hi2:
                return None, None
            values = (lo1 / lo2, lo1 / hi2, hi1 / lo2, hi1 / hi2)
            if any(math.isnan(v) for v in values):
                return None, None
            lo, hi = min(values), max(values)
        else:
            return None, None
    except (OverflowError, ZeroDivisionError):
        return None, None
    if not math.isfinite(lo) or not math.isfinite(hi):
        return None, None
    return lo, hi


def _exact_numeric_result_kind(left: Proof, right: Proof) -> str | None:
    """Return int/float for a safely modelled exact built-in numeric pair."""
    left_int = left.fact.kind in {'int', 'bool'}
    right_int = right.fact.kind in {'int', 'bool'}
    if left_int and right_int:
        return 'int'
    if left.fact.kind == 'float' and right.fact.kind == 'float':
        return 'float'
    if left.fact.kind == 'float' and _int_exactly_float_convertible(right):
        return 'float'
    if right.fact.kind == 'float' and _int_exactly_float_convertible(left):
        return 'float'
    return None


def _exact_range_element_bounds(args: list[Proof]) -> tuple[int | None, int | None]:
    """Element bounds for an exact constant range without iterating it."""
    if not args or not all(p.int_min is not None and p.int_min == p.int_max for p in args):
        return None, None
    values = [p.int_min for p in args]
    if len(values) == 1:
        start, stop, step = 0, values[0], 1
    elif len(values) == 2:
        start, stop, step = values[0], values[1], 1
    else:
        start, stop, step = values
    if step == 0:
        return None, None
    if step > 0:
        if start >= stop:
            return None, None
        last = start + ((stop - start - 1) // step) * step
    else:
        if start <= stop:
            return None, None
        last = start - ((start - stop - 1) // (-step)) * (-step)
    return min(start, last), max(start, last)


def _exact_range_length(args: list[Proof]) -> int | None:
    """Exact mathematical cardinality of a constant built-in range.

    Python permits arbitrarily large integer endpoints, while len(range(...))
    raises OverflowError when the cardinality exceeds Py_ssize_t.  Never call
    len(range(...)) here; compute the cardinality with arbitrary-precision ints.
    """
    if not args or not all(p.int_min is not None and p.int_min == p.int_max for p in args):
        return None
    values = [p.int_min for p in args]
    if len(values) == 1:
        start, stop, step = 0, values[0], 1
    elif len(values) == 2:
        start, stop, step = values[0], values[1], 1
    else:
        start, stop, step = values
    if step == 0:
        return None
    if step > 0:
        return 0 if start >= stop else ((stop - start - 1) // step) + 1
    return 0 if start <= stop else ((start - stop - 1) // (-step)) + 1


def common_type(items: list[TypeFact]) -> TypeFact:
    return items[0] if items and all(x == items[0] for x in items) else UNKNOWN


def literal_index(node: ast.AST) -> int | None:
    """Read only literal integer syntax, not arbitrary constant folding."""
    if isinstance(node, ast.Constant) and type(node.value) in (int, bool):
        return int(node.value)
    if (isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)) and
            isinstance(node.operand, ast.Constant) and type(node.operand.value) in (int, bool)):
        value = int(node.operand.value)
        return -value if isinstance(node.op, ast.USub) else value
    return None


class Names(ast.NodeVisitor):
    """Conservative lexical facts; never execute a nested definition body.

    Comprehension targets and lambda parameters are lexical locals. Loads in
    lambdas are retained as captures, not treated as executions of their calls.
    """

    def __init__(self):
        self.reads: dict[str, None] = {}
        self.writes: dict[str, None] = {}
        self.deletes: set[str] = set()
        self.locals: list[set[str]] = []
        self.global_names: set[str] = set()
        self.calls: dict[str, None] = {}
        self.escape_reasons: dict[str, None] = {}

    def visit_Name(self, node):
        if any(node.id in scope for scope in reversed(self.locals)):
            return
        if isinstance(node.ctx, ast.Load):
            self.reads[node.id] = None
        else:
            self.writes[node.id] = None
            if isinstance(node.ctx, ast.Del):
                self.deletes.add(node.id)

    def visit_FunctionDef(self, node):
        self.writes[node.name] = None
        for expression in (*node.decorator_list, *node.args.defaults,
                           *(d for d in node.args.kw_defaults if d is not None)):
            self.visit(expression)
        for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            if arg.annotation:
                self.visit(arg.annotation)
        for arg in (node.args.vararg, node.args.kwarg):
            if arg and arg.annotation:
                self.visit(arg.annotation)
        if node.returns:
            self.visit(node.returns)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        # Class bodies execute, but class-local bindings do not bind module names.
        self.writes[node.name] = None
        for expression in (*node.decorator_list, *node.bases, *(k.value for k in node.keywords)):
            self.visit(expression)
        inner = Names()
        for statement in node.body:
            inner.visit(statement)
        for name in inner.reads:
            self.reads[name] = None  # overapproximation, namespace handles missing class locals
        self.escape_reasons.update(inner.escape_reasons)

    def visit_Import(self, node):
        for alias in node.names:
            self.writes[alias.asname or alias.name.split('.')[0]] = None

    def visit_ImportFrom(self, node):
        for alias in node.names:
            if alias.name == '*':
                self.escape_reasons['wildcard import may install unenumerated live capabilities and bindings'] = None
            else:
                self.writes[alias.asname or alias.name] = None

    def visit_ExceptHandler(self, node):
        if node.name:
            self.writes[node.name] = None
            self.deletes.add(node.name)
        self.generic_visit(node)

    def visit_Global(self, node):
        self.global_names.update(node.names)

    visit_Nonlocal = visit_Global

    def visit_MatchAs(self, node):
        if node.name:
            self.writes[node.name] = None
        self.generic_visit(node)

    visit_MatchStar = visit_MatchAs

    def visit_MatchMapping(self, node):
        if node.rest:
            self.writes[node.rest] = None
        self.generic_visit(node)

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            self.calls[node.func.id] = None
        if isinstance(node.func, ast.Name):
            name = node.func.id
            if name in {'eval', 'exec'}:
                self.escape_reasons[f'{name} executes unbounded dynamic code; namespace/capability containment is unproved'] = None
            elif name in {'globals', 'locals'} or name == 'vars' and not node.args and not node.keywords:
                self.escape_reasons[f'{name} exposes a live scope mapping; retain its native scope identity and lifetime'] = None
            elif name in {'getattr', 'setattr', 'delattr'} and len(node.args) > 1:
                key = node.args[1]
                if isinstance(key, ast.Constant) and isinstance(key.value, str) and key.value in NAMESPACE_ATTRIBUTES:
                    self.escape_reasons[f'explicit namespace/frame capability {key.value!r} requires native scope retention'] = None
        self.generic_visit(node)

    def visit_Attribute(self, node):
        if node.attr in NAMESPACE_ATTRIBUTES:
            self.escape_reasons[f'explicit namespace/frame capability {node.attr!r} requires native scope retention'] = None
        self.generic_visit(node)

    def visit_Lambda(self, node):
        for expr in (*node.args.defaults, *(d for d in node.args.kw_defaults if d is not None)):
            self.visit(expr)
        bound = parameter_names(node.args)
        self.locals.append(bound)
        self.visit(node.body)
        self.locals.pop()

    def _comprehension(self, node):
        self.locals.append(set())
        for generator in node.generators:
            self.visit(generator.iter)  # iterable is evaluated before target binding
            self.locals[-1].update(target_names(generator.target))
            for expr in generator.ifs:
                self.visit(expr)
        if isinstance(node, ast.DictComp):
            self.visit(node.key)
            self.visit(node.value)
        else:
            self.visit(node.elt)
        self.locals.pop()

    visit_ListComp = _comprehension
    visit_SetComp = _comprehension
    visit_DictComp = _comprehension
    visit_GeneratorExp = _comprehension

    def visit_NamedExpr(self, node):
        # Assignment expressions in comprehensions bind in the containing scope.
        if isinstance(node.target, ast.Name):
            self.writes[node.target.id] = None
        self.visit(node.value)


def parameter_names(args: ast.arguments) -> set[str]:
    result = {p.arg for p in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
    for p in (args.vararg, args.kwarg):
        if p:
            result.add(p.arg)
    return result


def target_names(node: ast.AST) -> tuple[str, ...]:
    result = []
    stack = [node]
    while stack:
        part = stack.pop()
        if isinstance(part, ast.Name):
            result.append(part.id)
        elif isinstance(part, (ast.Tuple, ast.List)):
            stack.extend(reversed(part.elts))
        elif isinstance(part, ast.Starred):
            stack.append(part.value)
    return tuple(result)


def characteristics(node: ast.AST, *, include_body: bool = True) -> Characteristics:
    count = calls = loops = comprehensions = depth_max = 0
    stack = [(node, 0)]
    while stack:
        current, depth = stack.pop()
        count += 1
        calls += isinstance(current, ast.Call)
        if isinstance(current, (ast.For, ast.While, ast.AsyncFor)):
            loops += 1
            depth += 1
            depth_max = max(depth_max, depth)
        comprehensions += isinstance(current, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp))
        if not include_body and isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        stack.extend((child, depth) for child in ast.iter_child_nodes(current))
    return Characteristics(count, calls, loops, depth_max, comprehensions,
                           complexity="complex" if loops or comprehensions or count > 80 else "simple")


@dataclass
class FunctionSummary:
    id: str
    node: ast.FunctionDef
    free_names: tuple[str, ...]
    eligible: bool
    reason: str
    stats: Characteristics
    explicit_task: bool = False


_SAFE_FUNCTION_BUILTINS = frozenset({'sum', 'len', 'abs', 'min', 'max', 'range'})


def _supported_local_block(statements: list[ast.stmt], *, in_loop: bool = False) -> str | None:
    """Return the first unsupported local-control-flow reason, if any.

    This is only a syntactic gate.  Expression types, calls and exception
    totality are still proved later for the concrete call specialization.
    """
    for statement in statements:
        if isinstance(statement, ast.Assign):
            if not statement.targets or not all(isinstance(t, ast.Name) for t in statement.targets):
                return 'function assignment target is outside the local-name subset'
        elif isinstance(statement, ast.AugAssign):
            if not isinstance(statement.target, ast.Name):
                return 'function augmented assignment may mutate external/object state'
            if not isinstance(statement.op, (ast.Add, ast.Sub, ast.Mult)):
                return 'function augmented assignment operator is outside the exact numeric subset'
        elif isinstance(statement, ast.If):
            reason = _supported_local_block(statement.body, in_loop=in_loop)
            if reason:
                return reason
            reason = _supported_local_block(statement.orelse, in_loop=in_loop)
            if reason:
                return reason
        elif isinstance(statement, (ast.For, ast.While)):
            if isinstance(statement, ast.For) and not isinstance(statement.target, ast.Name):
                return 'function loop target is outside the simple local-name subset'
            reason = _supported_local_block(statement.body, in_loop=True)
            if reason:
                return reason
            reason = _supported_local_block(statement.orelse, in_loop=in_loop)
            if reason:
                return reason
        elif isinstance(statement, (ast.Break, ast.Continue)):
            if not in_loop:
                return 'loop control appears outside a supported loop'
        elif isinstance(statement, ast.Pass):
            pass
        elif isinstance(statement, ast.Expr):
            # A function docstring is harmless; other expression statements must
            # still pass the concrete expression proof during specialization.
            pass
        else:
            return f'{type(statement).__name__} is outside the bounded local-function statement subset'
    return None


def summarize(function_id: str, node: ast.FunctionDef, limit: int, *, explicit_task: bool = False) -> FunctionSummary:
    names = Names()
    for statement in node.body:
        names.visit(statement)
    local = set(names.writes) | parameter_names(node.args)
    local -= names.global_names
    free = tuple(n for n in names.reads if n not in local)
    stats = characteristics(node)
    reasons = []
    if stats.ast_nodes > limit and not explicit_task:
        reasons.append('local function exceeds the body proof budget')
    if names.global_names and not explicit_task:
        reasons.append('function declares global/nonlocal state')
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        body = body[1:]
    if not body or not isinstance(body[-1], ast.Return):
        reasons.append('function requires one final return in the bounded local subset')
    else:
        reason = _supported_local_block(body[:-1])
        if reason and not explicit_task:
            reasons.append(reason)
    if free and any(n not in _SAFE_FUNCTION_BUILTINS for n in free) and not explicit_task:
        reasons.append('function reads globals, a closure, or an unknown callable')
    return FunctionSummary(function_id, node, free, not reasons, '; '.join(dict.fromkeys(reasons)), stats, explicit_task)


@dataclass
class _LocalFlow:
    """Abstract exits from one local-function statement block."""
    env: dict[str, Proof] | None
    failure: Proof | None = None
    allocated_container: bool = False
    breaks: tuple[dict[str, Proof], ...] = ()
    continues: tuple[dict[str, Proof], ...] = ()


class ProofEngine:
    """One level of function-body inspection; specializations are bounded and cached."""

    def __init__(self, summaries: dict[str, FunctionSummary], *, budget: int = 100_000):
        self.summaries = summaries
        self.remaining = budget
        self.cache: dict[tuple, Proof] = {}

    def expression(self, node: ast.AST, lookup: Callable[[str], Proof], *,
                   builtin_ok: Callable[[str], bool], in_function: bool = False) -> Proof:
        self.remaining -= 1
        if self.remaining < 0:
            return uncertain("static expression proof budget exhausted")
        recur = lambda x: self.expression(x, lookup, builtin_ok=builtin_ok, in_function=in_function)
        if isinstance(node, ast.Constant):
            kind = {type(None): 'none', bool: 'bool', int: 'int', float: 'float', complex: 'complex',
                    str: 'str', bytes: 'bytes'}.get(type(node.value))
            if not kind:
                return uncertain("constant kind is outside the proof subset")
            if type(node.value) in (int, bool):
                value = int(node.value)
                return Proof(TypeFact(kind), int_min=value, int_max=value)
            if type(node.value) is float and math.isfinite(node.value):
                return Proof(TypeFact(kind), float_min=node.value, float_max=node.value)
            return Proof(TypeFact(kind))
        if isinstance(node, ast.Name):
            return lookup(node.id)
        if isinstance(node, (ast.List, ast.Tuple)):
            if any(isinstance(x, ast.Starred) for x in node.elts):
                return uncertain("star expansion can invoke iteration protocols")
            proofs = [recur(x) for x in node.elts]
            types = [p.fact for p in proofs]
            fact = TypeFact('list' if isinstance(node, ast.List) else 'tuple',
                            element=common_type(types) if types else NONE,
                            items=tuple(types) if len(types) <= 32 else None)
            return combine(fact, proofs, fresh_result=True)
        if isinstance(node, (ast.Dict, ast.Set)):
            if isinstance(node, ast.Dict):
                if any(k is None for k in node.keys):
                    return uncertain("mapping expansion can invoke user protocols")
                keys = [recur(k) for k in node.keys]
                vals = [recur(v) for v in node.values]
            else:
                keys, vals = [recur(k) for k in node.elts], []
            if not all(p.fact.kind in {'none', 'bool', 'int', 'float', 'complex', 'str', 'bytes'} for p in keys):
                return uncertain("hashing or comparing container keys may invoke Python code or raise")
            items = [p.fact for p in keys + vals]
            fact = TypeFact('dict' if isinstance(node, ast.Dict) else 'set',
                            element=common_type(items) if items else NONE,
                            items=tuple(items) if len(items) <= 32 else None)
            return combine(fact, keys + vals, fresh_result=True)
        if isinstance(node, ast.BinOp):
            left, right = recur(node.left), recur(node.right)
            numeric_kind = _exact_numeric_result_kind(left, right)
            if isinstance(node.op, (ast.Add, ast.Sub, ast.Mult)) and numeric_kind:
                result = combine(INT if numeric_kind == 'int' else TypeFact('float'), [left, right])
                if numeric_kind == 'int':
                    lo, hi = _int_bounds_binary(node.op, left, right)
                    result = replace(result, int_min=lo, int_max=hi)
                else:
                    lo, hi = _float_bounds_binary(node.op, left, right)
                    result = replace(result, float_min=lo, float_max=hi)
                return result
            ints = left.fact.kind in {'int', 'bool'} and right.fact.kind in {'int', 'bool'}
            if ints and left.safe and right.safe and isinstance(node.op, (ast.FloorDiv, ast.Mod)):
                # Modulo uses the bounded integer proof to exclude zero.
                # Floor division keeps the historical completion-fence contract
                # except for an exact non-zero integer literal divisor.  That
                # narrow case is enough for common local numeric loops such as
                # ``x // 2`` without changing existing semantics for a variable
                # divisor whose value merely happens to be known at one call site.
                literal_nonzero_int = (
                    isinstance(node.right, ast.Constant)
                    and type(node.right.value) in (int, bool)
                    and int(node.right.value) != 0
                )
                if isinstance(node.op, ast.Mod) and _excludes_zero(right):
                    return combine(INT, [left, right])
                if isinstance(node.op, ast.FloorDiv) and literal_nonzero_int:
                    return combine(INT, [left, right])
                return combine(INT, [left, right, uncertain('exact integer division may raise; completion ordering is required without namespace invalidation',
                                                            effect=EffectKind.PURE)])
            if isinstance(node.op, ast.Div) and numeric_kind and left.safe and right.safe:
                # Built-in true division is safe only when we can exclude a zero
                # divisor.  Mixed int/float pairs were already restricted above
                # to exactly-convertible integer ranges, avoiding OverflowError.
                if not _numeric_excludes_zero(right):
                    return combine(TypeFact('float'), [left, right,
                        uncertain('exact numeric division may have a zero divisor; preserve exception order',
                                  effect=EffectKind.PURE)])
                if numeric_kind == 'int' and not (
                    _int_exactly_float_convertible(left) and _int_exactly_float_convertible(right)
                ):
                    return combine(TypeFact('float'), [left, right,
                        uncertain('integer true division may overflow during float conversion; preserve exception order',
                                  effect=EffectKind.PURE)])
                result = combine(TypeFact('float'), [left, right])
                lo, hi = _float_bounds_binary(node.op, left, right)
                return replace(result, float_min=lo, float_max=hi)
            return uncertain("operator may dispatch to user methods, raise, or is outside the exact-type subset")
        if isinstance(node, ast.UnaryOp):
            proof = recur(node.operand)
            if isinstance(node.op, ast.Invert) and proof.fact.kind == 'bool':
                return uncertain('bool inversion can emit a deprecation warning or raise; warning hooks are unproved')
            if isinstance(node.op, (ast.UAdd, ast.USub, ast.Invert)) and proof.fact.kind in {'int', 'bool'}:
                result = combine(INT, [proof])
                if isinstance(node.op, ast.UAdd):
                    result = replace(result, int_min=proof.int_min, int_max=proof.int_max)
                elif isinstance(node.op, ast.USub):
                    result = replace(result,
                                     int_min=-proof.int_max if proof.int_max is not None else None,
                                     int_max=-proof.int_min if proof.int_min is not None else None)
                return result
            if isinstance(node.op, (ast.UAdd, ast.USub)) and proof.fact.kind == 'float':
                result = combine(TypeFact('float'), [proof])
                if proof.float_min is not None and proof.float_max is not None:
                    if isinstance(node.op, ast.UAdd):
                        return replace(result, float_min=proof.float_min, float_max=proof.float_max)
                    return replace(result, float_min=-proof.float_max, float_max=-proof.float_min)
                return result
            if isinstance(node.op, ast.Not) and proof.fact.passive:
                return combine(BOOL, [proof])
            return uncertain("unary operation has unproved protocol/exception behavior")
        if isinstance(node, ast.Compare):
            proofs = [recur(node.left), *(recur(x) for x in node.comparators)]
            if all(p.fact.kind in {'int', 'bool'} for p in proofs) and all(
                isinstance(op, (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)) for op in node.ops
            ):
                return combine(BOOL, proofs)
            if all(p.fact.kind == 'float' for p in proofs) and all(
                isinstance(op, (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)) for op in node.ops
            ):
                return combine(BOOL, proofs)
            return uncertain("comparison, membership, identity, or short-circuit protocol is unproved")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
                return uncertain("dynamic/attribute call or argument expansion has unknown effects", unknown_calls=True)
            args = [recur(a) for a in node.args]
            kwargs = {k.arg: recur(k.value) for k in node.keywords}
            if any(not p.safe for p in args + list(kwargs.values())):
                return combine(UNKNOWN, args + list(kwargs.values()) + [uncertain("call argument evaluation is unproved", unknown_calls=True)])
            name = node.func.id
            if name in _SAFE_FUNCTION_BUILTINS and builtin_ok(name):
                if name in {'min', 'max'}:
                    # The supported form is scalar min/max over >=2 exact
                    # built-in scalar arguments.  The zero-argument form and
                    # one exact scalar argument fail without invoking user code,
                    # so retain PURE while preserving their TypeError ordering.
                    if kwargs:
                        return uncertain(f"builtin {name} call signature/types are outside the supported subset")
                    if len(args) < 2:
                        if not args or all(p.fact.kind in {'int', 'bool', 'float'} for p in args):
                            return uncertain(
                                f"builtin {name} scalar call may raise TypeError; preserve exception order",
                                effect=EffectKind.PURE)
                        return uncertain(f"builtin {name} iterable form is outside the supported subset")
                    if all(p.fact.kind == 'bool' for p in args):
                        # min/max return one of their actual arguments.  Keep
                        # the exact bool type so later operations (notably
                        # ``~bool``) cannot accidentally inherit the int proof.
                        result = Proof(BOOL)
                        lows = [p.int_min for p in args]
                        highs = [p.int_max for p in args]
                        if name == 'max':
                            lo = max((v for v in lows if v is not None), default=None)
                            hi = max(highs) if all(v is not None for v in highs) else None
                        else:
                            lo = min(lows) if all(v is not None for v in lows) else None
                            hi = min((v for v in highs if v is not None), default=None)
                        return replace(result, int_min=lo, int_max=hi)
                    if all(p.fact.kind == 'int' for p in args):
                        result = Proof(INT)
                        lows = [p.int_min for p in args]
                        highs = [p.int_max for p in args]
                        if name == 'max':
                            lo = max((v for v in lows if v is not None), default=None)
                            hi = max(highs) if all(v is not None for v in highs) else None
                        else:
                            lo = min(lows) if all(v is not None for v in lows) else None
                            hi = min((v for v in highs if v is not None), default=None)
                        return replace(result, int_min=lo, int_max=hi)
                    if all(p.fact.kind in {'int', 'bool'} for p in args):
                        # bool is an int subclass for comparison, but min/max
                        # preserve the selected argument's runtime type.  The
                        # current abstract type domain has no bool|int union,
                        # so mixed candidates must remain conservative rather
                        # than being mislabelled as exact int.
                        return uncertain(
                            f"builtin {name} mixed bool/int result type is value/order dependent",
                            effect=EffectKind.PURE)
                    if all(p.fact.kind == 'float' for p in args):
                        intervals = [_float_interval(p) for p in args]
                        result = Proof(TypeFact('float'))
                        if all(interval is not None for interval in intervals):
                            lows = [interval[0] for interval in intervals]
                            highs = [interval[1] for interval in intervals]
                            if name == 'max':
                                return replace(result, float_min=max(lows), float_max=max(highs))
                            return replace(result, float_min=min(lows), float_max=min(highs))
                        return result
                    return uncertain(f"builtin {name} call signature/types are outside the supported subset")
                if name == 'range':
                    if kwargs or not 1 <= len(args) <= 3 or not all(p.fact.kind in {'int', 'bool'} for p in args):
                        return uncertain('range call signature/types are outside the supported subset')
                    if len(args) == 3 and not _excludes_zero(args[2]):
                        return uncertain('range step may be zero')
                    lo, hi = _exact_range_element_bounds(args)
                    return Proof(TypeFact('range'), int_min=lo, int_max=hi,
                                 range_length=_exact_range_length(args))
                if len(args) != 1 or kwargs:
                    return uncertain("builtin call signature is outside the supported subset")
                typ = args[0].fact
                if name == 'len' and typ.kind in {'list', 'tuple', 'dict', 'set', 'str', 'bytes'}:
                    return combine(INT, args)
                if name == 'len' and typ.kind == 'range':
                    if args[0].range_length is not None and args[0].range_length <= sys.maxsize:
                        return replace(combine(INT, args),
                                       int_min=args[0].range_length, int_max=args[0].range_length)
                    return combine(INT, args + [uncertain(
                        'range length may exceed Py_ssize_t; preserve OverflowError ordering',
                        effect=EffectKind.PURE)])
                if name == 'abs' and typ.kind in {'int', 'bool'}:
                    result = combine(INT, args)
                    lo = 0 if args[0].int_min is not None or args[0].int_max is not None else None
                    hi = None
                    if args[0].int_min is not None and args[0].int_max is not None:
                        hi = max(abs(args[0].int_min), abs(args[0].int_max))
                    return replace(result, int_min=lo, int_max=hi)
                if name == 'abs' and typ.kind == 'float':
                    result = combine(TypeFact('float'), args)
                    interval = _float_interval(args[0])
                    if interval is None:
                        return result
                    lo, hi = interval
                    if lo >= 0.0:
                        return replace(result, float_min=lo, float_max=hi)
                    if hi <= 0.0:
                        return replace(result, float_min=-hi, float_max=-lo)
                    return replace(result, float_min=0.0, float_max=max(-lo, hi))
                if name == 'sum' and typ.kind in {'list', 'tuple'} and (
                    typ.element is not None and typ.element.kind in {'int', 'bool'} or
                    typ.items is not None and all(t.kind in {'int', 'bool'} for t in typ.items)
                ):
                    return combine(INT, args)
                if name == 'sum' and typ.kind in {'list', 'tuple'} and (
                    typ.element is not None and typ.element.kind == 'float' or
                    typ.items is not None and all(t.kind == 'float' for t in typ.items)
                ):
                    return combine(TypeFact('float'), args)
                return uncertain("builtin may invoke user protocols or raise for these argument types")
            callee = lookup(name)
            if not callee.safe or callee.fact.function_id not in self.summaries:
                return uncertain("unknown callable: side effects, mutation, global rebinding, and exceptions are possible", unknown_calls=True)
            if in_function:
                return uncertain("interprocedural proof is limited to one local function body", unknown_calls=True)
            summary = self.summaries[callee.fact.function_id]
            if not summary.eligible:
                if summary.explicit_task:
                    return Proof(TypeFact('task_payload'), fresh_result=True, explicit_task_contract=True)
                return uncertain(summary.reason, unknown_calls=bool(summary.stats.call_count))
            result = self._call(summary, args, kwargs, builtin_ok)
            if summary.explicit_task and result.safe:
                result = replace(result, explicit_task_contract=True)
            if not result.safe and summary.explicit_task:
                # Explicit @task is a programmer contract: all runtime data must
                # arrive through arguments, no externally visible state may be
                # mutated, and the returned value is an owned transferable value.
                # Violating that contract is user error, not an analyzer proof.
                result = Proof(TypeFact('task_payload'), fresh_result=True, explicit_task_contract=True)
            return replace(result, allocates_container=result.allocates_container or
                           any(p.allocates_container for p in args + list(kwargs.values())))
        if isinstance(node, ast.ListComp):
            return self._list_comprehension(node, lookup, builtin_ok, in_function)
        if isinstance(node, (ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            return uncertain("comprehension/generator stays within one region; iteration and calls are not expanded")
        if isinstance(node, ast.Attribute):
            return uncertain("attribute access may run a descriptor or __getattribute__")
        if isinstance(node, ast.Subscript):
            base = recur(node.value)
            index = literal_index(node.slice)
            if base.safe and base.fact.kind in {'list', 'tuple'} and base.fact.items is not None and index is not None:
                items = base.fact.items
                if not -len(items) <= index < len(items):
                    return combine(UNKNOWN, [base, uncertain('exact built-in subscription has an out-of-bounds index; preserve exception order',
                                                            effect=EffectKind.PURE)])
                if items[index].immutable:
                    return combine(items[index], [base])
                return uncertain('subscription returning mutable/unknown aliases is outside the precise subset')
            return uncertain("subscription may invoke __getitem__ or raise; preserve order")
        if isinstance(node, ast.Lambda):
            return uncertain("lambda/closure captures late-bound namespace state")
        if isinstance(node, (ast.BoolOp, ast.IfExp)):
            return uncertain("conditional expression retains short-circuit evaluation inside one region")
        if isinstance(node, (ast.Await, ast.Yield, ast.YieldFrom)):
            return uncertain("async/generator execution requires native sequential context")
        if isinstance(node, ast.NamedExpr):
            return uncertain("assignment expression updates namespace within an expression")
        return uncertain(f"{type(node).__name__} is outside the bounded expression proof")

    def _list_comprehension(self, node, lookup, builtin_ok, in_function):
        """One abstract iteration, never task/iteration expansion or user evaluation."""
        if (len(node.generators) != 1 or node.generators[0].is_async or
                not isinstance(node.generators[0].target, ast.Name)):
            return uncertain('comprehension requires one synchronous generator with a simple local target')
        generator = node.generators[0]
        iterable = self.expression(generator.iter, lookup, builtin_ok=builtin_ok, in_function=in_function)
        typ = iterable.fact
        if not iterable.safe or not typ.flat_immutable_sequence:
            return uncertain('comprehension iteration requires an exact list/tuple of immutable values')
        element = common_type(list(typ.items)) if typ.items else typ.element
        if element is None or not element.immutable:
            return uncertain('comprehension element type is unknown, heterogeneous, or beyond the shape budget')
        local = generator.target.id
        inner_lookup = lambda name: Proof(element) if name == local else lookup(name)
        inner_builtin = lambda name: name != local and builtin_ok(name)
        proofs = [self.expression(expr, inner_lookup, builtin_ok=inner_builtin, in_function=in_function)
                  for expr in (*generator.ifs, node.elt)]
        if any(not p.safe or p.effect != EffectKind.PURE or not p.fact.immutable for p in proofs):
            return combine(UNKNOWN, proofs + [uncertain('comprehension body/filter lacks a pure, total, immutable-value proof')])
        result = proofs[-1].fact
        shape = tuple(result for _ in typ.items) if typ.items is not None and not generator.ifs else None
        return Proof(
            TypeFact('list', element=result, items=shape),
            fresh_result=True,
            allocates_container=True,
            local_owned=True,
        )

    def _join_local_proof(self, left: Proof | None, right: Proof | None, name: str) -> Proof:
        if left is None or right is None:
            return uncertain(f"local name {name!r} may be unbound on some control-flow path", effect=EffectKind.PURE)
        if left.fact == right.fact:
            fact = left.fact
        elif left.local_owned and right.local_owned and left.fact.kind == right.fact.kind == 'list':
            # Local exact lists may grow across loop iterations.  Preserve only a
            # stable element type; exact length/item shape is deliberately lost.
            if left.fact.items == ():
                element = right.fact.element
            elif right.fact.items == ():
                element = left.fact.element
            elif left.fact.element == right.fact.element:
                element = left.fact.element
            else:
                element = UNKNOWN
            fact = TypeFact('list', element=element)
        else:
            fact = UNKNOWN
        merged = combine(fact, [left, right])
        lo = min(left.int_min, right.int_min) if left.int_min is not None and right.int_min is not None else None
        hi = max(left.int_max, right.int_max) if left.int_max is not None and right.int_max is not None else None
        flo = min(left.float_min, right.float_min) if left.float_min is not None and right.float_min is not None else None
        fhi = max(left.float_max, right.float_max) if left.float_max is not None and right.float_max is not None else None
        # Widen changing loop/branch numeric ranges.  We only need stable sign
        # information; exact iteration counts are deliberately not inferred.
        return replace(
            merged,
            int_min=lo,
            int_max=hi,
            float_min=flo,
            float_max=fhi,
            fresh_result=False,
            local_owned=(left.local_owned and right.local_owned and fact.kind in {'list', 'dict', 'set'}),
        )

    def _merge_local_envs(self, before: dict[str, Proof], after: dict[str, Proof]) -> dict[str, Proof]:
        result = {}
        for name in before.keys() | after.keys():
            result[name] = self._join_local_proof(before.get(name), after.get(name), name)
        return result

    def _widen_loop_env(self, before: dict[str, Proof], after: dict[str, Proof]) -> dict[str, Proof]:
        """Join zero/many iterations and force a small stable abstract state.

        A single abstract iteration is not sound when a later iteration can
        observe a value changed by the previous one (for example a divisor that
        becomes zero).  Numeric bounds that move outward are widened so a second
        pass sees the full recurrent hazard instead of only iteration one.
        """
        result = {}
        for name in before.keys() | after.keys():
            left, right = before.get(name), after.get(name)
            merged = self._join_local_proof(left, right, name)
            if left is not None and right is not None and merged.fact.kind in {'int', 'bool'}:
                lo = None
                hi = None
                if left.int_min is not None and right.int_min is not None:
                    lo = min(left.int_min, right.int_min)
                if left.int_max is not None and right.int_max is not None and left.int_max == right.int_max:
                    hi = left.int_max
                merged = replace(merged, int_min=lo, int_max=hi)
            elif left is not None and right is not None and merged.fact.kind == 'float':
                # Force recurrent numeric ranges to a small stable abstraction.
                # A bound is retained only when it is invariant in the direction
                # that matters; otherwise it is dropped rather than extrapolated.
                lo = None
                hi = None
                if left.float_min is not None and right.float_min is not None:
                    lo = min(left.float_min, right.float_min)
                if left.float_max is not None and right.float_max is not None and left.float_max == right.float_max:
                    hi = left.float_max
                merged = replace(merged, float_min=lo, float_max=hi)
            result[name] = merged
        return result

    def _local_binop(self, op: ast.operator, left: Proof, right: Proof) -> Proof:
        numeric_kind = _exact_numeric_result_kind(left, right)
        if isinstance(op, (ast.Add, ast.Sub, ast.Mult)) and numeric_kind:
            result = combine(INT if numeric_kind == 'int' else TypeFact('float'), [left, right])
            if numeric_kind == 'int':
                lo, hi = _int_bounds_binary(op, left, right)
                result = replace(result, int_min=lo, int_max=hi)
            else:
                lo, hi = _float_bounds_binary(op, left, right)
                result = replace(result, float_min=lo, float_max=hi)
            return result
        return uncertain('local augmented assignment operator/type is outside the exact numeric subset', effect=EffectKind.PURE)

    def _local_list_append(self, call: ast.Call, env: dict[str, Proof], lookup, builtin_ok) -> Proof | None:
        """Apply one exact-list append to uniquely local function state.

        This is intentionally narrower than general method dispatch: the receiver
        must be a simple local name proven to refer to a locally allocated exact
        list, and the appended value must be a pure immutable exact value.
        """
        if not (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == 'append'
            and isinstance(call.func.value, ast.Name)
            and len(call.args) == 1
            and not call.keywords
            and not isinstance(call.args[0], ast.Starred)
        ):
            return None
        name = call.func.value.id
        base = lookup(name)
        if not (base.safe and base.local_owned and base.fact.kind == 'list'):
            return None
        item = self.expression(call.args[0], lookup, builtin_ok=builtin_ok, in_function=True)
        if not (item.safe and item.effect == EffectKind.PURE and item.fact.immutable):
            return None
        if base.fact.items == ():
            element = item.fact
        elif base.fact.element == item.fact:
            element = item.fact
        elif base.fact.element is NONE:
            element = item.fact
        else:
            element = UNKNOWN
        env[name] = replace(
            base,
            fact=TypeFact('list', element=element),
            fresh_result=False,
            allocates_container=False,
            local_owned=True,
        )
        return item

    def _merge_local_exit_envs(self, envs: list[dict[str, Proof]]) -> dict[str, Proof] | None:
        if not envs:
            return None
        merged = dict(envs[0])
        for other in envs[1:]:
            merged = self._merge_local_envs(merged, other)
        return merged

    def _analyze_local_block(
        self,
        statements: list[ast.stmt],
        env: dict[str, Proof],
        builtin_ok,
    ) -> _LocalFlow:
        env = dict(env)
        allocated_container = False
        breaks: list[dict[str, Proof]] = []
        continues: list[dict[str, Proof]] = []

        for statement in statements:
            lookup = lambda name: env.get(
                name, uncertain(f"global/captured or unbound local name {name!r}", effect=EffectKind.PURE)
            )
            if isinstance(statement, ast.Pass):
                continue
            if isinstance(statement, ast.Break):
                breaks.append(dict(env))
                return _LocalFlow(None, allocated_container=allocated_container,
                                  breaks=tuple(breaks), continues=tuple(continues))
            if isinstance(statement, ast.Continue):
                continues.append(dict(env))
                return _LocalFlow(None, allocated_container=allocated_container,
                                  breaks=tuple(breaks), continues=tuple(continues))
            if isinstance(statement, ast.Expr):
                if isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                    continue
                local_append = None
                if isinstance(statement.value, ast.Call):
                    local_append = self._local_list_append(statement.value, env, lookup, builtin_ok)
                if local_append is not None:
                    allocated_container |= local_append.allocates_container
                    continue
                proof = self.expression(statement.value, lookup, builtin_ok=builtin_ok, in_function=True)
                if not proof.safe or proof.effect != EffectKind.PURE:
                    return _LocalFlow(
                        env, uncertain('unproved local expression statement', unknown_calls=proof.unknown_calls),
                        allocated_container, tuple(breaks), tuple(continues))
                allocated_container |= proof.allocates_container
                continue
            if isinstance(statement, ast.Assign):
                proof = self.expression(statement.value, lookup, builtin_ok=builtin_ok, in_function=True)
                if not proof.safe or proof.effect != EffectKind.PURE:
                    return _LocalFlow(
                        env, uncertain('unproved intermediate function statement; remaining body effects and return type are unknown',
                                       unknown_calls=proof.unknown_calls),
                        allocated_container, tuple(breaks), tuple(continues))
                allocated_container |= proof.allocates_container
                if (proof.local_owned and proof.fact.kind in {'list', 'dict', 'set'} and
                        (isinstance(statement.value, ast.Name) or len(statement.targets) != 1)):
                    return _LocalFlow(
                        env, uncertain('aliasing a locally owned mutable container is outside the bounded ownership proof'),
                        allocated_container, tuple(breaks), tuple(continues))
                for target in statement.targets:
                    if not isinstance(target, ast.Name):
                        return _LocalFlow(
                            env, uncertain('function assignment target is outside the local-name subset'),
                            allocated_container, tuple(breaks), tuple(continues))
                    env[target.id] = replace(proof, fresh_result=False, allocates_container=False)
                continue
            if isinstance(statement, ast.AugAssign):
                if not isinstance(statement.target, ast.Name):
                    return _LocalFlow(
                        env, uncertain('augmented assignment may mutate nonlocal/object state'),
                        allocated_container, tuple(breaks), tuple(continues))
                left = lookup(statement.target.id)
                right = self.expression(statement.value, lookup, builtin_ok=builtin_ok, in_function=True)
                proof = self._local_binop(statement.op, left, right)
                if not proof.safe:
                    return _LocalFlow(env, proof, allocated_container, tuple(breaks), tuple(continues))
                allocated_container |= proof.allocates_container
                env[statement.target.id] = proof
                continue
            if isinstance(statement, ast.If):
                condition = self.expression(statement.test, lookup, builtin_ok=builtin_ok, in_function=True)
                if not condition.safe or not condition.fact.passive:
                    return _LocalFlow(
                        env, uncertain('if condition lacks a total passive-value proof'),
                        allocated_container, tuple(breaks), tuple(continues))
                allocated_container |= condition.allocates_container
                body = self._analyze_local_block(statement.body, env, builtin_ok)
                other = self._analyze_local_block(statement.orelse, env, builtin_ok)
                allocated_container |= body.allocated_container or other.allocated_container
                if body.failure:
                    return _LocalFlow(env, body.failure, allocated_container, tuple(breaks), tuple(continues))
                if other.failure:
                    return _LocalFlow(env, other.failure, allocated_container, tuple(breaks), tuple(continues))
                breaks.extend(body.breaks); breaks.extend(other.breaks)
                continues.extend(body.continues); continues.extend(other.continues)
                fallthrough = [candidate for candidate in (body.env, other.env) if candidate is not None]
                merged = self._merge_local_exit_envs(fallthrough)
                if merged is None:
                    return _LocalFlow(None, allocated_container=allocated_container,
                                      breaks=tuple(breaks), continues=tuple(continues))
                env = merged
                continue
            if isinstance(statement, ast.For):
                iterable = self.expression(statement.iter, lookup, builtin_ok=builtin_ok, in_function=True)
                if not iterable.safe:
                    return _LocalFlow(env, uncertain('for iterable lacks a total local proof'), allocated_container,
                                      tuple(breaks), tuple(continues))
                allocated_container |= iterable.allocates_container
                if iterable.fact.kind == 'range':
                    element = Proof(INT, int_min=iterable.int_min, int_max=iterable.int_max)
                elif iterable.fact.flat_immutable_sequence:
                    typ = common_type(list(iterable.fact.items)) if iterable.fact.items else iterable.fact.element
                    if typ is None or not typ.immutable:
                        return _LocalFlow(env, uncertain('for iterable element type is not a proved immutable value'),
                                          allocated_container, tuple(breaks), tuple(continues))
                    element = Proof(typ)
                else:
                    return _LocalFlow(env, uncertain('for loop requires range or an exact immutable list/tuple'),
                                      allocated_container, tuple(breaks), tuple(continues))
                if not isinstance(statement.target, ast.Name):
                    return _LocalFlow(env, uncertain('for target is outside the simple local-name subset'),
                                      allocated_container, tuple(breaks), tuple(continues))
                pre_loop = dict(env)
                loop_state = dict(env)
                converged = False
                body_flow: _LocalFlow | None = None
                for _ in range(4):
                    body_start = dict(loop_state)
                    body_start[statement.target.id] = element
                    body_flow = self._analyze_local_block(statement.body, body_start, builtin_ok)
                    allocated_container |= body_flow.allocated_container
                    if body_flow.failure:
                        return _LocalFlow(env, body_flow.failure, allocated_container, tuple(breaks), tuple(continues))
                    recurrent = ([body_flow.env] if body_flow.env is not None else []) + list(body_flow.continues)
                    recurrent_env = self._merge_local_exit_envs(recurrent)
                    candidate = pre_loop if recurrent_env is None else self._widen_loop_env(pre_loop, recurrent_env)
                    if candidate == loop_state:
                        converged = True
                        loop_state = candidate
                        break
                    loop_state = candidate
                if not converged or body_flow is None:
                    return _LocalFlow(env, uncertain('for-loop abstract state did not converge within the proof budget'),
                                      allocated_container, tuple(breaks), tuple(continues))
                normal_env = loop_state
                normal_exits: list[dict[str, Proof]] = []
                if statement.orelse:
                    else_flow = self._analyze_local_block(statement.orelse, normal_env, builtin_ok)
                    allocated_container |= else_flow.allocated_container
                    if else_flow.failure or else_flow.breaks or else_flow.continues:
                        return _LocalFlow(
                            env, else_flow.failure or uncertain('loop else contains unsupported control transfer', effect=EffectKind.PURE),
                            allocated_container, tuple(breaks), tuple(continues))
                    if else_flow.env is not None:
                        normal_exits.append(else_flow.env)
                else:
                    normal_exits.append(normal_env)
                normal_exits.extend(body_flow.breaks)
                merged = self._merge_local_exit_envs(normal_exits)
                if merged is None:
                    return _LocalFlow(None, allocated_container=allocated_container,
                                      breaks=tuple(breaks), continues=tuple(continues))
                env = merged
                continue
            if isinstance(statement, ast.While):
                pre_loop = dict(env)
                loop_state = dict(env)
                converged = False
                body_flow: _LocalFlow | None = None
                for _ in range(4):
                    loop_lookup = lambda name: loop_state.get(
                        name, uncertain(f"global/captured or unbound local name {name!r}", effect=EffectKind.PURE))
                    condition = self.expression(statement.test, loop_lookup, builtin_ok=builtin_ok, in_function=True)
                    if not condition.safe or not condition.fact.passive:
                        return _LocalFlow(env, uncertain('while condition lacks a total passive-value proof'),
                                          allocated_container, tuple(breaks), tuple(continues))
                    allocated_container |= condition.allocates_container
                    body_flow = self._analyze_local_block(statement.body, loop_state, builtin_ok)
                    allocated_container |= body_flow.allocated_container
                    if body_flow.failure:
                        return _LocalFlow(env, body_flow.failure, allocated_container, tuple(breaks), tuple(continues))
                    recurrent = ([body_flow.env] if body_flow.env is not None else []) + list(body_flow.continues)
                    recurrent_env = self._merge_local_exit_envs(recurrent)
                    candidate = pre_loop if recurrent_env is None else self._widen_loop_env(pre_loop, recurrent_env)
                    if candidate == loop_state:
                        converged = True
                        loop_state = candidate
                        break
                    loop_state = candidate
                if not converged or body_flow is None:
                    return _LocalFlow(env, uncertain('while-loop abstract state did not converge within the proof budget'),
                                      allocated_container, tuple(breaks), tuple(continues))
                normal_env = loop_state
                normal_exits: list[dict[str, Proof]] = []
                if statement.orelse:
                    else_flow = self._analyze_local_block(statement.orelse, normal_env, builtin_ok)
                    allocated_container |= else_flow.allocated_container
                    if else_flow.failure or else_flow.breaks or else_flow.continues:
                        return _LocalFlow(
                            env, else_flow.failure or uncertain('loop else contains unsupported control transfer', effect=EffectKind.PURE),
                            allocated_container, tuple(breaks), tuple(continues))
                    if else_flow.env is not None:
                        normal_exits.append(else_flow.env)
                else:
                    normal_exits.append(normal_env)
                normal_exits.extend(body_flow.breaks)
                merged = self._merge_local_exit_envs(normal_exits)
                if merged is None:
                    return _LocalFlow(None, allocated_container=allocated_container,
                                      breaks=tuple(breaks), continues=tuple(continues))
                env = merged
                continue
            return _LocalFlow(
                env, uncertain(f"{type(statement).__name__} is outside the bounded local-function statement subset"),
                allocated_container, tuple(breaks), tuple(continues))
        return _LocalFlow(env, allocated_container=allocated_container,
                          breaks=tuple(breaks), continues=tuple(continues))

    def _call(self, summary, args, kwargs, builtin_ok):
        if not all(p.fact.passive for p in args + list(kwargs.values())):
            return uncertain("local function receives objects outside the exact passive-data subset")
        signature = summary.node.args
        if signature.vararg or signature.kwarg:
            return uncertain("variadic local functions are outside the proof subset")
        positional = [p.arg for p in signature.posonlyargs + signature.args]
        kwonly = [p.arg for p in signature.kwonlyargs]
        if len(args) > len(positional):
            return uncertain("call may raise TypeError: too many positional arguments")
        env = {name: replace(p, fresh_result=False, allocates_container=False) for name, p in zip(positional, args)}
        for name, proof in kwargs.items():
            if name in env or name not in positional + kwonly or name in {p.arg for p in signature.posonlyargs}:
                return uncertain("call may raise TypeError: duplicate, positional-only, or unknown keyword")
            env[name] = replace(proof, fresh_result=False, allocates_container=False)
        defaults = dict(zip(positional[len(positional)-len(signature.defaults):], signature.defaults))
        defaults.update((name, d) for name, d in zip(kwonly, signature.kw_defaults) if d is not None)
        for name in positional + kwonly:
            if name not in env:
                if name not in defaults:
                    return uncertain("call may raise TypeError: missing required argument")
                env[name] = self.expression(defaults[name], lambda _: uncertain("unresolved default"), builtin_ok=builtin_ok)

        names = Names()
        for statement in summary.node.body:
            names.visit(statement)
        local_names = parameter_names(signature) | set(names.writes)
        local_names -= names.global_names
        outer_builtin_ok = builtin_ok
        builtin_ok = lambda name: name not in local_names and outer_builtin_ok(name)
        key = (summary.id,
               tuple((name, env[name].fact, env[name].int_min, env[name].int_max,
                      env[name].float_min, env[name].float_max, env[name].local_owned,
                      env[name].range_length)
                     for name in positional + kwonly),
               tuple(builtin_ok(n) for n in sorted(_SAFE_FUNCTION_BUILTINS)))
        if key in self.cache:
            return self.cache[key]
        if len(self.cache) >= 4096:
            return uncertain("local-function specialization cache budget exhausted")

        body = summary.node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
            body = body[1:]
        if not body or not isinstance(body[-1], ast.Return):
            result = uncertain('missing final return')
            self.cache[key] = result
            return result

        flow = self._analyze_local_block(body[:-1], env, builtin_ok)
        if flow.failure:
            self.cache[key] = flow.failure
            return flow.failure
        if flow.env is None or flow.breaks or flow.continues:
            result = uncertain('local control transfer escapes its supported loop', effect=EffectKind.PURE)
            self.cache[key] = result
            return result
        env = flow.env
        body_allocated = flow.allocated_container
        lookup = lambda name: env.get(name, uncertain(f"global/captured or unbound local name {name!r}", effect=EffectKind.PURE))
        result = Proof(NONE) if body[-1].value is None else self.expression(
            body[-1].value, lookup, builtin_ok=builtin_ok, in_function=True)
        if result.safe and result.local_owned and result.fact.flat_immutable_sequence:
            # A uniquely local flat container returned from the function is a
            # fresh caller-owned result, not an alias of any input.
            result = replace(result, fresh_result=True)
        result = replace(result, allocates_container=result.allocates_container or body_allocated)
        self.cache[key] = result
        return result

