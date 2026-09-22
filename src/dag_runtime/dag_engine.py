"""Conservative static DAG analysis for a deliberately bounded Python runtime.

Public API: analyze_source(), analyze_file(), DAG, and AnalysisOptions.
This module does not execute/import analyzed code, schedule work, or use Graphviz.
Read DAG_REPORT.md before using this graph as an execution contract.
"""
from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass, field, replace
from pathlib import Path
import tokenize
import warnings

from .dag_model import (
    BindingEvent, Certainty, Characteristics, DAG, Definition, DependencyEdge,
    EdgeKind, EdgeReason, EffectKind, GraphValidationError, ReadinessError, ReadinessState, SourceSpan, TaskNode, Value,
)
from .dag_static import (
    FunctionSummary, Names, Proof, ProofEngine, TypeFact, UNKNOWN,
    characteristics, common_type, summarize, uncertain, NONE,
)

__all__ = ['analyze_source', 'analyze_file', 'AnalysisOptions', 'DAG', 'TaskNode', 'Value',
           'DependencyEdge', 'EdgeReason', 'EdgeKind', 'EffectKind', 'Certainty', 'SourceSpan', 'Characteristics',
           'BindingEvent', 'Definition', 'ReadinessState', 'ReadinessError', 'GraphValidationError']


ASSUMPTIONS = (
    "Fresh ordinary module namespace; standard builtins initially, no preinstalled tracing or namespace hooks.",
    "CERTAIN means proved only within the documented exact-type subset; annotations are not trusted.",
    "No resource-exhaustion, asynchronous signal, tracing, or debugger observations are modeled.",
    "Source-order binding commits, Python exceptions, alias identity, and shared namespace state must be preserved.",
    "Opaque operations and namespace tokens require a native shared execution context; edges are not object copies.",
    "No concurrent external mutation may cross task boundaries; concurrency constructs require native scope execution.",
    "@task is an explicit programmer contract: arguments/return carry observable data and independent calls do not mutate hidden shared state.",
)


OPAQUE_STATEMENT_REASONS = {
    'If': 'conditional branches execute inside one native control-flow region',
    'For': 'dynamic for loop remains one region; no iteration expansion',
    'While': 'dynamic while loop remains one region; no graph back edge',
    'Try': 'exception handling/finally stays in one native region',
    'TryStar': 'exception-group handling stays in one native region',
    'With': 'context-manager entry/exit may have effects and exceptions',
    'AsyncWith': 'async context requires native execution context',
    'AsyncFor': 'async iteration requires native execution context',
    'Import': 'import executes external module code and can change global state',
    'ImportFrom': 'import executes external module code and can change global state',
    'FunctionDef': 'decorators/defaults/annotations or rebinding execute at definition time',
    'AsyncFunctionDef': 'async definition retained as opaque native setup',
    'ClassDef': 'class body, bases, decorators, and metaclass can execute arbitrary code',
    'AugAssign': 'augmented assignment may mutate in-place and invoke user protocols',
    'AnnAssign': 'annotation evaluation/binding retains native Python semantics',
    'Delete': 'deletion may finalize objects, delete attributes, or leave names unbound',
    'Global': 'global declaration refers to shared namespace state',
    'Nonlocal': 'nonlocal declaration refers to enclosing scope state',
    'Match': 'pattern matching may invoke user code and binds names conditionally',
    'Assert': 'assertion can raise and prevent later execution',
    'Raise': 'raise prevents successful completion and blocks downstream work',
}


@dataclass(frozen=True)
class AnalysisOptions:
    max_function_ast_nodes: int = 192
    max_proof_steps: int = 100_000
    max_ast_nodes: int = 250_000

    def __post_init__(self):
        if min(self.max_function_ast_nodes, self.max_proof_steps, self.max_ast_nodes) < 1:
            raise ValueError("Analysis budgets must be positive")


class SourceIndex:
    """UTF-8 byte-column source slicing without splitting the whole file per task."""

    def __init__(self, source: str, filename: str):
        self.source, self.filename = source, filename
        self.lines = source.encode('utf-8').splitlines(keepends=True)

    def span(self, node: ast.AST) -> SourceSpan:
        line, column = getattr(node, 'lineno', 1), getattr(node, 'col_offset', 0)
        decorators = getattr(node, 'decorator_list', ())
        if decorators:
            # AST definition spans start at `def`/`class`; retain all decorators,
            # including @(\n expression\n) where the expression starts later.
            start = decorators[0].lineno - 1
            while start >= 0:
                raw = self.lines[start]
                if raw[column:].startswith(b'@'):
                    line = start + 1
                    break
                start -= 1
        return SourceSpan(self.filename, line, column,
                          getattr(node, 'end_lineno', None) or len(self.lines) or 1,
                          getattr(node, 'end_col_offset', None) or 0)

    def text(self, node: ast.AST) -> str:
        s = self.span(node)
        if not self.lines:
            return ''
        if s.line == s.end_line:
            return self.lines[s.line - 1][s.column:s.end_column].decode('utf-8')
        return b''.join([self.lines[s.line - 1][s.column:],
                         *self.lines[s.line:s.end_line - 1],
                         self.lines[s.end_line - 1][:s.end_column]]).decode('utf-8')


@dataclass
class _Task:
    id: str
    node: ast.AST
    kind: str
    label: str
    callable_repr: str | None
    source: str
    proof: Proof
    stats: Characteristics
    inputs: dict[str, None] = field(default_factory=dict)
    outputs: list[str] = field(default_factory=list)
    dependencies: set[str] = field(default_factory=set)
    dependents: set[str] = field(default_factory=set)
    placement: str = 'shared_namespace'
    fence_generation: int = 0
    namespace_epoch: int = 0
    mutated_objects: tuple[str, ...] = ()
    region_scope: str | None = None
    statement_count: int = 1


@dataclass(frozen=True)
class _Binding:
    value_id: str
    fact: TypeFact
    epoch: int


@dataclass
class _Object:
    """A fresh flat list and direct aliases, not a general points-to graph."""
    fact: TypeFact
    state: str | None = None
    readers: dict[str, None] = field(default_factory=dict)
    local_mutation_eligible: bool = True


@dataclass(frozen=True)
class _DefinitionWithDependencies(Definition):
    # FIXES F30: the base record lives in dag_model.py, outside this finding's
    # file scope. Extend it here without changing the frozen DAG record API.
    dependency_ids: tuple[str, ...] = ()


def definition_closures(definitions):
    """Resolve global function references, including cycles, without execution."""
    by_name = {d.name: d.id for d in definitions}
    direct = {d.id: {by_name[n] for n in d.free_names if n in by_name}
              for d in definitions}
    result = {}
    for definition in definitions:
        seen, todo = set(), list(direct[definition.id])
        while todo:
            ident = todo.pop()
            if ident in seen:
                continue
            seen.add(ident)
            todo.extend(direct[ident] - seen)
        result[definition.id] = tuple(d.id for d in definitions
                                      if d.id in seen and d.id != definition.id)
    return result


class _Analyzer:
    def __init__(self, source: str, filename: str, options: AnalysisOptions):
        self.source, self.filename, self.options = source, filename, options
        self.index = SourceIndex(source, filename)
        self.tasks: dict[str, _Task] = {}
        self.values: dict[str, Value] = {}
        self.edges: dict[tuple[str, str], dict[EdgeReason, None]] = {}
        self.env: dict[str, _Binding] = {}
        self.versions: dict[str, int] = {}
        self.bindings: list[BindingEvent] = []
        self.definitions: list[Definition] = []
        self.summaries: dict[str, FunctionSummary] = {}
        self.prover = ProofEngine(self.summaries, budget=options.max_proof_steps)
        self.frontier: dict[str, None] = {}
        self.epoch = 0
        self.state: str | None = None
        self.namespace_state: str | None = None
        self.last_barrier: str | None = None
        self.fence_generation = 0
        self.objects: dict[str, _Object] = {}
        self.diagnostics: list[str] = []
        self.task_markers: dict[str, str] = {}

    def _value(self, name: str, producer: str | None, origin: str, fact: TypeFact = UNKNOWN, *,
               alias_of: str | None = None, projection=(), maybe_unbound=False,
               object_id: str | None = None) -> str:
        version = 0 if origin == 'external' and name not in self.versions else self.versions.get(name, 0) + 1
        self.versions[name] = version
        ident = f'V{len(self.values) + 1:06d}'
        storage = ('definition' if fact.kind in {'function', 'builtin'} else
                   'immutable_value' if fact.immutable else
                   'shared_reference' if fact.passive else 'native_namespace')
        value = Value(ident, name, version, producer, origin, fact.describe(), object_id or ident,
                      alias_of, tuple(projection), maybe_unbound, fact.function_id, storage)
        self.values[ident] = value
        if producer is not None:
            self.tasks[producer].outputs.append(ident)
        return ident

    def _read(self, name: str) -> str:
        binding = self.env.get(name)
        if binding is not None and binding.epoch == self.epoch:
            return binding.value_id
        if self.last_barrier is not None:
            # Lazy namespace projection: old binding may have been replaced/deleted.
            ident = self._value(name, self.last_barrier, 'namespace', projection=(name,), maybe_unbound=True)
        elif name in {'len', 'sum', 'abs', 'min', 'max', 'range'}:
            fact = TypeFact('builtin')
            ident = self._value(name, None, 'builtin', fact)
            self.env[name] = _Binding(ident, fact, self.epoch)
            return ident
        else:
            ident = self._value(name, None, 'external', maybe_unbound=True)
        self.env[name] = _Binding(ident, UNKNOWN, self.epoch)
        return ident

    def _lookup(self, name: str) -> Proof:
        ident = self._read(name)
        if self.values[ident].may_be_unbound:
            return uncertain(f"name {name!r} may be unbound or changed by an opaque operation")
        return Proof(self._fact(self.env[name]))

    def _fact(self, binding: _Binding) -> TypeFact:
        obj = self.objects.get(self.values[binding.value_id].object_id)
        return obj.fact if obj is not None else binding.fact

    def _builtin_ok(self, name: str) -> bool:
        if self.epoch != 0:
            return False
        binding = self.env.get(name)
        return binding is None or self.values[binding.value_id].origin == 'builtin'

    def _edge(self, a: str, b: str, reason: EdgeReason):
        if a == b:
            raise AssertionError("Analyzer attempted a self edge")
        self.edges.setdefault((a, b), {})[reason] = None
        self.tasks[b].dependencies.add(a)
        self.tasks[a].dependents.add(b)

    def _input(self, task: _Task, value_id: str):
        if value_id in task.inputs:
            return
        task.inputs[value_id] = None
        value = self.values[value_id]
        if value.producer:
            state = value.origin in {'state', 'namespace', 'object_state'}
            object_state = value.origin == 'object_state'
            reason = EdgeReason(EdgeKind.STATE if state else EdgeKind.DATA,
                                Certainty.CONSERVATIVE if state and not object_state else Certainty.CERTAIN,
                                f"{task.id} reads {value.label} produced by {value.producer}" +
                                (f"; exact list object {value.object_id} state must be preserved" if object_state else
                                 f"; namespace/completion state in epoch {self.epoch} must be preserved" if state else ''), value.id)
            self._edge(value.producer, task.id, reason)

    def _new_task(self, node, proof: Proof, reads, *, kind='expression', label=None,
                  callable_repr=None, source=None, stats=None, write_object=None) -> _Task:
        ident = f'T{len(self.tasks) + 1:06d}'
        stats = stats or characteristics(node)
        stats = replace(stats, unknown_calls=proof.unknown_calls,
                        possible_side_effects=proof.effect != EffectKind.PURE, may_raise=proof.may_raise,
                        complexity=stats.complexity if proof.safe else 'unknown')
        task = _Task(ident, node, kind, label or type(node).__name__, callable_repr,
                     self.index.text(node) if source is None else source, proof, stats)
        self.tasks[ident] = task
        touched = {}
        for value_id in reads:
            self._input(task, value_id)
            object_id = self.values[value_id].object_id
            if object_id in self.objects:
                touched[object_id] = self.objects[object_id]
        for object_id, obj in touched.items():
            if obj.state:
                self._input(task, obj.state)
            if object_id == write_object:
                for reader in obj.readers:
                    self._edge(reader, ident, EdgeReason(EdgeKind.STATE, Certainty.CERTAIN,
                        f'{ident} mutates exact list object {object_id}; prior reader {reader} must finish (WAR)'))
                obj.readers.clear()
            else:
                obj.readers[ident] = None
        # Every task stamped with generation g is transitively after fence g.
        # Keep DATA inputs even when redundant; omit only an unnecessary fence
        # requirement, never a value that the computation actually reads.
        if self.state is not None and not any(
            self.tasks[p].fence_generation == self.fence_generation for p in task.dependencies
        ):
            self._input(task, self.state)
        if not proof.safe:
            why = '; '.join(proof.reasons) or 'possible exception must preserve source order'
            for previous in self.frontier:
                self._edge(previous, ident, EdgeReason(
                    EdgeKind.ORDER, Certainty.CONSERVATIVE,
                    f"conservative barrier preserves earlier reads/writes/effects before {ident}: {why}"))
            self.frontier.clear()
            self.fence_generation += 1
            if proof.effect in {EffectKind.NAMESPACE, EffectKind.ESCAPE}:
                self.epoch += 1
                self.last_barrier = ident
                self.objects.clear()  # each registered object is retired once
                self.state = self._value('@namespace', ident, 'state')
                self.namespace_state = self.state
            else:
                self.state = self._value('@completion', ident, 'state')
        else:
            for parent in task.dependencies:
                self.frontier.pop(parent, None)
        self.frontier[ident] = None
        task.fence_generation = self.fence_generation
        task.namespace_epoch = self.epoch
        if write_object is not None:
            task.mutated_objects = (write_object,)
            state = self._value('@object:' + write_object, ident, 'object_state', object_id=write_object)
            self.objects[write_object].state = state
        return task

    def _bind(self, name, value_id, fact, node, kind='assignment'):
        previous = self.env.get(name)
        producer = self.values[value_id].producer
        if previous is not None and producer is not None:
            # A persistent namespace must capture prior aliases/reads before a
            # newer SSA version overwrites the same Python name (WAR ordering).
            for reader in self.tasks.values():
                if reader.id != producer and previous.value_id in reader.inputs:
                    self._edge(reader.id, producer, EdgeReason(
                        EdgeKind.ORDER, Certainty.CERTAIN,
                        f"capture prior binding of {name!r} before rebinding"))
        marker = self.task_markers.get(name)
        if marker is not None and marker != value_id:
            self.task_markers.pop(name, None)
        self.env[name] = _Binding(value_id, fact, self.epoch)
        self.bindings.append(BindingEvent(name, value_id, self.index.span(node), kind))

    def _overwrites(self, names) -> tuple[str, ...]:
        reasons = []
        for name in names:
            if name not in self.env and self.epoch:
                reasons.append(f"opaque code may have introduced {name!r}; namespace rebinding/finalization is unproved")
            elif name in self.env:
                self._read(name)
                if not self._fact(self.env[name]).passive:
                    reasons.append(f"rebinding {name!r} may release an object with finalization effects")
        return tuple(dict.fromkeys(reasons))

    def _runtime_task_import(self, node) -> bool:
        """Recognize the runtime's own no-op @task marker import.

        This is a language/runtime intrinsic, not a general claim that imports
        are pure.  Any other import keeps the existing conservative behavior.
        """
        if not (isinstance(node, ast.ImportFrom) and node.level == 0 and
                node.module == 'dag_runtime' and len(node.names) == 1 and
                node.names[0].name == 'task'):
            return False
        alias = node.names[0]
        name = alias.asname or alias.name
        if self._overwrites([name]):
            return False
        fact = TypeFact('builtin')
        ident = self._value(name, None, 'builtin', fact)
        self._bind(name, ident, fact, node, 'runtime_task_import')
        self.task_markers[name] = ident
        return True

    def _task_decorator_name(self, node) -> str | None:
        if not isinstance(node, ast.FunctionDef) or len(node.decorator_list) != 1:
            return None
        decorator = node.decorator_list[0]
        if not isinstance(decorator, ast.Name):
            return None
        marker = self.task_markers.get(decorator.id)
        binding = self.env.get(decorator.id)
        return decorator.id if marker is not None and binding is not None and binding.value_id == marker else None

    def _plain_definition(self, node) -> bool:
        if not isinstance(node, ast.FunctionDef):
            return False
        task_decorator = self._task_decorator_name(node)
        decorators_ok = not node.decorator_list or task_decorator is not None
        if (node.name == '__builtins__' or
                not decorators_ok or node.returns or getattr(node, 'type_params', [])):
            return False
        args = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs, node.args.vararg, node.args.kwarg)
        if any(arg is not None and arg.annotation is not None for arg in args):
            return False
        defaults = [*node.args.defaults, *(v for v in node.args.kw_defaults if v is not None)]
        return all(isinstance(d, ast.Constant) and type(d.value) in (type(None), bool, int, float, complex, str, bytes)
                   for d in defaults) and not self._overwrites([node.name])

    def _definition(self, node):
        ident = f'D{len(self.definitions) + 1:06d}'
        explicit_task = self._task_decorator_name(node) is not None
        summary = summarize(ident, node, self.options.max_function_ast_nodes, explicit_task=explicit_task)
        self.summaries[ident] = summary
        self.definitions.append(_DefinitionWithDependencies(ident, node.name, self.index.span(node), self.index.text(node),
                                           summary.free_names, summary.eligible, summary.reason, summary.stats))
        fact = TypeFact('function', function_id=ident)
        value_id = self._value(node.name, None, 'definition', fact)
        self._bind(node.name, value_id, fact, node, 'task_definition' if explicit_task else 'definition')

    def _reads(self, names: Names, *, region: bool = False) -> list[str]:
        names_to_read = dict(names.reads)
        if region:
            # Region-local stores are not mandatory external inputs. Native region
            # execution decides which conditional bindings actually exist.
            names_to_read = {n: None for n in names_to_read if n not in names.writes or n in self.env}
            for name in names.writes:
                if name in self.env:
                    names_to_read[name] = None  # branch/loop may preserve old binding
        values = dict.fromkeys(self._read(n) for n in names_to_read)
        # Include late-bound globals used by a directly referenced known function.
        # Further unresolved/nested calls are covered by the whole namespace token.
        for name in names.calls:
            binding = self.env.get(name)
            summary = self.summaries.get(binding.fact.function_id) if binding else None
            if summary:
                for free in summary.free_names:
                    # F58 code-reality correction: explicit @task keeps its
                    # programmer contract for unresolved hidden effects/names,
                    # but a free name already bound in this module is ordinary
                    # Python state and must become a real data dependency before
                    # the call can move to another process.
                    if summary.explicit_task and free not in self.env:
                        continue
                    value_id = self._read(free)
                    values[value_id] = None
        return list(values)

    def _targets(self, target, fact, path=()):
        """Return (name, fact, projection) records and unpacking uncertainty."""
        if isinstance(target, ast.Name):
            return [(target.id, fact, path)], ()
        if isinstance(target, (ast.Tuple, ast.List)):
            parts = target.elts
            stars = [i for i, t in enumerate(parts) if isinstance(t, ast.Starred)]
            shape = fact.items if fact.kind in {'list', 'tuple'} else None
            exact = shape is not None and (len(shape) == len(parts) if not stars else len(shape) >= len(parts)-1)
            reasons = () if exact else ('unpacking length/iteration may raise or invoke user protocols',)
            output = []
            for i, part in enumerate(parts):
                suffix: int | str = i
                child_fact = UNKNOWN
                if isinstance(part, ast.Starred):
                    suffix = f'{i}:*'
                    if exact:
                        count = len(shape) - len(parts) + 1
                        children = shape[i:i+count]
                        child_fact = TypeFact('list', items=children)
                    part = part.value
                elif exact:
                    offset = i if not stars or i < stars[0] else len(shape)-len(parts)+i
                    child_fact = shape[offset]
                values, child_reasons = self._targets(part, child_fact, path+(suffix,))
                output.extend(values)
                reasons += child_reasons
            return output, tuple(dict.fromkeys(reasons))
        if isinstance(target, ast.Starred):
            return self._targets(target.value, UNKNOWN, path+('*',))
        return [], ('attribute/subscript assignment mutates shared state and may invoke user code',)

    def _append(self, node, reads) -> bool:
        """Only an atomic exact-list append with a proven immutable argument.

        No removal/finalization, iteration, hashing, descriptor override, or
        ordinary exception is possible in this subset. Resource failures remain
        outside the existing execution contract.
        """
        if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
            return False
        call = node.value
        if (not isinstance(call.func, ast.Attribute) or call.func.attr != 'append' or
                not isinstance(call.func.value, ast.Name) or len(call.args) != 1 or call.keywords):
            return False
        binding = self.env.get(call.func.value.id)
        if binding is None or binding.epoch != self.epoch:
            return False
        object_id = self.values[binding.value_id].object_id
        obj = self.objects.get(object_id)
        if obj is None or not obj.local_mutation_eligible or not obj.fact.flat_immutable_sequence:
            return False
        arg = self.prover.expression(call.args[0], self._lookup, builtin_ok=self._builtin_ok)
        if not arg.safe or arg.effect != EffectKind.PURE or not arg.fact.immutable:
            return False
        task = self._new_task(node, Proof(NONE, effect=EffectKind.OBJECT_LOCAL), reads,
                              kind='object_mutation', label=f'{call.func.value.id}.append()',
                              callable_repr=self.index.text(call.func), write_object=object_id)
        items = obj.fact.items
        if items is not None:
            children = [*items, arg.fact]
            updated = TypeFact('list', element=common_type(children),
                               items=tuple(children) if len(children) <= 32 else None)
        else:
            updated = TypeFact('list', element=common_type([obj.fact.element or UNKNOWN, arg.fact]))
        obj.fact = updated
        self._value('@discard', task.id, 'result', NONE)
        return True

    def statement(self, node):
        if isinstance(node, ast.Pass):
            return
        if self._runtime_task_import(node):
            return
        if self._plain_definition(node):
            self._definition(node)
            return
        names = Names()
        names.visit(node)
        if '__builtins__' in names.writes:
            self._region(node, names, 'rebinding __builtins__ changes the environment captured by future functions')
            return
        if not isinstance(node, (ast.Assign, ast.Expr)):
            node_type = type(node).__name__
            reason = OPAQUE_STATEMENT_REASONS.get(node_type, f'{node_type} needs opaque native execution')
            self._region(node, names, reason)
            return
        reads = self._reads(names)
        if self._append(node, reads):
            return
        expression = node.value
        proof = self.prover.expression(expression, self._lookup, builtin_ok=self._builtin_ok)
        if self.epoch and proof.allocates_container:
            # Unknown code can leave cyclic garbage with __del__ or install GC
            # callbacks. A later container allocation can run that code even
            # without reading an old name. Do not call it isolated/pure merely
            # because all of its explicit elements are literals.
            proof = uncertain('container allocation after namespace effects may trigger unbounded GC/finalization callbacks')
        targets = node.targets if isinstance(node, ast.Assign) else []
        outputs, target_reasons = [], ()
        for target in targets:
            outs, reasons = self._targets(target, proof.fact)
            outputs.extend(outs)
            target_reasons += reasons
        target_reasons += self._overwrites(name for name, _, _ in outputs)
        if target_reasons:
            # Target protocols/finalizers can change earlier bindings in the same
            # assignment. Treat all named outputs as conditional namespace views.
            self._region(node, names, '; '.join(dict.fromkeys(proof.reasons + target_reasons)))
            return
        if names.global_names:
            proof = uncertain('global/nonlocal namespace effects')
        # A plain alias requires a definitely bound reference, not a purity proof
        # of the referenced object. It performs no call, copy, or object mutation.
        if (isinstance(node, ast.Assign) and isinstance(expression, ast.Name) and
                all(isinstance(t, ast.Name) for t in targets) and proof.safe):
            source_id = self._read(expression.id)
            base = self.values[source_id]
            # F24: a source-order alias has a real namespace owner. The frozen
            # DAG alias_of invariant requires identical producers, so a binding
            # task uses its own output ID and retains the original object_id.
            owner = self._new_task(node, proof, [source_id], kind='binding', label='alias binding')
            for target in targets:
                ident = self._value(target.id, owner.id, 'alias', proof.fact,
                                    object_id=base.object_id, projection=base.projection)
                self._bind(target.id, ident, proof.fact, node, 'alias')
            return
        # Names bound inside walrus expressions are part of this atomic region.
        if isinstance(expression, ast.NamedExpr) or any(isinstance(x, ast.NamedExpr) for x in ast.walk(expression)):
            self._region(node, names, 'assignment expression retains internal namespace updates')
            return
        callable_repr = self.index.text(expression.func) if isinstance(expression, ast.Call) else None
        label = f'{callable_repr}()' if callable_repr else type(expression).__name__
        kind = 'call' if callable_repr else 'expression'
        if not proof.safe:
            kind = 'opaque_expression'
        stats = self._call_characteristics(node, expression)
        input_facts = [self._fact(self.env[name]) for name in names.reads if name in self.env]
        task = self._new_task(node, proof, reads, kind=kind, label=label, callable_repr=callable_repr, stats=stats)
        self._publish_outputs(task, node, outputs, proof)
        self._configure_placement(task, proof, reads, input_facts)

    def _call_characteristics(self, node: ast.AST, expression: ast.AST) -> Characteristics:
        """Merge a direct local call's static summary into statement metadata."""
        stats = characteristics(node)
        if not (isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name)):
            return stats

        binding = self.env.get(expression.func.id)
        summary = self.summaries.get(binding.fact.function_id) if binding else None
        if summary is None:
            return stats

        ast_nodes = stats.ast_nodes + summary.stats.ast_nodes
        return replace(
            stats,
            loop_count=summary.stats.loop_count,
            max_loop_depth=summary.stats.max_loop_depth,
            comprehension_count=stats.comprehension_count + summary.stats.comprehension_count,
            call_count=stats.call_count + summary.stats.call_count,
            ast_nodes=ast_nodes,
            complexity=(
                'complex'
                if ast_nodes > 80 or summary.stats.loop_count or summary.stats.comprehension_count
                else stats.complexity
            ),
        )

    def _publish_outputs(self, task: _Task, node: ast.AST, outputs, proof: Proof) -> None:
        """Publish statement outputs without mixing binding logic into statement analysis."""
        groups: dict[tuple, str] = {}
        successful_binding = proof.effect == EffectKind.PURE

        for name, fact, projection in outputs:
            fact = fact if successful_binding else UNKNOWN
            ident = self._value(
                name,
                task.id,
                'result' if successful_binding else 'namespace',
                fact,
                projection=projection if successful_binding else (name,),
                maybe_unbound=not successful_binding,
                object_id=groups.get(projection) if successful_binding else None,
            )
            groups.setdefault(projection, self.values[ident].object_id)
            self._bind(name, ident, fact, node)

            if (
                proof.safe
                and proof.fresh_result
                and not projection
                and fact.kind == 'list'
                and fact.flat_immutable_sequence
            ):
                self.objects.setdefault(self.values[ident].object_id, _Object(fact))

        if not outputs and isinstance(node, ast.Expr):
            self._value('@discard', task.id, 'result', proof.fact if proof.safe else UNKNOWN)

    def _configure_placement(
        self,
        task: _Task,
        proof: Proof,
        reads: list[str],
        input_facts: list[TypeFact],
    ) -> None:
        """Set placement metadata and retire unsafe local-mutation assumptions."""
        if not proof.safe:
            return

        # A fresh flat result cannot alias a mutable input. The proven body has
        # no identity-sensitive operation, mutation, or user protocols.
        independent_result = (
            proof.fact.immutable
            or proof.fact.kind == 'task_payload'
            or proof.fresh_result and proof.fact.flat_immutable_sequence
        )
        inputs_are_isolatable = all(
            fact.immutable
            or fact.flat_immutable_sequence
            or fact.kind in {'function', 'builtin', 'task_payload'}
            for fact in input_facts
        )
        task.placement = (
            'isolated_candidate'
            if independent_result and inputs_are_isolatable
            else 'shared_namespace'
        )

        if independent_result:
            return

        # A returned alias or container holding a mutable input could expose an
        # alias we do not index. Future mutation therefore fails closed.
        for value_id in reads:
            obj = self.objects.get(self.values[value_id].object_id)
            if obj is not None:
                obj.local_mutation_eligible = False

    def _region(self, node, names, reason, *, source=None, effect=EffectKind.NAMESPACE, region_scope='statement'):
        reads = self._reads(names, region=True)
        stats = characteristics(node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Function body is descriptive metadata, not definition-time execution.
            stats = characteristics(node, include_body=False)
        proof = uncertain(reason, unknown_calls=bool(stats.call_count), effect=effect)
        expression = node.value if isinstance(node, (ast.Assign, ast.Expr)) else None
        callee = self.index.text(expression.func) if isinstance(expression, ast.Call) else None
        label = f'{callee}() / region' if callee else f'{type(node).__name__} region'
        task = self._new_task(node, proof, reads, kind='opaque_region', label=label,
                              callable_repr=callee, source=source, stats=stats)
        task.region_scope = region_scope
        task.statement_count = len(node.body) if isinstance(node, ast.Module) else 1
        for name in names.writes:
            ident = self._value(name, task.id, 'namespace', projection=(name,), maybe_unbound=True)
            self._bind(name, ident, UNKNOWN, node, 'region')

    def build(self, tree) -> DAG:
        for i, statement in enumerate(tree.body):
            if (i == 0 and isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant)
                    and isinstance(statement.value.value, str)):
                fact = TypeFact('str')
                task = self._new_task(statement, Proof(fact), [], kind='module_docstring', label='module __doc__')
                value_id = self._value('__doc__', task.id, 'result', fact)
                self._bind('__doc__', value_id, fact, statement)
                task.placement = 'isolated_candidate'
                continue
            names = Names()
            names.visit(statement)
            if names.escape_reasons:
                # Reflection can expose live dictionaries/frames: preserve its tail
                # as a single native region instead of pretending to snapshot it.
                tail = ast.Module(body=tree.body[i:], type_ignores=[])
                # Module._attributes is empty: ast.copy_location does not set
                # these synthetic span fields and would accidentally retain the
                # already analyzed prefix in the task source.
                start = self.index.span(statement)
                tail.lineno = start.line
                tail.col_offset = start.column
                tail.end_lineno = tree.body[-1].end_lineno
                tail.end_col_offset = tree.body[-1].end_col_offset
                combined = Names()
                combined.visit(tail)
                self._region(tail, combined,
                             'dynamic namespace tail collapsed: ' + '; '.join(names.escape_reasons),
                             source=self.index.text(tail), effect=EffectKind.ESCAPE, region_scope='tail')
                break
            self.statement(statement)
        # Resolve final namespace views once, including possible hidden rebinding.
        final = {name: self._read(name) for name in list(self.env)
                 if self.values[self.env[name].value_id].origin not in {'external', 'builtin'}}
        return self.finish(final)

    def finish(self, final=None, *, executable=True):
        closures = definition_closures(self.definitions)
        self.definitions = [replace(d, dependency_ids=closures[d.id]) for d in self.definitions]
        tasks = []
        for task in self.tasks.values():
            tasks.append(TaskNode(
                task.id, task.kind, task.label, task.callable_repr, self.index.span(task.node), task.source,
                type(task.node).__name__, tuple(task.inputs), tuple(task.outputs),
                frozenset(task.dependencies), frozenset(task.dependents),
                Certainty.CERTAIN if task.proof.safe else Certainty.CONSERVATIVE,
                task.proof.reasons,
                ('Explicit @task programmer contract; hidden shared effects/aliases are forbidden by declaration.'
                 if task.proof.explicit_task_contract else
                 'Exact list append; immutable contents/argument, tracked aliases and object-state ordering.'
                 if task.proof.effect == EffectKind.OBJECT_LOCAL else
                 'Exact built-in operations / bounded local body; no unproved effects or ordinary exceptions.') if task.proof.safe else '',
                task.stats, task.placement, executable, task.proof.effect, task.namespace_epoch,
                task.mutated_objects, task.region_scope, task.statement_count))
        return DAG(tasks, self.values.values(),
                   [DependencyEdge(a, b, tuple(reasons)) for (a, b), reasons in self.edges.items()],
                   final_bindings=final or {}, bindings=self.bindings, definitions=self.definitions,
                   diagnostics=self.diagnostics, source=self.source, filename=self.filename,
                   execution_permitted=executable, assumptions=ASSUMPTIONS, final_namespace=self.namespace_state,
                   final_object_states={ident: obj.state for ident, obj in self.objects.items() if obj.state is not None})


def _fallback(source, filename, options, reason, *, executable, statement_count=1):
    analyzer = _Analyzer(source, filename, options)
    node = ast.Module(body=[], type_ignores=[])
    node.lineno = 1
    node.col_offset = 0
    node.end_lineno = len(analyzer.index.lines) or 1
    node.end_col_offset = len(analyzer.index.lines[-1].rstrip(b'\r\n')) if analyzer.index.lines else 0
    analyzer.diagnostics.append(reason)
    task = analyzer._new_task(node, uncertain(reason, effect=EffectKind.ESCAPE), [],
                             kind='opaque_module' if executable else 'invalid_source',
                             label='native module' if executable else 'invalid Python source', source=source)
    task.region_scope = 'module'
    task.statement_count = statement_count
    return analyzer.finish(executable=executable)


def analyze_source(source: str, *, filename: str = '<source>', options: AnalysisOptions | None = None) -> DAG:
    """Analyze a module without executing it; malformed code returns a diagnostic DAG.

    Nested calls stay inside their containing statement. Functions are definitions,
    not entrypoints; their calls can receive a bounded, argument-type-specific proof.
    """
    if not isinstance(source, str):
        raise TypeError('source must be str')
    options = options or AnalysisOptions()
    try:
        tree = ast.parse(source, filename=filename)
        # ast.parse alone accepts e.g. a module-level return; check compiler scopes.
        # compile creates a code object only. It never evaluates/imports source.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', SyntaxWarning)
            compile(tree, filename, 'exec', dont_inherit=True)
    except (SyntaxError, ValueError) as error:
        return _fallback(source, filename, options, f'{type(error).__name__}: {error}', executable=False)
    except (RecursionError, OverflowError) as error:
        return _fallback(source, filename, options, f'parser/compiler resource limit: {error}', executable=False)
    native_context = False
    for count, part in enumerate(ast.walk(tree), start=1):
        if count > options.max_ast_nodes:
            return _fallback(source, filename, options, 'AST analysis budget exceeded; use native module execution',
                             executable=True, statement_count=len(tree.body))
        if isinstance(part, (ast.AsyncFunctionDef, ast.Await, ast.Yield, ast.YieldFrom,
                             ast.GeneratorExp, ast.AsyncFor, ast.AsyncWith)):
            native_context = True
        if isinstance(part, (ast.Import, ast.ImportFrom)):
            modules = [a.name for a in part.names] if isinstance(part, ast.Import) else [part.module or '']
            if any(n.split('.')[0] in {'threading', '_thread', 'asyncio', 'multiprocessing', 'concurrent'} for n in modules):
                native_context = True
    if native_context:
        return _fallback(source, filename, options,
                         'async/generator/concurrency constructs require one native module context; no distributed expansion',
                         executable=True, statement_count=len(tree.body))
    try:
        return _Analyzer(source, filename, options).build(tree)
    except RecursionError:
        return _fallback(source, filename, options, 'analysis nesting budget exceeded; use native module execution',
                         executable=True, statement_count=len(tree.body))


def analyze_file(path: str | Path, *, options: AnalysisOptions | None = None) -> DAG:
    """Respect Python encoding cookies; ordinary I/O errors remain I/O errors."""
    path = Path(path)
    with tokenize.open(path) as handle:
        source = handle.read()
    return analyze_source(source, filename=str(path), options=options)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--json', type=Path, help='write graph records and reasons as JSON')
    args = parser.parse_args(argv)
    dag = analyze_file(args.file)
    if args.json:
        args.json.write_text(json.dumps(dag.to_dict(), indent=2, ensure_ascii=False)+'\n', encoding='utf-8')
    print(f'{len(dag.tasks)} tasks, {len(dag.values)} values, {len(dag.edges)} dependency pairs')
    print('initially ready:', ', '.join(dag.initial_ready_tasks()) or '(none)')
    for task in dag.tasks.values():
        print(f'{task.id} {task.label} [{task.certainty.value}] <- {", ".join(sorted(task.dependencies)) or "(none)"}')
        for reason in task.conservative_reasons:
            print(f'  reason: {reason}')
    for message in dag.diagnostics:
        print(message)
    return 0 if dag.execution_permitted else 2


if __name__ == '__main__':
    raise SystemExit(main())
