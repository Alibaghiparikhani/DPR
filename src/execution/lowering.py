"""DAG -> execution manifest projection. No AST proof, code execution or dispatch."""
from __future__ import annotations

import hashlib
import ast

from dag_runtime.dag_engine import definition_closures

from dag_runtime.dag_model import DAG, GraphValidationError

from .model import (
    CodeRequirement, ExecutionPlan, ExecutionValidationError, ProgramIdentity,
    TaskManifest, ValueRequirement, _graph_digest,
)


def lower_dag(dag: DAG, *, environment_id: str, package_id: str | None = None) -> ExecutionPlan:
    """Lower a validated runnable DAG without changing its graph or readiness.

    environment_id is an explicit caller-owned compatibility key (for example a
    pinned interpreter/build + dependency-lock digest), never auto-detected from
    the lowering host. package_id optionally identifies separately supplied code.
    Neither identifier causes packaging, imports, installation or distribution.
    """
    try:
        dag.validate()
    except GraphValidationError as error:
        raise ExecutionValidationError(f"Invalid input DAG: {error}") from error
    if not dag.execution_permitted:
        raise ExecutionValidationError("Diagnostic/non-runnable DAG cannot become an execution plan")
    program = ProgramIdentity(hashlib.sha256(dag.source.encode("utf-8")).hexdigest(),
                              dag.filename, environment_id, package_id)
    requirements = {v.id: ValueRequirement(v) for v in dag.values.values()}
    incoming = {tid: [] for tid in dag.tasks}
    for edge in dag.edges.values():
        incoming[edge.target].append(edge)
    manifests = []
    for task in dag.tasks.values():
        inputs = tuple(requirements[v] for v in task.inputs)
        # Resolve against definitions visible at this statement, not definitions
        # introduced later. Forward helper references become valid at call time.
        available = tuple(d for d in dag.definitions.values()
                          if d.span.line < task.span.line)
        by_name = {d.name: d.id for d in available}
        needed = {v.value.definition_id for v in inputs if v.value.definition_id}
        needed.update(by_name[n.id] for n in ast.walk(ast.parse(task.source))
                      if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                      and n.id in by_name)
        closures = definition_closures(available)
        for ident in tuple(needed):
            needed.update(closures.get(ident, ()))
        definition_ids = tuple(d.id for d in dag.definitions.values() if d.id in needed)
        manifests.append(TaskManifest(task, CodeRequirement(program.id, definition_ids), inputs,
                                      tuple(requirements[v] for v in task.outputs), tuple(incoming[task.id]),
                                      tuple(b for b in dag.bindings
                                            if (b.kind in {'definition', 'task_definition', 'runtime_task_import'}
                                                and b.span.line < task.span.line)
                                            or (b.kind == 'alias' and b.value_id in task.outputs))))
    return ExecutionPlan(
        program=program, source=dag.source, dag_digest=_graph_digest(dag), tasks=tuple(manifests),
        values=tuple(dag.values.values()), edges=tuple(dag.edges.values()),
        definitions=tuple(dag.definitions.values()), bindings=dag.bindings,
        final_bindings=dag.final_bindings, final_namespace=dag.final_namespace,
        final_object_states=dag.final_object_states, assumptions=dag.assumptions, diagnostics=dag.diagnostics,
    )

