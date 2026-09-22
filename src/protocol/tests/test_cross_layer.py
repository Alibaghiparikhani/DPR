from dataclasses import replace
import json

import pytest
import protocol as p
from dag_runtime.dag_engine import analyze_source
from execution import (
    AttemptIdentity,
    ExecutionMode,
    ExecutionValidationError,
    FailureInfo,
    FailureKind,
    TaskFailure,
    TaskSuccess,
    ValueKind,
    ValueRequirement,
    lower_dag,
)
from scheduler import (
    ClusterSnapshot,
    CommitmentPhase,
    DataForm,
    DataLocation,
    ReadyTask,
    Replica,
    ReplicaStatus,
    Scheduler,
    SnapshotValidationError,
    TaskAffinity,
    TaskCommitment,
    WorkerContext,
    WorkerState,
)
from .samples import ATTEMPT, DATA, MESSAGES, TRANSFER


def plan_for(source):
    dag = analyze_source(source)
    return dag, lower_dag(
        dag, environment_id="cpython-312-exact", package_id="revision-7"
    )


def worker_for(plan):
    return WorkerState(
        "W1",
        8,
        environment_ids=frozenset({plan.program.environment_id}),
        prepared_program_ids=frozenset({plan.program.id}),
        supported_modes=frozenset(ExecutionMode),
    )


def roundtrip(message):
    return p.decode_message(p.encode_message(message))


def test_real_scheduler_placement_dispatch_result_does_not_commit():
    dag, plan = plan_for("a=1\nb=2\nc=a+b\n")
    readiness = dag.new_readiness()
    snapshot = ClusterSnapshot(
        plan.id,
        "run-1",
        "snapshot-1",
        ready=tuple(ReadyTask(t, i) for i, t in enumerate(readiness.ready)),
        workers=(worker_for(plan),),
    )
    before = (readiness.ready, readiness.completed, snapshot)
    decision = Scheduler(plan).schedule(snapshot)
    assert decision.placements
    for placement in decision.placements:
        manifest = plan.task_index[placement.task_id]
        attempt = AttemptIdentity(
            decision.plan_id, decision.run_id, placement.task_id, "caller-attempt"
        )
        dispatch = p.TaskDispatch(
            placement.worker_id,
            attempt,
            manifest.code.program_id,
            manifest.mode,
            placement.context_id,
            message_id="dispatch",
        )
        decoded = roundtrip(dispatch)
        assert decoded.attempt is not attempt and decoded.attempt == attempt
        assert decoded.mode is manifest.mode
        commitment = TaskCommitment(
            decoded.attempt, decoded.worker_id, CommitmentPhase.DISPATCHED
        )
        assert commitment.attempt.plan_id == plan.id
        success = p.TaskSucceeded(
            "W1",
            TaskSuccess(attempt, manifest.reported_output_ids),
            message_id="report",
            correlation_id=dispatch.message_id,
        )
        result = roundtrip(success).result
        assert type(result) is TaskSuccess
        plan.validate_result(result, expected_attempt=attempt)
    assert before == (readiness.ready, readiness.completed, snapshot)


def test_actual_native_context_and_failure_semantics_survive():
    dag, plan = plan_for("a=[1]\na.append(2)\n")
    manifest = next(t for t in plan.tasks if t.mode == ExecutionMode.SHARED_CONTEXT)
    context = WorkerContext("context-1", "W1", frozenset({manifest.task_id}), 1)
    report = p.ContextPrepared(
        plan.id, "run-1", context, message_id="ready", correlation_id="prepare"
    )
    context = roundtrip(report).context
    snapshot = ClusterSnapshot(
        plan.id,
        "run-1",
        "s",
        ready=(ReadyTask(manifest.task_id, 0),),
        workers=(worker_for(plan),),
        contexts=(context,),
        affinities=(
            TaskAffinity(
                manifest.task_id, required_worker="W1", context_id=context.context_id
            ),
        ),
        data=tuple(
            DataLocation(
                value.id, DataForm.OBJECT_SNAPSHOT,
                (Replica("W1", ReplicaStatus.AVAILABLE),),
                object_state_id=None,
            )
            for value in plan.context_seed_requirements(manifest.task_id)
        ),
    )
    placement = Scheduler(plan).schedule(snapshot).placements[0]
    assert placement.context_id == context.context_id
    attempt = AttemptIdentity(plan.id, "run-1", manifest.task_id, "a")
    failure = TaskFailure(
        attempt,
        FailureInfo(
            FailureKind.PYTHON_EXCEPTION,
            "partial mutation possible",
            "builtins.ValueError",
            "text only",
        ),
    )
    result = roundtrip(
        p.TaskFailed("W1", failure, message_id="failed", correlation_id="dispatch")
    ).result
    assert type(result) is TaskFailure
    plan.validate_result(result, expected_attempt=attempt)
    assert not dag.new_readiness().completed


@pytest.mark.parametrize(
    "field,value",
    [
        ("plan_id", "c" * 64),
        ("run_id", "run-2"),
        ("task_id", "T2"),
        ("attempt_id", "attempt-2"),
    ],
)
def test_all_attempt_axes_remain_distinct(field, value):
    changed = replace(ATTEMPT, **{field: value})
    old = p.TaskStarted("W1", ATTEMPT, message_id="m", correlation_id="c")
    new = replace(old, attempt=changed)
    assert roundtrip(old) != roundtrip(new)
    assert p.encode_message(old) != p.encode_message(new)
    assert getattr(roundtrip(new).attempt, field) == value


def test_late_result_stays_distinguishable_and_parent_checks_it():
    _, plan = plan_for("x=1\n")
    manifest = plan.tasks[0]
    old = AttemptIdentity(plan.id, "run", manifest.task_id, "old")
    current = replace(old, attempt_id="new")
    result = roundtrip(
        p.TaskSucceeded(
            "W1",
            TaskSuccess(old, manifest.reported_output_ids),
            message_id="m",
            correlation_id="old-dispatch",
        )
    ).result
    with pytest.raises(ExecutionValidationError, match="attempt mismatch"):
        plan.validate_result(result, expected_attempt=current)


def test_program_environment_package_and_plan_are_distinct():
    _, plan = plan_for("x=1\n")
    decoded = roundtrip(
        p.PrepareProgram("W1", plan.id, plan.program, message_id="prepare")
    )
    assert decoded.program == plan.program
    assert decoded.plan_id == plan.id
    assert (
        len(
            {
                decoded.plan_id,
                decoded.program.id,
                decoded.program.package_id,
                decoded.program.environment_id,
            }
        )
        == 4
    )
    assert (
        roundtrip(
            replace(decoded, program=replace(plan.program, package_id=None))
        ).program.package_id
        is None
    )


def test_real_snapshot_versions_and_replicas_project_losslessly():
    _, plan = plan_for("a=[1,2]\nb=a[0]\na.append(3)\nc=a[0]\n")
    reference = next(
        v for v in plan.values if ValueRequirement(v).kind == ValueKind.SHARED_REFERENCE
    )
    state = next(
        v for v in plan.values if ValueRequirement(v).kind == ValueKind.OBJECT_STATE
    )
    replicas = (
        Replica("W1", ReplicaStatus.AVAILABLE),
        Replica("W2", ReplicaStatus.IN_FLIGHT),
    )
    initial = DataLocation(reference.id, DataForm.OBJECT_SNAPSHOT, replicas, None, None)
    updated = replace(initial, object_state_id=state.id, size_bytes=512)
    first = p.DataReference.from_location(plan.id, "run", initial)
    second = p.DataReference.from_location(plan.id, "run", updated)
    assert first != second
    assert first.object_state_id is None and second.object_state_id == state.id
    assert first.to_location(replicas, None) == initial
    assert second.to_location(replicas, 512) == updated
    notification = roundtrip(
        p.ObjectAvailable("W1", second, 512, message_id="available")
    )
    assert notification.data == second
    snapshot = ClusterSnapshot(
        plan.id,
        "run",
        "s",
        workers=(worker_for(plan), replace(worker_for(plan), worker_id="W2")),
        data=(updated,),
    )
    snapshot.validate(plan)
    with pytest.raises(p.ValidationError):
        first.to_location((replicas[0], replicas[0]))
    with pytest.raises(p.ValidationError):
        first.to_location(list(replicas))


@pytest.mark.parametrize(
    "kind",
    [
        ValueKind.NAMESPACE_STATE,
        ValueKind.COMPLETION_STATE,
        ValueKind.OBJECT_STATE,
        ValueKind.NATIVE_REFERENCE,
        ValueKind.CODE_BINDING,
        ValueKind.DISCARDED_RESULT,
        ValueKind.SHARED_REFERENCE,
    ],
)
def test_no_native_or_state_data_forms(kind):
    with pytest.raises(p.ValidationError):
        replace(DATA, form=kind)
    obj = json.loads(p.encode_message(MESSAGES[23]))
    obj["payload"]["data"]["form"] = kind.value
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())


def test_protocol_cannot_certify_peer_value_claims_or_serialize_requirements():
    _, plan = plan_for("a=[1]\na.append(2)\nx=external_name\ndef f():\n return 1\n")
    for value in plan.values:
        requirement = ValueRequirement(value)
        if requirement.kind in (ValueKind.IMMUTABLE, ValueKind.SHARED_REFERENCE):
            continue
        with pytest.raises(p.EncodingError):
            p.encode_message(requirement)
        with pytest.raises(p.ValidationError):
            p.DataReference.from_location(plan.id, "run", requirement)
        # IDs are opaque: only the plan-aware parent layer can refute a forged claim.
        forged = p.DataReference(plan.id, "run", value.id, DataForm.IMMUTABLE_VALUE)
        location = forged.to_location((Replica("W1"),))
        snapshot = ClusterSnapshot(
            plan.id, "run", "s", workers=(worker_for(plan),), data=(location,)
        )
        with pytest.raises(SnapshotValidationError):
            snapshot.validate(plan)


@pytest.mark.parametrize(
    "field,value",
    [
        ("transfer_id", "another"),
        ("transfer_attempt_id", "next"),
        ("source_worker_id", "W3"),
        ("destination_worker_id", "W4"),
    ],
)
def test_transfer_axes_are_preserved(field, value):
    changed = replace(TRANSFER, **{field: value})
    message = p.TransferFailed(
        changed.source_worker_id,
        changed,
        p.TransferFailureCode.IO_ERROR,
        message_id="failed",
        correlation_id="request",
    )
    assert roundtrip(message).transfer == changed != TRANSFER


def test_transfer_run_plan_value_and_version_are_preserved():
    keys = [
        ("plan_id", "c" * 64),
        ("run_id", "other-run"),
        ("value_id", "other-value"),
        ("object_state_id", None),
    ]
    for field, value in keys:
        changed = replace(TRANSFER, data=replace(DATA, **{field: value}))
        message = p.TransferStarted(
            "W1", changed, message_id="started", correlation_id="request"
        )
        assert roundtrip(message).transfer == changed != TRANSFER
