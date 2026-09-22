from dataclasses import replace
import json

import pytest
import protocol as p
from execution import FailureInfo, FailureKind, TaskFailure
from scheduler import DataForm, Replica, ReplicaStatus
from .samples import ATTEMPT, DATA, ENDPOINT, MESSAGES, TRANSFER


@pytest.mark.parametrize("code", list(p.ProtocolErrorCode))
def test_every_protocol_error_code(code):
    message = p.ErrorReport(code, message_id="m")
    assert p.decode_message(p.encode_message(message)).code is code


@pytest.mark.parametrize("code", list(p.RejectionCode))
def test_every_rejection_code(code):
    message = p.TaskRejected("W1", ATTEMPT, code, message_id="m", correlation_id="c")
    assert p.decode_message(p.encode_message(message)).code is code


@pytest.mark.parametrize("code", list(p.TransferFailureCode))
def test_every_transfer_failure_code(code):
    message = p.TransferFailed("W2", TRANSFER, code, message_id="m", correlation_id="c")
    assert p.decode_message(p.encode_message(message)).code is code


@pytest.mark.parametrize("outcome", list(p.CancellationOutcome))
def test_every_cancellation_outcome(outcome):
    message = p.TaskCancellationResult(
        "W1", ATTEMPT, outcome, message_id="m", correlation_id="c"
    )
    assert p.decode_message(p.encode_message(message)).outcome is outcome


@pytest.mark.parametrize("kind", list(FailureKind))
def test_every_execution_failure_kind(kind):
    for trace in (None, "", "traceback text"):
        failure = FailureInfo(
            kind,
            "",
            "builtins.ValueError" if kind == FailureKind.PYTHON_EXCEPTION else None,
            trace,
        )
        message = p.TaskFailed(
            "W1", TaskFailure(ATTEMPT, failure), message_id="m", correlation_id="c"
        )
        assert p.decode_message(p.encode_message(message)).result.failure == failure


@pytest.mark.parametrize("status", list(ReplicaStatus))
def test_existing_replica_statuses_and_nullable_size(status):
    for size in (None, 0, p.MAX_INTEGER):
        replica = Replica("W1", status)
        location = DATA.to_location((replica,), size)
        assert location.replicas == (replica,)
        assert location.size_bytes == size


@pytest.mark.parametrize("port", [1, 65535])
def test_legal_endpoint_port_boundaries(port):
    message = replace(MESSAGES[0], endpoint=replace(ENDPOINT, port=port))
    assert p.decode_message(p.encode_message(message)) == message


@pytest.mark.parametrize("port", [0, -1, 65536, 1.0, "9", True, None])
def test_invalid_endpoint_ports_both_boundaries(port):
    with pytest.raises(p.ValidationError):
        replace(ENDPOINT, port=port)
    obj = json.loads(p.encode_message(MESSAGES[0]))
    obj["payload"]["endpoint"]["port"] = port
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())


@pytest.mark.parametrize("form", list(DataForm))
def test_all_data_forms(form):
    message = p.ObjectAvailable(
        "W1", replace(DATA, form=form, object_state_id=None), message_id="m"
    )
    assert p.decode_message(p.encode_message(message)).data.form is form


def test_initially_zero_worker_capacity_is_a_structural_claim():
    original = MESSAGES[0]
    worker = replace(
        original.worker,
        total_slots=0,
        running_slots=0,
        reserved_slots=0,
        online=False,
        accepting_work=False,
        environment_ids=frozenset(),
        supported_modes=frozenset(),
        prepared_program_ids=frozenset(),
    )
    message = replace(original, worker=worker)
    assert p.decode_message(p.encode_message(message)) == message


@pytest.mark.parametrize(
    "index,field",
    [(2, "code"), (21, "reason"), (22, "outcome"), (31, "code"), (32, "code")],
)
def test_wrong_types_for_remaining_lifecycle_fields(index, field):
    for value in (None, {}, [], 1, True):
        obj = json.loads(p.encode_message(MESSAGES[index]))
        obj["payload"][field] = value
        with pytest.raises(p.ValidationError):
            p.decode_message(json.dumps(obj).encode())


def test_context_tasks_and_versions_are_not_silently_deduplicated():
    obj = json.loads(p.encode_message(MESSAGES[12]))
    obj["payload"]["context"]["prepared_task_ids"] = ["T1", "T1"]
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())
    obj = json.loads(p.encode_message(MESSAGES[0]))
    obj["payload"]["supported_versions"] = [1, 1]
    with pytest.raises(p.ValidationError):
        p.decode_message(json.dumps(obj).encode())
