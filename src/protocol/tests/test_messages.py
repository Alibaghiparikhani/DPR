from dataclasses import FrozenInstanceError, replace

import pytest
import protocol as p
from execution import ExecutionMode, FailureInfo, FailureKind, TaskSuccess
from scheduler import DataForm
from protocol.messages import DEFINITIONS
from .samples import (
    ATTEMPT,
    CONTEXT,
    DATA,
    DESTINATION,
    ENDPOINT,
    FAILURE,
    MESSAGES,
    PLAN,
    PROGRAM,
    TRANSFER,
    WORKER,
)


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: type(m).__name__)
def test_immutable_message_and_correlation_rules(message):
    with pytest.raises(FrozenInstanceError):
        message.message_id = "changed"
    rule = next(d.correlation for d in DEFINITIONS if d.cls is type(message))
    if rule == "required":
        with pytest.raises(p.ValidationError):
            replace(message, correlation_id=None)
    elif rule == "none":
        with pytest.raises(p.ValidationError):
            replace(message, correlation_id="prior-command")
    else:
        assert replace(message, correlation_id=None).correlation_id is None
    with pytest.raises(p.ValidationError):
        replace(message, correlation_id=message.message_id)


@pytest.mark.parametrize(
    "record", [ATTEMPT, CONTEXT, DATA, DESTINATION, FAILURE, PROGRAM, TRANSFER, WORKER]
)
def test_nested_records_are_frozen(record):
    with pytest.raises(FrozenInstanceError):
        setattr(record, next(iter(record.__dataclass_fields__)), "changed")


@pytest.mark.parametrize("mode", list(ExecutionMode))
def test_dispatch_preserves_exact_mode_and_native_context(mode):
    kwargs = dict(
        worker_id="W1",
        attempt=ATTEMPT,
        program_id=PROGRAM.id,
        mode=mode,
        message_id="m",
    )
    if mode != ExecutionMode.ISOLATED_CANDIDATE:
        with pytest.raises(p.ValidationError):
            p.TaskDispatch(**kwargs)
    message = p.TaskDispatch(**kwargs, context_id="context-1")
    assert p.decode_message(p.encode_message(message)).mode is mode


@pytest.mark.parametrize(
    "factory",
    [
        lambda: p.WorkerHello(WORKER, DESTINATION, message_id="m"),
        lambda: p.WorkerHello(WORKER, ENDPOINT, (2,), message_id="m"),
        lambda: p.WorkerHello(WORKER, ENDPOINT, (1, 1), message_id="m"),
        lambda: p.WorkerAccepted("W1", "s", 2, message_id="m", correlation_id="c"),
        lambda: p.WorkerAccepted(
            "W1",
            "s",
            1,
            (ENDPOINT, replace(ENDPOINT, port=7)),
            message_id="m",
            correlation_id="c",
        ),
        lambda: p.MembershipUpdate("W1", "s", 1, (ENDPOINT, ENDPOINT), message_id="m"),
        lambda: replace(DATA, form=DataForm.IMMUTABLE_VALUE),
        lambda: replace(TRANSFER, destination_worker_id="W1"),
        lambda: p.TransferRequest(TRANSFER, ENDPOINT, message_id="m"),
        lambda: p.TransferAccepted("W2", TRANSFER, message_id="m", correlation_id="c"),
        lambda: p.TransferStarted("W2", TRANSFER, message_id="m", correlation_id="c"),
        lambda: p.TransferCompleted("W1", TRANSFER, message_id="m", correlation_id="c"),
        lambda: p.TransferFailed(
            "W3",
            TRANSFER,
            p.TransferFailureCode.IO_ERROR,
            message_id="m",
            correlation_id="c",
        ),
        lambda: p.PrepareContext(
            "W1", PLAN, "run", PROGRAM.id, "c", ("T1", "T1"), message_id="m"
        ),
        lambda: p.PrepareContext(
            "W1", PLAN, "run", PROGRAM.id, "c", ["T1"], message_id="m"
        ),
        lambda: p.Message(message_id="m"),
    ],
)
def test_local_incompatible_combinations(factory):
    with pytest.raises(p.ValidationError):
        factory()


def test_unknown_subclass_is_not_supported():
    class Pretender(p.WorkerGoodbye):
        pass

    with pytest.raises(p.ValidationError):
        Pretender("W1", message_id="m")
    with pytest.raises(p.EncodingError):
        p.encode_message(object.__new__(Pretender))


def test_encoder_revalidates_tampered_frozen_parent_records():
    worker = replace(WORKER)
    message = p.Heartbeat(worker, 1, message_id="m")
    object.__setattr__(worker, "running_slots", 999)
    with pytest.raises(p.ValidationError):
        p.encode_message(message)
    program = replace(PROGRAM)
    object.__setattr__(program, "id", "c" * 64)
    with pytest.raises(p.ValidationError):
        p.PrepareProgram("W1", PLAN, program, message_id="m")


def test_output_acknowledgements_and_failure_text_are_not_payloads():
    report = TaskSuccess(ATTEMPT, ("native-view", "completion-token", "value"))
    message = p.TaskSucceeded("W1", report, message_id="m", correlation_id="c")
    assert p.decode_message(p.encode_message(message)).result == report
    info = FailureInfo(
        FailureKind.EXECUTION_ERROR, "__import__('os').system('anything')"
    )
    message = p.ProgramPreparationFailed(
        "W1", PLAN, PROGRAM.id, info, message_id="m", correlation_id="c"
    )
    assert p.decode_message(p.encode_message(message)).failure == info
