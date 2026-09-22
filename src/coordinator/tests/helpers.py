from __future__ import annotations

from dataclasses import dataclass

import protocol as p
from execution import ExecutionMode, TaskFailure, TaskSuccess, ValueKind
from scheduler import DataForm, WorkerState


@dataclass
class FakeWorkerSession:
    coordinator: object
    handle: object

    def drain(self):
        return self.coordinator.drain_outbox(self.handle)

    def send(self, message, *, now=None):
        return self.coordinator.handle_message(self.handle, message, now=now)


def worker_state(plan, worker_id="W1", *, slots=2, prepared=True, accepting=True,
                 running=0, reserved=0):
    return WorkerState(
        worker_id=worker_id,
        total_slots=slots,
        running_slots=running,
        reserved_slots=reserved,
        accepting_work=accepting,
        cpu_percent=5.0,
        total_memory_bytes=1_000_000,
        available_memory_bytes=900_000,
        cpu_cores=4,
        environment_ids=frozenset({plan.program.environment_id}),
        prepared_program_ids=frozenset({plan.program.id}) if prepared else frozenset(),
        supported_modes=frozenset(ExecutionMode),
    )


def connect(coordinator, plan, worker_id="W1", *, slots=2, prepared=True, now=0.0, port=9000):
    state = worker_state(plan, worker_id, slots=slots, prepared=prepared)
    hello = p.WorkerHello(
        worker=state,
        endpoint=p.WorkerEndpoint(worker_id, f"{worker_id.lower()}.lan", port),
        supported_versions=(p.PROTOCOL_VERSION,),
        message_id=f"hello-{worker_id}",
    )
    handle = coordinator.register_worker(hello, now=now)
    fake = FakeWorkerSession(coordinator, handle)
    accepted = fake.drain()
    assert len(accepted) == 1 and isinstance(accepted[0], p.WorkerAccepted)
    return fake


def dispatch_for(fake, task_id=None):
    messages = fake.drain()
    dispatches = [m for m in messages if isinstance(m, p.TaskDispatch)]
    if task_id is None:
        assert len(dispatches) == 1
        return dispatches[0]
    return next(m for m in dispatches if m.attempt.task_id == task_id)


def accept(fake, dispatch, *, suffix="accept"):
    return fake.send(p.TaskAccepted(
        worker_id=fake.handle.worker_id,
        attempt=dispatch.attempt,
        message_id=f"{suffix}-{dispatch.attempt.attempt_id}",
        correlation_id=dispatch.message_id,
    ))


def start(fake, dispatch, *, suffix="start"):
    return fake.send(p.TaskStarted(
        worker_id=fake.handle.worker_id,
        attempt=dispatch.attempt,
        message_id=f"{suffix}-{dispatch.attempt.attempt_id}",
        correlation_id=dispatch.message_id,
    ))


def succeed(fake, plan, dispatch, *, suffix="success", publish_available=True):
    """Mirror the real worker's success/availability control-stream sequence.

    Physical snapshot availability is a separate post-commit observation. Tests
    that intentionally exercise TaskSucceeded *without* ObjectAvailable can pass
    ``publish_available=False``; ordinary coordinator fixtures should model the
    production worker and publish transferable shared-reference/object-state
    snapshots after the success observation.
    """
    manifest = plan.task_index[dispatch.attempt.task_id]
    msg = p.TaskSucceeded(
        worker_id=fake.handle.worker_id,
        result=TaskSuccess(dispatch.attempt, manifest.reported_output_ids),
        message_id=f"{suffix}-{dispatch.attempt.attempt_id}",
        correlation_id=dispatch.message_id,
    )
    disposition = fake.send(msg)
    if not publish_available:
        return msg, disposition

    published = set()
    for requirement in manifest.outputs:
        if requirement.id not in manifest.reported_output_ids:
            continue
        if requirement.kind is ValueKind.SHARED_REFERENCE:
            ref = p.DataReference(
                plan.id, dispatch.attempt.run_id, requirement.id,
                DataForm.OBJECT_SNAPSHOT, None,
            )
            if ref not in published:
                fake.send(p.ObjectAvailable(
                    worker_id=fake.handle.worker_id, data=ref,
                    message_id=f"available-{requirement.id}-{dispatch.attempt.attempt_id}",
                ))
                published.add(ref)

    for obj in manifest.objects:
        if not obj.input_ids:
            continue
        reference_id = obj.input_ids[0]
        for state_id in obj.state_outputs:
            ref = p.DataReference(
                plan.id, dispatch.attempt.run_id, reference_id,
                DataForm.OBJECT_SNAPSHOT, state_id,
            )
            if ref in published:
                continue
            fake.send(p.ObjectAvailable(
                worker_id=fake.handle.worker_id, data=ref,
                message_id=f"available-{reference_id}-{state_id}-{dispatch.attempt.attempt_id}",
            ))
            published.add(ref)
    return msg, disposition


def fail(fake, dispatch, failure, *, suffix="failure"):
    msg = p.TaskFailed(
        worker_id=fake.handle.worker_id,
        result=TaskFailure(dispatch.attempt, failure),
        message_id=f"{suffix}-{dispatch.attempt.attempt_id}",
        correlation_id=dispatch.message_id,
    )
    return msg, fake.send(msg)


def seed_location(coordinator, plan, run_id, worker_id, data, size_bytes=None):
    """Test-only setup for scenarios that begin after provenance was certified.

    Provenance itself is exercised through real protocol messages in dedicated
    tests; transfer/failure tests should not forge ObjectAvailable merely to
    manufacture their starting state.
    """
    generation = coordinator._workers[worker_id].handle.generation
    coordinator._locations.announce(plan, run_id, worker_id, generation, data, size_bytes)
    coordinator._touch()


def release_terminal_objects(coordinator, *workers):
    """Acknowledge every staged Phase-3 terminal object release in unit tests."""
    coordinator.request_terminal_object_releases()
    released = 0
    for worker in workers:
        for message in worker.drain():
            if not isinstance(message, p.ReleaseObject):
                continue
            worker.send(p.ObjectReleased(
                worker_id=worker.handle.worker_id,
                data=message.data,
                message_id=f"released-{message.message_id}",
                correlation_id=message.message_id,
            ))
            released += 1
    return released
