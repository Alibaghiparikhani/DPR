"""In-memory control-plane smoke example. It analyzes but does not execute code."""
from dag_runtime.dag_engine import analyze_source
from execution import ExecutionMode, TaskSuccess, lower_dag
import protocol as p
from scheduler import WorkerState

from coordinator import Coordinator, RunStatus


def main() -> None:
    dag = analyze_source("a = 1\nb = a + 1\n")
    plan = lower_dag(dag, environment_id="example-env", package_id="example-package")
    coordinator = Coordinator()
    worker = WorkerState(
        "W1", 1, cpu_percent=1.0, total_memory_bytes=1024, available_memory_bytes=1024,
        cpu_cores=1, environment_ids={plan.program.environment_id},
        prepared_program_ids={plan.program.id}, supported_modes=set(ExecutionMode),
    )
    hello = p.WorkerHello(
        worker, p.WorkerEndpoint("W1", "worker-one.lan", 9000),
        message_id="hello-1",
    )
    session = coordinator.register_worker(hello)
    coordinator.drain_outbox(session)  # WorkerAccepted
    coordinator.submit(plan, run_id="example-run")

    while coordinator.inspect_run("example-run").status == RunStatus.RUNNING:
        coordinator.schedule("example-run")
        for message in coordinator.drain_outbox(session):
            if not isinstance(message, p.TaskDispatch):
                continue
            coordinator.handle_message(session, p.TaskAccepted(
                "W1", message.attempt, message_id=f"accept-{message.attempt.attempt_id}",
                correlation_id=message.message_id,
            ))
            coordinator.handle_message(session, p.TaskStarted(
                "W1", message.attempt, message_id=f"start-{message.attempt.attempt_id}",
                correlation_id=message.message_id,
            ))
            manifest = plan.task_index[message.attempt.task_id]
            coordinator.handle_message(session, p.TaskSucceeded(
                "W1", TaskSuccess(message.attempt, manifest.reported_output_ids),
                message_id=f"success-{message.attempt.attempt_id}",
                correlation_id=message.message_id,
            ))

    coordinator.validate_state()
    print("Coordinator smoke passed:", coordinator.inspect_run("example-run").status.value)


if __name__ == "__main__":
    main()
