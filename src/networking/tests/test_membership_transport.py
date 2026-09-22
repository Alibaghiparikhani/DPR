"""Removing a worker while the coordinator serves: it is cut off, told so, and stays out."""
from __future__ import annotations

import asyncio

import pytest

from networking import CoordinatorNetworkService, TransportLimits, WorkerNotAdmitted
from coordinator import Coordinator
from runtime_security import NodeAuthenticator

from .test_real_transport import eventually, make_client, secret, server_tls

pytestmark = pytest.mark.asyncio


def _offline(coordinator, worker_id: str) -> bool:
    try:
        return not coordinator.inspect_worker(worker_id).state.online
    except Exception:
        return True  # no longer known at all


async def _service(certs, members):
    coordinator = Coordinator(heartbeat_timeout=.4)
    service = CoordinatorNetworkService(
        coordinator, tls_policy=server_tls(certs),
        authenticator=NodeAuthenticator({node: secret(node) for node in ("W1", "W2")}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
        worker_node_ids=frozenset(members),
    )
    await service.start()
    return coordinator, service


async def test_a_removed_worker_is_disconnected_and_stops_knocking(tls_certs):
    coordinator, service = await _service(tls_certs, {"W1", "W2"})
    client = make_client(tls_certs, service, "W1", reconnect=None)
    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(client.active.wait(), 3)
        service.set_worker_identities(frozenset({"W2"}))
        await asyncio.wait_for(task, 5)          # no endless reconnecting
        assert isinstance(client.not_admitted, WorkerNotAdmitted)
        await eventually(lambda: _offline(coordinator, "W1"))
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await service.stop()


async def test_a_worker_that_was_never_a_member_is_told_so(tls_certs):
    _coordinator, service = await _service(tls_certs, {"W2"})
    client = make_client(tls_certs, service, "W1", reconnect=None)
    try:
        await asyncio.wait_for(client.run(), 5)
        assert isinstance(client.not_admitted, WorkerNotAdmitted)
        assert not client.active.is_set()
    finally:
        await service.stop()


async def test_removal_during_admission_leaves_no_gap(tls_certs):
    coordinator, service = await _service(tls_certs, {"W1"})
    client = make_client(tls_certs, service, "W1", reconnect=0)
    # Hold admission at the point where it waits for the coordinator, remove the
    # worker meanwhile, then let admission continue: it must not get in.
    await service._coordinator_lock.acquire()
    task = asyncio.create_task(client.run())
    try:
        await asyncio.sleep(0.5)
        service.set_worker_identities(frozenset())
        service._coordinator_lock.release()
        await asyncio.wait_for(task, 5)
        assert not client.active.is_set()
        with pytest.raises(Exception):
            coordinator.inspect_worker("W1")
    finally:
        if service._coordinator_lock.locked():
            service._coordinator_lock.release()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await service.stop()
