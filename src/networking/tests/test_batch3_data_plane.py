from __future__ import annotations

import asyncio

import pytest

import protocol as p
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import DataForm
from worker import (
    DataPlaneAuthorizationError, DataPlaneLimits, LocalDataStore, WorkerDataPlane,
    WorkerDataPlaneConfig,
)

pytestmark = pytest.mark.asyncio


def policy(certs, node: str) -> TlsPolicy:
    return TlsPolicy(TlsCredentials(certs.cert(node), certs.key(node), certs.ca))


async def eventually(predicate, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.01)


def transfer() -> p.TransferIdentity:
    data = p.DataReference("a" * 64, "run", "value", DataForm.IMMUTABLE_VALUE)
    return p.TransferIdentity(data, "transfer-1", "attempt-1", "W1", "W2")


async def start_plane(tmp_path, certs, node, session):
    store = LocalDataStore(tmp_path / node)
    plane = WorkerDataPlane(
        WorkerDataPlaneConfig(
            node, "127.0.0.1", 0, policy(certs, node),
            DataPlaneLimits(idle_timeout=1.0, total_timeout=3.0, cleanup_timeout=.5),
        ), store,
    )
    completed=[]; failed=[]; started=[]
    async def on_completed(t, size): completed.append((t,size))
    async def on_failed(t, destination, detail): failed.append((t,destination,detail))
    async def on_started(t): started.append(t)
    await plane.start(session, on_completed=on_completed, on_failed=on_failed, on_started=on_started)
    return plane,store,completed,failed,started


async def test_real_tls_p2p_moves_verified_bytes_directly_and_publishes_atomically(tmp_path, tls_certs):
    source, source_store, _, source_fail, started = await start_plane(tmp_path, tls_certs, "W1", "s1")
    dest, dest_store, completed, dest_fail, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t=transfer(); payload=b'{"t":"int","v":"42"}'
    source_store.publish_bytes(t.data, payload, session_id="s1", serialization="dpr-json-v1")
    prep=p.PrepareReceive(
        t, len(payload), "s1", "s2", "auth-token",
        message_id="prepare-receive",
    )
    await dest.prepare_receive(prep, session_id="s2"); dest.mark_ready_sent(t)
    request=p.TransferRequest(
        t, p.WorkerEndpoint("W2","127.0.0.1",dest.listening_port),
        "s1","s2","auth-token", message_id="transfer-request",
    )
    try:
        await source.send(request, session_id="s1")
        await eventually(lambda: bool(completed))
        assert started == [t]
        assert not source_fail and not dest_fail
        entry=dest_store.get(t.data, session_id="s2")
        assert entry is not None and entry.path.read_bytes()==payload
        assert completed == [(t,len(payload))]
        assert dest.active_receive_count == 0
    finally:
        await source.close(); await dest.close()


async def test_transfer_authorization_is_bound_to_sessions_and_exact_token(tmp_path, tls_certs):
    source, source_store, _, source_fail, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    dest, _, _, dest_fail, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t=transfer(); source_store.publish_bytes(t.data,b'abc',session_id="s1")
    prep=p.PrepareReceive(t,3,"s1","s2","good",message_id="prep")
    await dest.prepare_receive(prep, session_id="s2"); dest.mark_ready_sent(t)
    bad=p.TransferRequest(
        t,p.WorkerEndpoint("W2","127.0.0.1",dest.listening_port),
        "s1","s2","wrong",message_id="send",
    )
    try:
        await source.send(bad, session_id="s1")
        await eventually(lambda: bool(source_fail) or bool(dest_fail))
        assert source_fail or dest_fail
        assert not dest.store.has(t.data, session_id="s2")
        with pytest.raises(DataPlaneAuthorizationError):
            await source.send(
                p.TransferRequest(t,bad.destination,"old-session","s2","good",message_id="stale"),
                session_id="s1",
            )
    finally:
        await source.cancel(t, session_id="s1")
        await dest.cancel(t, session_id="s2")
        await source.close(); await dest.close()


async def test_cancelled_destination_never_publishes_late_bytes(tmp_path, tls_certs):
    source, source_store, _, _, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    dest, dest_store, completed, _, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t=transfer(); payload=b'x'*(2*1024*1024)
    source_store.publish_bytes(t.data,payload,session_id="s1")
    prep=p.PrepareReceive(t,len(payload),"s1","s2","auth",message_id="prep")
    await dest.prepare_receive(prep,session_id="s2"); dest.mark_ready_sent(t)
    req=p.TransferRequest(t,p.WorkerEndpoint("W2","127.0.0.1",dest.listening_port),"s1","s2","auth",message_id="send")
    try:
        await source.send(req,session_id="s1")
        await eventually(lambda: source.active_send_count or dest.active_receive_count)
        await dest.cancel(t,session_id="s2")
        await asyncio.sleep(.1)
        assert not completed
        assert dest_store.get(t.data,session_id="s2") is None
    finally:
        with __import__('contextlib').suppress(Exception): await source.cancel(t,session_id="s1")
        await source.close(); await dest.close()

async def _raw_peer_send(certs, node: str, host: str, port: int, header: dict[str, object], payload: bytes = b''):
    import json as _json
    import struct as _struct
    context = policy(certs, node).build_client_context()
    reader, writer = await asyncio.open_connection(
        host, port, ssl=context, server_hostname="W2", ssl_handshake_timeout=2.0,
    )
    try:
        encoded = _json.dumps(header, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        writer.write(_struct.pack("!I", len(encoded)) + encoded)
        await writer.drain()
        # A valid authorization receives a small ready header before payload bytes.
        if payload:
            raw_len = await asyncio.wait_for(reader.readexactly(4), 1.0)
            (length,) = _struct.unpack("!I", raw_len)
            await asyncio.wait_for(reader.readexactly(length), 1.0)
            writer.write(payload)
            await writer.drain()
    finally:
        writer.close()
        with __import__('contextlib').suppress(Exception):
            await writer.wait_closed()


def _raw_header(t: p.TransferIdentity, *, source_session="s1", destination_session="s2", authorization="auth", payload=b"abc", sha256=None):
    import hashlib as _hashlib
    return {
        "version": 1,
        "plan_id": t.data.plan_id,
        "run_id": t.data.run_id,
        "value_id": t.data.value_id,
        "form": t.data.form.value,
        "object_state_id": t.data.object_state_id,
        "transfer_id": t.transfer_id,
        "transfer_attempt_id": t.transfer_attempt_id,
        "source_worker_id": t.source_worker_id,
        "destination_worker_id": t.destination_worker_id,
        "source_session_id": source_session,
        "destination_session_id": destination_session,
        "authorization": authorization,
        "size_bytes": len(payload),
        "sha256": sha256 or _hashlib.sha256(payload).hexdigest(),
        "serialization": "dpr-json-v1",
    }


async def test_stale_transfer_attempt_bytes_cannot_satisfy_retry(tmp_path, tls_certs):
    source, source_store, _, _, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    dest, dest_store, completed, failed, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    old = transfer()
    new = p.TransferIdentity(old.data, old.transfer_id, "attempt-2", "W1", "W2")
    payload = b"retry-payload"
    source_store.publish_bytes(old.data, payload, session_id="s1", serialization="dpr-json-v1")
    try:
        await dest.prepare_receive(p.PrepareReceive(old, len(payload), "s1", "s2", "old-auth", message_id="old-prep"), session_id="s2")
        dest.mark_ready_sent(old)
        await dest.cancel(old, session_id="s2")
        await eventually(lambda: dest.active_receive_count == 0)

        await dest.prepare_receive(p.PrepareReceive(new, len(payload), "s1", "s2", "new-auth", message_id="new-prep"), session_id="s2")
        dest.mark_ready_sent(new)
        # Old-attempt traffic is rejected instead of being translated to the retry.
        with __import__('contextlib').suppress(Exception):
            await _raw_peer_send(tls_certs, "W1", "127.0.0.1", dest.listening_port,
                                 _raw_header(old, authorization="old-auth", payload=payload), payload)
        await asyncio.sleep(.05)
        assert not completed
        assert dest_store.get(new.data, session_id="s2") is None

        await source.send(p.TransferRequest(new, p.WorkerEndpoint("W2", "127.0.0.1", dest.listening_port),
                                            "s1", "s2", "new-auth", message_id="new-send"), session_id="s1")
        await eventually(lambda: completed == [(new, len(payload))])
        assert not failed or all(item[0] == old for item in failed)
    finally:
        await source.close(); await dest.close()


async def test_destination_session_replacement_fences_old_preparation(tmp_path, tls_certs):
    dest, dest_store, completed, _, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t = transfer(); payload = b"abc"
    prep = p.PrepareReceive(t, len(payload), "s1", "s2", "auth", message_id="prep")
    await dest.prepare_receive(prep, session_id="s2"); dest.mark_ready_sent(t)
    try:
        assert await dest.stop_session("s2")
        async def done(_t, _size): completed.append((_t, _size))
        async def failed(_t, _destination, _detail): pass
        async def started(_t): pass
        await dest.start("s2-new", on_completed=done, on_failed=failed, on_started=started)
        with __import__('contextlib').suppress(Exception):
            await _raw_peer_send(tls_certs, "W1", "127.0.0.1", dest.listening_port,
                                 _raw_header(t, destination_session="s2", authorization="auth", payload=payload), payload)
        await asyncio.sleep(.05)
        assert completed == []
        assert dest_store.get(t.data, session_id="s2-new") is None
        with pytest.raises(DataPlaneAuthorizationError):
            await dest.prepare_receive(prep, session_id="s2-new")
    finally:
        await dest.close()


async def test_source_session_replacement_fences_old_send_command(tmp_path, tls_certs):
    source, source_store, _, _, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    t = transfer(); source_store.publish_bytes(t.data, b"abc", session_id="s1")
    command = p.TransferRequest(t, p.WorkerEndpoint("W2", "127.0.0.1", 1), "s1", "s2", "auth", message_id="send")
    try:
        assert await source.stop_session("s1")
        async def completed(_t, _size): pass
        async def failed(_t, _destination, _detail): pass
        async def started(_t): pass
        await source.start("s1-new", on_completed=completed, on_failed=failed, on_started=started)
        with pytest.raises(DataPlaneAuthorizationError):
            await source.send(command, session_id="s1-new")
    finally:
        await source.close()


async def test_truncated_payload_is_never_published(tmp_path, tls_certs):
    dest, dest_store, completed, failed, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t = transfer(); declared = b"abcdef"
    await dest.prepare_receive(p.PrepareReceive(t, len(declared), "s1", "s2", "auth", message_id="prep"), session_id="s2")
    dest.mark_ready_sent(t)
    try:
        header = _raw_header(t, authorization="auth", payload=declared)
        # Send fewer bytes than the authenticated declared length, then EOF.
        await _raw_peer_send(tls_certs, "W1", "127.0.0.1", dest.listening_port, header, b"abc")
        await eventually(lambda: bool(failed))
        assert completed == []
        assert dest_store.get(t.data, session_id="s2") is None
        assert dest.active_receive_count == 0
    finally:
        await dest.close()


async def test_incorrect_payload_digest_is_never_published(tmp_path, tls_certs):
    dest, dest_store, completed, failed, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t = transfer(); payload = b"abcdef"
    await dest.prepare_receive(p.PrepareReceive(t, len(payload), "s1", "s2", "auth", message_id="prep"), session_id="s2")
    dest.mark_ready_sent(t)
    try:
        header = _raw_header(t, authorization="auth", payload=payload, sha256="0" * 64)
        await _raw_peer_send(tls_certs, "W1", "127.0.0.1", dest.listening_port, header, payload)
        await eventually(lambda: bool(failed))
        assert completed == []
        assert dest_store.get(t.data, session_id="s2") is None
    finally:
        await dest.close()


async def test_wrong_object_state_version_cannot_satisfy_prepared_transfer(tmp_path, tls_certs):
    base = transfer()
    expected_data = p.DataReference(base.data.plan_id, base.data.run_id, base.data.value_id,
                                    DataForm.OBJECT_SNAPSHOT, "state-2")
    expected = p.TransferIdentity(expected_data, base.transfer_id, base.transfer_attempt_id, "W1", "W2")
    stale_data = p.DataReference(base.data.plan_id, base.data.run_id, base.data.value_id,
                                 DataForm.OBJECT_SNAPSHOT, "state-1")
    stale = p.TransferIdentity(stale_data, base.transfer_id, base.transfer_attempt_id, "W1", "W2")
    dest, dest_store, completed, _, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    await dest.prepare_receive(p.PrepareReceive(expected, 3, "s1", "s2", "auth", message_id="prep"), session_id="s2")
    dest.mark_ready_sent(expected)
    try:
        with __import__('contextlib').suppress(Exception):
            await _raw_peer_send(tls_certs, "W1", "127.0.0.1", dest.listening_port,
                                 _raw_header(stale, authorization="auth", payload=b"abc"), b"abc")
        await asyncio.sleep(.05)
        assert completed == []
        assert dest_store.get(expected.data, session_id="s2") is None
        assert dest.active_receive_count == 1  # valid exact attempt is still prepared
    finally:
        await dest.cancel(expected, session_id="s2")
        await dest.close()


async def test_destination_store_exhaustion_fails_without_publication(tmp_path, tls_certs):
    from worker import DataStoreLimits
    source, source_store, _, _, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    tiny_store = LocalDataStore(tmp_path / "W2-tiny", limits=DataStoreLimits(max_bytes=8, max_items=2, max_value_bytes=8))
    dest = WorkerDataPlane(
        WorkerDataPlaneConfig("W2", "127.0.0.1", 0, policy(tls_certs, "W2"),
                              DataPlaneLimits(idle_timeout=1.0, total_timeout=3.0, cleanup_timeout=.5)),
        tiny_store,
    )
    completed=[]; failed=[]
    async def on_completed(t, size): completed.append((t,size))
    async def on_failed(t, destination, detail): failed.append((t,destination,detail))
    async def on_started(_t): pass
    await dest.start("s2", on_completed=on_completed, on_failed=on_failed, on_started=on_started)
    t=transfer(); payload=b"0123456789abcdef"
    source_store.publish_bytes(t.data,payload,session_id="s1")
    await dest.prepare_receive(p.PrepareReceive(t,len(payload),"s1","s2","auth",message_id="prep"),session_id="s2")
    dest.mark_ready_sent(t)
    try:
        await source.send(p.TransferRequest(t,p.WorkerEndpoint("W2","127.0.0.1",dest.listening_port),
                                            "s1","s2","auth",message_id="send"),session_id="s1")
        await eventually(lambda: bool(failed))
        assert completed == []
        assert tiny_store.get(t.data, session_id="s2") is None
        assert tiny_store.item_count == 0
    finally:
        await source.close(); await dest.close()


async def test_oversized_declared_transfer_is_rejected_before_receive_state(tmp_path, tls_certs):
    store = LocalDataStore(tmp_path / "W2")
    plane = WorkerDataPlane(
        WorkerDataPlaneConfig("W2", "127.0.0.1", 0, policy(tls_certs, "W2"),
                              DataPlaneLimits(max_transfer_bytes=8, chunk_bytes=4, total_timeout=1.0)), store,
    )
    async def noop(*_args): pass
    await plane.start("s2", on_completed=noop, on_failed=noop, on_started=noop)
    t=transfer()
    try:
        from worker import DataPlaneResourceError
        with pytest.raises(DataPlaneResourceError):
            await plane.prepare_receive(p.PrepareReceive(t, 9, "s1", "s2", "auth", message_id="prep"), session_id="s2")
        assert plane.active_receive_count == 0
    finally:
        await plane.close()


async def test_wrong_source_certificate_cannot_consume_legitimate_preparation(tmp_path,tls_certs):
    dest,dest_store,completed,_,_=await start_plane(tmp_path,tls_certs,"W2","s2")
    t=transfer(); payload=b"abc"
    await dest.prepare_receive(p.PrepareReceive(t,3,"s1","s2","auth",message_id="prep"),session_id="s2")
    dest.mark_ready_sent(t)
    try:
        with __import__('contextlib').suppress(Exception):
            await _raw_peer_send(tls_certs,"W3","127.0.0.1",dest.listening_port,
                                 _raw_header(t,authorization="auth",payload=payload),payload)
        await asyncio.sleep(.05)
        assert completed==[] and dest_store.get(t.data,session_id="s2") is None
        assert dest.active_receive_count==1
    finally:
        await dest.cancel(t,session_id="s2"); await dest.close()


async def test_conflicting_duplicate_receive_and_send_attempts_fail_closed(tmp_path,tls_certs):
    source,source_store,_,_,_=await start_plane(tmp_path,tls_certs,"W1","s1")
    dest,_,_,_,_=await start_plane(tmp_path,tls_certs,"W2","s2")
    t=transfer(); source_store.publish_bytes(t.data,b"abc",session_id="s1")
    prep=p.PrepareReceive(t,3,"s1","s2","auth-a",message_id="prep-a")
    await dest.prepare_receive(prep,session_id="s2")
    try:
        with pytest.raises(DataPlaneAuthorizationError):
            await dest.prepare_receive(
                p.PrepareReceive(t,3,"s1","s2","auth-b",message_id="prep-b"),session_id="s2"
            )
        req=p.TransferRequest(t,p.WorkerEndpoint("W2","127.0.0.1",dest.listening_port),
                              "s1","s2","auth-a",message_id="send-a")
        await source.send(req,session_id="s1")
        with pytest.raises(DataPlaneAuthorizationError):
            await source.send(
                p.TransferRequest(t,req.destination,"s1","s2","auth-b",message_id="send-b"),
                session_id="s1",
            )
    finally:
        await source.cancel(t,session_id="s1")
        await dest.cancel(t,session_id="s2")
        await source.close(); await dest.close()

async def test_trailing_payload_byte_is_rejected(tmp_path, tls_certs):
    dest, dest_store, completed, failed, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    t = transfer(); payload = b"abcdef"
    await dest.prepare_receive(
        p.PrepareReceive(t, len(payload), "s1", "s2", "auth", message_id="prep"),
        session_id="s2",
    )
    dest.mark_ready_sent(t)
    try:
        # Header authenticates exactly `payload`, but the peer sends one extra byte.
        await _raw_peer_send(
            tls_certs, "W1", "127.0.0.1", dest.listening_port,
            _raw_header(t, authorization="auth", payload=payload), payload + b"X",
        )
        await eventually(lambda: bool(failed))
        assert completed == []
        assert dest_store.get(t.data, session_id="s2") is None
        assert "trailing bytes" in failed[-1][2]
    finally:
        await dest.close()


def numbered_transfer(index: int) -> p.TransferIdentity:
    data = p.DataReference("a" * 64, "run", f"value-{index}", DataForm.IMMUTABLE_VALUE)
    return p.TransferIdentity(data, f"transfer-{index}", f"attempt-{index}", "W1", "W2")


async def test_sends_beyond_the_stream_limit_queue_instead_of_failing(tmp_path, tls_certs):
    """A fan-in of more values than there are send streams queues on the source."""
    source, source_store, _, source_fail, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    dest, dest_store, completed, dest_fail, _ = await start_plane(tmp_path, tls_certs, "W2", "s2")
    count = source.config.limits.max_outgoing * 3
    payload = b"z" * (256 * 1024)
    transfers = [numbered_transfer(index) for index in range(count)]
    try:
        for index, t in enumerate(transfers):
            source_store.publish_bytes(t.data, payload, session_id="s1")
            await dest.prepare_receive(
                p.PrepareReceive(t, len(payload), "s1", "s2", f"auth-{index}",
                                 message_id=f"prep-{index}"), session_id="s2")
            dest.mark_ready_sent(t)
        for index, t in enumerate(transfers):
            await source.send(p.TransferRequest(
                t, p.WorkerEndpoint("W2", "127.0.0.1", dest.listening_port),
                "s1", "s2", f"auth-{index}", message_id=f"send-{index}"), session_id="s1")
        await eventually(lambda: len(completed) == count, timeout=15.0)
        assert not source_fail and not dest_fail
        assert all(dest_store.get(t.data, session_id="s2") is not None for t in transfers)
        await eventually(lambda: source.active_send_count == 0)
    finally:
        await source.close(); await dest.close()


async def test_send_cancelled_before_it_starts_is_retired(tmp_path, tls_certs):
    """Cancelling a queued send whose task never ran must not leave its record behind."""
    source, source_store, _, source_fail, _ = await start_plane(tmp_path, tls_certs, "W1", "s1")
    t = transfer(); source_store.publish_bytes(t.data, b"abc", session_id="s1")
    request = p.TransferRequest(t, p.WorkerEndpoint("W2", "127.0.0.1", 9), "s1", "s2", "auth",
                                message_id="send")
    try:
        # No await between the two: the send task has not taken its first step.
        await source.send(request, session_id="s1")
        await source.cancel(t, session_id="s1")
        assert source.active_send_count == 0
        assert any(transfer_ is t and destination is False for transfer_, destination, _ in source_fail)
    finally:
        await source.close()


async def test_transfer_deadline_grows_with_the_value_size():
    limits = DataPlaneLimits()
    assert limits.deadline_for(None) == limits.total_timeout
    size = 256 * 1024 * 1024
    assert limits.deadline_for(size) == limits.total_timeout + size / limits.min_bytes_per_second
    assert DataPlaneLimits(total_timeout=None).deadline_for(size) is None
    with pytest.raises(ValueError):
        DataPlaneLimits(min_bytes_per_second=0)
