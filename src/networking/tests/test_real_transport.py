from __future__ import annotations

import asyncio
from contextlib import suppress
import ssl
import sys

import pytest

import protocol as p
from coordinator import Coordinator, StaleWorkerSession, UnknownWorker
from networking import CoordinatorNetworkService, TransportLimits
from networking.common import FramedProtocolStream, close_writer, encode_frame, new_transport_id, write_message
from runtime_security import AuthChallenge, NodeAuthenticator, TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import WorkerControlClient, WorkerControlConfig, WorkerReconnectPolicy


pytestmark = pytest.mark.asyncio


def server_tls(certs):
    return TlsPolicy(TlsCredentials(certs.server_cert, certs.server_key, certs.ca))


def client_tls(certs, node="W1", *, ca=None):
    return TlsPolicy(TlsCredentials(certs.cert(node), certs.key(node), ca or certs.ca))


def make_state(node: str, *, accepting: bool = True, total_slots: int = 2, online: bool = True):
    return WorkerState(node, total_slots, online=online, accepting_work=accepting)


def make_client(certs, service, node="W1", *, secret=None, heartbeat=.05, reconnect=0, state_provider=None):
    state = make_state(node)
    config = WorkerControlConfig(
        node_id=node,
        secret=secret or (node.encode() * 32)[:32],
        coordinator_host="127.0.0.1",
        coordinator_port=service.listening_port,
        server_hostname="localhost",
        endpoint=p.WorkerEndpoint(node, "127.0.0.1", 9000 + int(node[1:])),
        initial_state=state,
        tls_policy=client_tls(certs, node),
        heartbeat_interval=heartbeat,
        reconnect=WorkerReconnectPolicy(reconnect, .03),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
    )
    return WorkerControlClient(config, state_provider=state_provider)


async def start_service(certs, secrets, *, heartbeat_timeout=.4, limits=None, authenticator=None):
    coordinator = Coordinator(heartbeat_timeout=heartbeat_timeout)
    service = CoordinatorNetworkService(
        coordinator,
        tls_policy=server_tls(certs),
        authenticator=authenticator or NodeAuthenticator(secrets),
        limits=limits or TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
    )
    await service.start()
    return coordinator, service


async def eventually(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last = None
    while loop.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception:
            pass
        await asyncio.sleep(.01)
    raise AssertionError(f"condition not reached; last={last!r}")


class RawPeer:
    def __init__(self, reader, writer, stream, node, secret):
        self.reader = reader
        self.writer = writer
        self.stream = stream
        self.node = node
        self.secret = secret
        self.accepted = None

    @classmethod
    async def connect(cls, certs, service, node="W1", secret=None):
        tls = client_tls(certs, node).build_client_context()
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", service.listening_port, ssl=tls, server_hostname="localhost"
        )
        return cls(reader, writer, FramedProtocolStream(reader), node, secret or (node.encode()*32)[:32])

    async def send(self, message, *, fragment=None):
        frame = encode_frame(message)
        if fragment is None:
            self.writer.write(frame)
            await self.writer.drain()
        else:
            for offset in range(0, len(frame), fragment):
                self.writer.write(frame[offset:offset+fragment])
                await self.writer.drain()

    async def read(self, timeout=2):
        return await asyncio.wait_for(self.stream.read_message(), timeout)

    async def authenticate(self, *, intent=None, fragment=None):
        intent = intent or new_transport_id("intent")
        request = p.AuthenticationRequest(self.node, intent, message_id=new_transport_id("req"))
        await self.send(request, fragment=fragment)
        challenge = await self.read()
        assert isinstance(challenge, p.AuthenticationChallenge)
        proof = NodeAuthenticator({self.node: self.secret}).prove(
            AuthChallenge(challenge.node_id, challenge.session_id, challenge.nonce, challenge.issued_at)
        )
        wire = p.AuthenticationProof(
            proof.node_id, proof.session_id, proof.nonce, proof.issued_at, proof.mac_hex,
            message_id=new_transport_id("proof"), correlation_id=challenge.message_id,
        )
        await self.send(wire, fragment=fragment)
        accepted = await self.read()
        assert isinstance(accepted, p.AuthenticationAccepted)
        return request, challenge, wire, accepted

    async def register(self, *, state=None, fragment=None):
        await self.authenticate(fragment=fragment)
        hello = p.WorkerHello(
            state or make_state(self.node),
            p.WorkerEndpoint(self.node, "127.0.0.1", 9000 + int(self.node[1:])),
            message_id=new_transport_id("hello"),
        )
        await self.send(hello, fragment=fragment)
        accepted = await self.read()
        assert isinstance(accepted, p.WorkerAccepted)
        self.accepted = accepted
        return accepted

    async def close(self):
        with suppress(Exception):
            await close_writer(self.writer, timeout=.5)


def secret(node):
    return (node.encode() * 32)[:32]


async def test_one_worker_real_tls_auth_registration_heartbeat_and_clean_shutdown(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    client = make_client(tls_certs, service)
    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(client.active.wait(), 2)
        view = coordinator.inspect_worker("W1")
        assert view.handle.generation == 1
        assert client.session and client.session.session_id == view.handle.session_id
        await eventually(lambda: client.last_ack_sequence >= 1)
        await client.stop()
        await asyncio.wait_for(task, 2)
        await eventually(lambda: _unknown(coordinator, "W1"))
    finally:
        if not task.done():
            task.cancel(); await asyncio.gather(task, return_exceptions=True)
        await service.stop()


async def test_three_workers_real_tls_are_distinct_and_heartbeat(tls_certs):
    secrets = {node: secret(node) for node in ("W1", "W2", "W3")}
    coordinator, service = await start_service(tls_certs, secrets)
    clients = [make_client(tls_certs, service, node) for node in secrets]
    tasks = [asyncio.create_task(c.run()) for c in clients]
    try:
        await asyncio.gather(*(asyncio.wait_for(c.active.wait(), 2) for c in clients))
        views = [coordinator.inspect_worker(node) for node in secrets]
        assert {v.handle.worker_id for v in views} == set(secrets)
        assert len({v.handle.session_id for v in views}) == 3
        await eventually(lambda: all(c.last_ack_sequence >= 0 for c in clients))
    finally:
        await asyncio.gather(*(c.stop() for c in clients), return_exceptions=True)
        await asyncio.gather(*tasks, return_exceptions=True)
        await service.stop()


async def test_abrupt_disconnect_reconnects_with_new_generation(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    client = make_client(tls_certs, service, reconnect=2)
    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(client.active.wait(), 2)
        first = coordinator.inspect_worker("W1").handle
        assert client._writer is not None
        client._writer.transport.abort()
        second = await eventually(
            lambda: (v.handle if (v := coordinator.inspect_worker("W1")).handle.generation >= 2 else None),
            timeout=3,
        )
        assert second.generation == first.generation + 1
        assert second.session_id != first.session_id
    finally:
        await client.stop(); await asyncio.gather(task, return_exceptions=True); await service.stop()


async def test_old_socket_cannot_act_after_same_worker_reconnect(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    old = await RawPeer.connect(tls_certs, service)
    new = None
    try:
        first = await old.register()
        first_handle = coordinator.inspect_worker("W1").handle
        new = await RawPeer.connect(tls_certs, service)
        second = await new.register()
        current = coordinator.inspect_worker("W1").handle
        assert current.generation == first_handle.generation + 1
        assert second.session_id == current.session_id != first.session_id
        bad = p.Heartbeat(make_state("W1", accepting=False), 99, message_id="stale-heartbeat")
        await old.send(bad)
        await asyncio.sleep(.05)
        view = coordinator.inspect_worker("W1")
        assert view.handle == current
        assert view.state.accepting_work is True
    finally:
        await old.close()
        if new: await new.close()
        await service.stop()


async def test_fragmented_frames_and_multiple_frames_in_one_read(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register(fragment=1)
        h1 = p.Heartbeat(make_state("W1", accepting=False), 1, message_id="h1")
        h2 = p.Heartbeat(make_state("W1", accepting=True), 2, message_id="h2")
        peer.writer.write(encode_frame(h1) + encode_frame(h2))
        await peer.writer.drain()
        acks = {type(await peer.read()).__name__, type(await peer.read()).__name__}
        assert acks == {"HeartbeatAck"}
        await eventually(lambda: coordinator.inspect_worker("W1").state.accepting_work is True)
    finally:
        await peer.close(); await service.stop()


async def test_duplicate_and_stale_heartbeat_use_existing_coordinator_semantics(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register()
        await peer.send(p.Heartbeat(make_state("W1", accepting=False), 5, message_id="h5"))
        ack = await peer.read(); assert isinstance(ack, p.HeartbeatAck) and ack.sequence == 5
        await peer.send(p.Heartbeat(make_state("W1", accepting=True), 5, message_id="h5dup"))
        dup = await peer.read(); assert isinstance(dup, p.HeartbeatAck) and dup.sequence == 5
        assert coordinator.inspect_worker("W1").state.accepting_work is False
        await peer.send(p.Heartbeat(make_state("W1", accepting=True), 4, message_id="h4stale"))
        await asyncio.sleep(.05)
        assert coordinator.inspect_worker("W1").state.accepting_work is False
    finally:
        await peer.close(); await service.stop()


async def test_inbound_burst_is_throttled_not_disconnected(tls_certs):
    """A full inbound queue pauses reading (TCP backpressure); every message is still
    handled and the session survives.  Closing instead dropped real workers whenever
    they answered a burst of requests faster than the coordinator handled them."""
    limits = TransportLimits(inbound_queue_messages=1, outbound_queue_messages=64,
                             handshake_timeout=2, write_timeout=2, maintenance_interval=.02)
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")},
                                               heartbeat_timeout=5, limits=limits)
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register()
        frames = b"".join(
            encode_frame(p.Heartbeat(make_state("W1", accepting=i < 199), i, message_id=f"burst-{i}"))
            for i in range(200))
        peer.writer.write(frames); await peer.writer.drain()
        # The last heartbeat of the burst is applied, on the same session.
        await eventually(lambda: coordinator.inspect_worker("W1").state.accepting_work is False)
        assert coordinator.inspect_worker("W1").handle.generation == 1
    finally:
        await peer.close(); await service.stop()


async def test_wrong_psk_and_certificate_identity_fail_before_admission(tls_certs):
    coordinator, service = await start_service(
        tls_certs, {"W1": secret("W1"), "W2": secret("W2")}
    )
    bad_psk = make_client(tls_certs, service, "W1", secret=b"x"*32)
    bad_psk_task = asyncio.create_task(bad_psk.run())
    wrong_identity = make_client(tls_certs, service, "W2")
    # Use W1's certificate while claiming W2.
    object.__setattr__(wrong_identity.config, "tls_policy", client_tls(tls_certs, "W1"))
    wrong_task = asyncio.create_task(wrong_identity.run())
    try:
        await asyncio.wait_for(asyncio.gather(bad_psk_task, wrong_task), 4)
        assert not bad_psk.active.is_set() and not wrong_identity.active.is_set()
        assert _unknown(coordinator, "W1") and _unknown(coordinator, "W2")
    finally:
        for task in (bad_psk_task, wrong_task):
            if not task.done(): task.cancel()
        await asyncio.gather(bad_psk_task, wrong_task, return_exceptions=True)
        await service.stop()


async def test_unauthenticated_workerhello_is_rejected(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        hello = p.WorkerHello(make_state("W1"), p.WorkerEndpoint("W1", "127.0.0.1", 9001), message_id="early")
        await peer.send(hello)
        with pytest.raises((EOFError, ConnectionError, asyncio.TimeoutError)):
            await peer.read(.5)
        assert _unknown(coordinator, "W1")
    finally:
        await peer.close(); await service.stop()


def _unknown(coordinator, worker_id):
    try:
        coordinator.inspect_worker(worker_id)
    except UnknownWorker:
        return True
    return False


async def _tls_attempt(host, port, context, hostname):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=context, server_hostname=hostname), 2
        )
    except (ssl.SSLError, OSError, asyncio.TimeoutError, ConnectionError):
        return False
    try:
        data = await asyncio.wait_for(reader.read(1), .5)
        return bool(data)
    except (ssl.SSLError, OSError, asyncio.TimeoutError, ConnectionError):
        return False
    finally:
        with suppress(Exception): await close_writer(writer, timeout=.5)


async def test_tls_rejects_missing_untrusted_hostname_mismatch_and_tls12(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    try:
        no_cert = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        no_cert.minimum_version = ssl.TLSVersion.TLSv1_3
        no_cert.check_hostname = True; no_cert.verify_mode = ssl.CERT_REQUIRED
        no_cert.load_verify_locations(cafile=str(tls_certs.ca))
        assert not await _tls_attempt("127.0.0.1", service.listening_port, no_cert, "localhost")

        rogue_client = TlsPolicy(TlsCredentials(
            tls_certs.root/"rogue-client.pem", tls_certs.root/"rogue-client.key", tls_certs.ca
        )).build_client_context()
        assert not await _tls_attempt("127.0.0.1", service.listening_port, rogue_client, "localhost")

        distrust = TlsPolicy(TlsCredentials(
            tls_certs.cert("W1"), tls_certs.key("W1"), tls_certs.rogue_ca
        )).build_client_context()
        assert not await _tls_attempt("127.0.0.1", service.listening_port, distrust, "localhost")

        mismatch = client_tls(tls_certs, "W1").build_client_context()
        assert not await _tls_attempt("127.0.0.1", service.listening_port, mismatch, "not-localhost")

        tls12 = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls12.minimum_version = ssl.TLSVersion.TLSv1_2
        tls12.maximum_version = ssl.TLSVersion.TLSv1_2
        tls12.check_hostname = True; tls12.verify_mode = ssl.CERT_REQUIRED
        tls12.load_verify_locations(cafile=str(tls_certs.ca))
        tls12.load_cert_chain(str(tls_certs.cert("W1")), str(tls_certs.key("W1")))
        assert not await _tls_attempt("127.0.0.1", service.listening_port, tls12, "localhost")
        assert _unknown(coordinator, "W1")
    finally:
        await service.stop()


async def test_real_worker_subprocess_connects_over_localhost(tls_certs, tmp_path):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    script = tmp_path / "worker_proc.py"
    script.write_text(f'''
import asyncio
import protocol as p
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import WorkerControlClient, WorkerControlConfig, WorkerReconnectPolicy
from networking import TransportLimits
async def main():
    cfg=WorkerControlConfig("W1", {secret("W1")!r}, "127.0.0.1", {service.listening_port}, "localhost",
        p.WorkerEndpoint("W1","127.0.0.1",9001), WorkerState("W1",2),
        TlsPolicy(TlsCredentials({str(tls_certs.cert("W1"))!r},{str(tls_certs.key("W1"))!r},{str(tls_certs.ca)!r})),
        heartbeat_interval=.05, reconnect=WorkerReconnectPolicy(0,0), limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.02))
    c=WorkerControlClient(cfg); t=asyncio.create_task(c.run()); await asyncio.wait_for(c.active.wait(),2)
    print("ACTIVE", flush=True); await asyncio.sleep(.15); await c.stop(); await t
asyncio.run(main())
''')
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", script.read_text(), cwd=str(__import__('pathlib').Path(__file__).resolve().parents[2]),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        line = await asyncio.wait_for(proc.stdout.readline(), 3)
        assert line.strip() == b"ACTIVE"
        await eventually(lambda: coordinator.inspect_worker("W1").handle.generation == 1)
        code = await asyncio.wait_for(proc.wait(), 4)
        if code != 0:
            raise AssertionError((await proc.stderr.read()).decode())
        await eventually(lambda: _unknown(coordinator, "W1"))
    finally:
        if proc.returncode is None:
            proc.kill(); await proc.wait()
        await service.stop()

async def test_disconnect_during_authentication_and_before_workeraccepted_leave_no_member(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        req = p.AuthenticationRequest("W1", "drop-intent", message_id="drop-request")
        await peer.send(req)
        challenge = await peer.read(); assert isinstance(challenge, p.AuthenticationChallenge)
        await peer.close()
        await asyncio.sleep(.05)
        assert _unknown(coordinator, "W1")

        peer2 = await RawPeer.connect(tls_certs, service)
        await peer2.authenticate()
        hello = p.WorkerHello(make_state("W1"), p.WorkerEndpoint("W1", "127.0.0.1", 9001), message_id="drop-hello")
        await peer2.send(hello)
        peer2.writer.transport.abort()
        await eventually(lambda: _unknown(coordinator, "W1"))
    finally:
        await service.stop()


async def test_duplicate_workerhello_after_admission_is_not_re_admitted(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register()
        first = coordinator.inspect_worker("W1").handle
        duplicate = p.WorkerHello(make_state("W1"), p.WorkerEndpoint("W1", "127.0.0.1", 9001), message_id="duplicate-hello")
        await peer.send(duplicate)
        await eventually(lambda: _unknown(coordinator, "W1"))
        # It was rejected as active-session traffic, not interpreted as a reconnect.
        assert first.generation == 1
    finally:
        await peer.close(); await service.stop()


async def test_wrong_auth_session_binding_and_expired_proof_are_rejected(tls_certs):
    clock = [100.0]
    auth = NodeAuthenticator({"W1": secret("W1")}, max_age_seconds=2, clock=lambda: clock[0])
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")}, authenticator=auth)
    peer = await RawPeer.connect(tls_certs, service)
    try:
        request = p.AuthenticationRequest("W1", "s1", message_id="r1")
        await peer.send(request)
        challenge = await peer.read()
        proof = NodeAuthenticator({"W1": secret("W1")}).prove(
            AuthChallenge(challenge.node_id, challenge.session_id, challenge.nonce, challenge.issued_at)
        )
        wrong = p.AuthenticationProof("W1", "s2", proof.nonce, proof.issued_at, proof.mac_hex,
                                      message_id="p1", correlation_id=challenge.message_id)
        await peer.send(wrong)
        with pytest.raises((EOFError, ConnectionError, asyncio.TimeoutError)):
            await peer.read(.5)
        assert _unknown(coordinator, "W1")
    finally:
        await peer.close()

    peer2 = await RawPeer.connect(tls_certs, service)
    try:
        request = p.AuthenticationRequest("W1", "s-expire", message_id="r2")
        await peer2.send(request)
        challenge = await peer2.read()
        proof = NodeAuthenticator({"W1": secret("W1")}).prove(
            AuthChallenge(challenge.node_id, challenge.session_id, challenge.nonce, challenge.issued_at)
        )
        clock[0] = 103.0
        expired = p.AuthenticationProof("W1", "s-expire", proof.nonce, proof.issued_at, proof.mac_hex,
                                        message_id="p2", correlation_id=challenge.message_id)
        await peer2.send(expired)
        with pytest.raises((EOFError, ConnectionError, asyncio.TimeoutError)):
            await peer2.read(.5)
        assert _unknown(coordinator, "W1")
    finally:
        await peer2.close(); await service.stop()


async def test_replayed_authentication_proof_does_not_authorize_reconnect(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    first = await RawPeer.connect(tls_certs, service)
    old_proof = None
    try:
        _, _, old_proof, _ = await first.authenticate(intent="same-intent")
        await first.close()
        second = await RawPeer.connect(tls_certs, service)
        request = p.AuthenticationRequest("W1", "same-intent", message_id="replay-request")
        await second.send(request)
        challenge = await second.read(); assert isinstance(challenge, p.AuthenticationChallenge)
        replay = p.AuthenticationProof(
            old_proof.node_id, old_proof.session_id, old_proof.nonce, old_proof.issued_at,
            old_proof.mac_hex, message_id="replay-proof", correlation_id=challenge.message_id,
        )
        await second.send(replay)
        with pytest.raises((EOFError, ConnectionError, asyncio.TimeoutError)):
            await second.read(.5)
        assert _unknown(coordinator, "W1")
        await second.close()
    finally:
        await service.stop()


@pytest.mark.parametrize("bad_wire", [
    (p.MAX_FRAME_PAYLOAD + 1).to_bytes(4, "big"),
    (5).to_bytes(4, "big") + b"{bad}",
    (2).to_bytes(4, "big") + b"\xff\xff",
])
async def test_hostile_pre_auth_frames_are_connection_scoped(tls_certs, bad_wire):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service)
    try:
        peer.writer.write(bad_wire); await peer.writer.drain()
        await asyncio.sleep(.05)
        assert _unknown(coordinator, "W1")
    finally:
        await peer.close()
    # A malformed peer must not poison the listener or another connection.
    good = make_client(tls_certs, service)
    task = asyncio.create_task(good.run())
    try:
        await asyncio.wait_for(good.active.wait(), 2)
        assert coordinator.inspect_worker("W1").handle.generation == 1
    finally:
        await good.stop(); await asyncio.gather(task, return_exceptions=True); await service.stop()


async def test_missed_heartbeats_expire_session_and_coordinator_shutdown_stops_worker(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")}, heartbeat_timeout=.12)
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register()
        await eventually(lambda: _unknown(coordinator, "W1"), timeout=1)
    finally:
        await peer.close(); await service.stop()

    coordinator2, service2 = await start_service(tls_certs, {"W1": secret("W1")})
    client = make_client(tls_certs, service2, reconnect=0)
    task = asyncio.create_task(client.run())
    await asyncio.wait_for(client.active.wait(), 2)
    await service2.stop()
    await asyncio.wait_for(task, 2)
    assert not client.active.is_set()


async def test_heartbeat_accepting_work_and_offline_state_cross_real_transport(tls_certs):
    states = [make_state("W1", accepting=True, online=True)]
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    client = make_client(tls_certs, service, state_provider=lambda: states[0])
    task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(client.active.wait(), 2)
        states[0] = make_state("W1", accepting=False, online=True)
        await eventually(lambda: coordinator.inspect_worker("W1").state.accepting_work is False)
        states[0] = make_state("W1", accepting=False, online=False)
        await eventually(lambda: coordinator.inspect_worker("W1").state.online is False)
    finally:
        await client.stop(); await asyncio.gather(task, return_exceptions=True); await service.stop()


async def test_small_outbound_queue_neither_drops_the_session_nor_its_state(tls_certs):
    limits = TransportLimits(inbound_queue_messages=64, outbound_queue_messages=1,
                             handshake_timeout=2, write_timeout=2, maintenance_interval=.02)
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")},
                                               heartbeat_timeout=5, limits=limits)
    peer = await RawPeer.connect(tls_certs, service)
    try:
        await peer.register()
        # Replies are produced faster than a one-slot socket queue takes them; the
        # coordinator keeps them in its own bounded outbox and sends them in turn.
        frames = b"".join(
            encode_frame(p.Heartbeat(make_state("W1", accepting=i < 49), i, message_id=f"out-{i}"))
            for i in range(50))
        peer.writer.write(frames); await peer.writer.drain()
        await eventually(lambda: coordinator.inspect_worker("W1").state.accepting_work is False)
        assert coordinator.inspect_worker("W1").handle.generation == 1
        ack = await peer.read()
        assert isinstance(ack, p.HeartbeatAck)
    finally:
        await peer.close(); await service.stop()


def _worker_process_code(certs, service, node: str, hold_seconds: float) -> str:
    return f'''
import asyncio
import protocol as p
from runtime_security import TlsCredentials, TlsPolicy
from scheduler import WorkerState
from worker import WorkerControlClient, WorkerControlConfig, WorkerReconnectPolicy
from networking import TransportLimits
async def main():
    cfg=WorkerControlConfig({node!r}, {secret(node)!r}, "127.0.0.1", {service.listening_port}, "localhost",
        p.WorkerEndpoint({node!r},"127.0.0.1",{9000 + int(node[1:])}), WorkerState({node!r},2),
        TlsPolicy(TlsCredentials({str(certs.cert(node))!r},{str(certs.key(node))!r},{str(certs.ca)!r})),
        heartbeat_interval=.05, reconnect=WorkerReconnectPolicy(0,0), limits=TransportLimits(handshake_timeout=2,write_timeout=2,maintenance_interval=.02))
    c=WorkerControlClient(cfg); t=asyncio.create_task(c.run()); await asyncio.wait_for(c.active.wait(),2)
    print("ACTIVE", flush=True); await asyncio.sleep({hold_seconds!r}); await c.stop(); await t
asyncio.run(main())
'''


async def test_three_real_worker_subprocesses_are_simultaneously_admitted(tls_certs):
    nodes = ("W1", "W2", "W3")
    coordinator, service = await start_service(tls_certs, {n: secret(n) for n in nodes})
    root = str(__import__('pathlib').Path(__file__).resolve().parents[2])
    procs = [
        await asyncio.create_subprocess_exec(
            sys.executable, "-c", _worker_process_code(tls_certs, service, node, .5),
            cwd=root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        for node in nodes
    ]
    try:
        lines = await asyncio.gather(*(asyncio.wait_for(proc.stdout.readline(), 3) for proc in procs))
        assert lines == [b"ACTIVE\n"] * 3
        await eventually(lambda: all(coordinator.inspect_worker(n).handle.generation == 1 for n in nodes))
        assert len({coordinator.inspect_worker(n).handle.session_id for n in nodes}) == 3
        codes = await asyncio.gather(*(asyncio.wait_for(proc.wait(), 4) for proc in procs))
        if codes != [0, 0, 0]:
            errors = [await proc.stderr.read() for proc in procs]
            raise AssertionError((codes, errors))
    finally:
        for proc in procs:
            if proc.returncode is None:
                proc.kill()
        await asyncio.gather(*(proc.wait() for proc in procs if proc.returncode is None), return_exceptions=True)
        await service.stop()


async def test_abrupt_real_worker_process_death_reconciles_session_loss(tls_certs):
    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    root = str(__import__('pathlib').Path(__file__).resolve().parents[2])
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", _worker_process_code(tls_certs, service, "W1", 30.0),
        cwd=root, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert (await asyncio.wait_for(proc.stdout.readline(), 3)).strip() == b"ACTIVE"
        await eventually(lambda: coordinator.inspect_worker("W1").handle.generation == 1)
        proc.kill()
        await asyncio.wait_for(proc.wait(), 3)
        await eventually(lambda: _unknown(coordinator, "W1"), timeout=2)
    finally:
        if proc.returncode is None:
            proc.kill(); await proc.wait()
        await service.stop()


async def test_fix_guide_f3_coordinator_transition_error_does_not_disconnect_worker(tls_certs):
    from coordinator import InvalidTaskTransition

    coordinator, service = await start_service(tls_certs, {"W1": secret("W1")})
    peer = await RawPeer.connect(tls_certs, service, "W1")
    original = coordinator.handle_message
    try:
        accepted = await peer.register()
        before = coordinator.inspect_worker("W1").handle

        def reject_one(session, message, *, now=None):
            if isinstance(message, p.Heartbeat) and message.sequence == 777:
                raise InvalidTaskTransition("synthetic current-session transition race")
            return original(session, message, now=now)

        coordinator.handle_message = reject_one
        await peer.send(p.Heartbeat(make_state("W1"), 777, message_id="bad-transition"))
        await asyncio.sleep(.05)
        current = coordinator.inspect_worker("W1").handle
        assert current == before
        assert current.generation == before.generation

        # Prove the same TLS session still processes a subsequent valid message.
        await peer.send(p.Heartbeat(make_state("W1"), 778, message_id="good-heartbeat"))
        ack = await peer.read()
        assert isinstance(ack, p.HeartbeatAck)
        assert ack.sequence == 778
        assert coordinator.inspect_worker("W1").handle == before
    finally:
        coordinator.handle_message = original
        await peer.close()
        await service.stop()


async def test_fix_guide_f59_status_survives_coordinator_restart(tls_certs, tmp_path):
    from coordinator import RetryPolicy, SQLiteRunHistoryStore
    from coordinator.tests.helpers import accept, connect as fake_connect, dispatch_for, start
    from dag_runtime.dag_engine import analyze_source
    from execution import lower_dag
    from networking import CoordinatorClient, CoordinatorClientConfig

    history_path = tmp_path / "restart-history.sqlite3"
    history = SQLiteRunHistoryStore(history_path)
    first = Coordinator(history_store=history, retry_policy=RetryPolicy(max_attempts_per_task=1))
    plan = lower_dag(
        analyze_source("a = 1\n", filename="main.py"),
        environment_id="test-env", package_id="test-package",
    )
    worker = fake_connect(first, plan, "W1", slots=1, port=9001)
    first.submit(plan, run_id="inflight")
    first.schedule("inflight")
    dispatch = dispatch_for(worker)
    accept(worker, dispatch); start(worker, dispatch)
    # A coordinator/service restart retires the old worker session. Model that
    # physical loss before replacing the coordinator so the terminal outcome is
    # durably archived, exactly as the real shutdown path does.
    first.disconnect_session(worker.handle, reason="coordinator restart")
    assert first.inspect_run("inflight").status.value == "failed"
    assert history.has_run("inflight")

    service1 = CoordinatorNetworkService(
        first, tls_policy=server_tls(tls_certs),
        authenticator=NodeAuthenticator({"W4": secret("W4")}),
        client_node_ids=frozenset({"W4"}),
        limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
    )
    await service1.start()
    port = service1.listening_port
    config = CoordinatorClientConfig(
        "W4", secret("W4"), "127.0.0.1", port, "localhost",
        client_tls(tls_certs, "W4"),
        TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
        operation_timeout=3,
    )
    client = CoordinatorClient(config)
    await client.connect()
    service2 = None
    try:
        before = await client.run_status("inflight", include_tasks=False)
        assert before.status == "failed"

        await service1.stop()
        second = Coordinator(history_store=SQLiteRunHistoryStore(history_path))
        service2 = CoordinatorNetworkService(
            second, tls_policy=server_tls(tls_certs),
            authenticator=NodeAuthenticator({"W4": secret("W4")}),
            host="127.0.0.1", port=port,
            client_node_ids=frozenset({"W4"}),
            limits=TransportLimits(handshake_timeout=2, write_timeout=2, maintenance_interval=.02),
        )

        async def delayed_restart():
            await asyncio.sleep(.15)
            await service2.start()

        restart = asyncio.create_task(delayed_restart())
        recovered = await client.run_status_resilient("inflight", include_tasks=True)
        await restart
        assert recovered.status == "failed"
        assert recovered.run_id == "inflight"
        assert recovered.plan_id == plan.id
        assert recovered.task_count == len(plan.tasks)
        assert recovered.tasks
        assert second.run_ids() == ()
    finally:
        await client.close()
        if service2 is not None and service2._server is not None:
            await service2.stop()
        elif service1._server is not None:
            await service1.stop()


async def test_worker_waits_for_outbound_room_and_is_released_when_the_session_ends():
    """A reply that finds the worker's outbound queue full waits for room instead of
    ending the session, and a session that ends meanwhile releases the waiter."""
    from networking.errors import TransportIOError
    from worker.client import WorkerControlClient
    client = WorkerControlClient.__new__(WorkerControlClient)
    client._outbound = asyncio.Queue(maxsize=1)
    client._session_ended = asyncio.Event()
    await client._queue_outbound("first")
    waiting = asyncio.create_task(client._queue_outbound("second"))
    await asyncio.sleep(.05)
    assert not waiting.done()                  # waits, does not fail
    assert client._outbound.get_nowait() == "first"
    await asyncio.wait_for(waiting, 1)         # room appeared: delivered
    assert client._outbound.get_nowait() == "second"
    await client._queue_outbound("third")
    stuck = asyncio.create_task(client._queue_outbound("fourth"))
    await asyncio.sleep(.05)
    client._session_ended.set()
    with pytest.raises(TransportIOError):
        await asyncio.wait_for(stuck, 1)
