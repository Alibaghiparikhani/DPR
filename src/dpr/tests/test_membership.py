"""Host approval and removal: who may join, and taking a machine back out."""
from __future__ import annotations

import json
from pathlib import Path
import ssl
import threading
import time

import pytest

from dpr import credentials, enroll
from dpr.home import Home, read_json

needs_openssl = pytest.mark.skipif(credentials.openssl() is None, reason="needs openssl")
A, B = "a" * 32, "b" * 32


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _ref(admissions: enroll.Admissions, name: str) -> str:
    return next(entry["ref"] for entry in admissions._waiting.values() if entry["name"] == name)


# -------------------------------------------------------------------- admissions

def test_nothing_is_handed_out_before_approval(tmp_path: Path):
    board = tmp_path / "board.json"
    admissions = enroll.Admissions(tmp_path / "claims.json", ["w1", "w2"], board=board)
    decision, number = admissions.request(A, "laptop")
    assert decision == "waiting" and len(number) == 6 and number.isdigit()
    assert admissions.request(A, "laptop") == ("waiting", number)
    assert admissions.members() == frozenset()
    shown = read_json(board)["waiting"]
    assert [(entry["name"], entry["code"]) for entry in shown] == [("laptop", number)]
    assert A not in board.read_text(), "a machine's secret id never leaves the node"
    assert admissions.approve(shown[0]["ref"]) == "approved"
    assert admissions.request(A, "laptop") == ("approved", "w1")
    assert admissions.members() == frozenset({"w1"}) and read_json(board)["waiting"] == []


def test_an_approval_can_only_be_collected_by_the_machine_that_asked(tmp_path: Path):
    admissions = enroll.Admissions(tmp_path / "claims.json", ["w1", "w2"])
    admissions.request(A, "laptop")
    admissions.approve(_ref(admissions, "laptop"))
    # Same name, different machine: a new request, not the approved identity.
    assert admissions.request(B, "laptop")[0] == "waiting"


def test_a_denied_machine_stays_refused_until_the_host_restarts(tmp_path: Path):
    admissions = enroll.Admissions(tmp_path / "claims.json", ["w1"])
    admissions.request(A, "stranger")
    assert admissions.deny(_ref(admissions, "stranger")) == "denied"
    assert admissions.request(A, "stranger") == ("refused", None)
    again = enroll.Admissions(tmp_path / "claims.json", ["w1"])
    assert again.request(A, "stranger")[0] == "waiting"


def test_a_removed_identity_is_never_handed_out_again(tmp_path: Path):
    claims = tmp_path / "claims.json"
    admissions = enroll.Admissions(claims, ["w1", "w2"])
    admissions.request(A, "laptop")
    admissions.approve(_ref(admissions, "laptop"))
    assert admissions.kick("w1") == "removed" and admissions.kick("w1") == "gone"
    assert admissions.members() == frozenset()
    assert admissions.request(A, "laptop") == ("refused", None)
    again = enroll.Admissions(claims, ["w1", "w2"])            # the host restarts
    again.request(A, "laptop")
    assert again.approve(_ref(again, "laptop")) == "approved"
    assert again.request(A, "laptop") == ("approved", "w2")
    assert json.loads(claims.read_text())["retired"] == ["w1"]
    assert again.request(B, "other") == ("full", None)


def test_waiting_is_bounded_and_forgets_machines_that_stop_asking(tmp_path: Path):
    clock = Clock()
    board = tmp_path / "board.json"
    admissions = enroll.Admissions(tmp_path / "claims.json", [f"w{i}" for i in range(20)],
                                   board=board, clock=clock)
    machines = [f"{index:032x}" for index in range(enroll.MAX_WAITING + 1)]
    for machine in machines[:-1]:
        assert admissions.request(machine, "m")[0] == "waiting"
    assert admissions.request(machines[-1], "m") == ("busy", None)
    clock.now += enroll.WAITING_IDLE + 1
    admissions.tick()
    assert read_json(board)["waiting"] == []
    assert admissions.request(machines[-1], "m")[0] == "waiting"


def test_decisions_channel_ignores_anything_malformed(tmp_path: Path):
    from dpr.node import _decide

    class Service:
        allowed: list = []

        def set_worker_identities(self, identities):
            self.allowed.append(frozenset(identities))

    admissions = enroll.Admissions(tmp_path / "claims.json", ["w1"], board=tmp_path / "board.json")
    admissions.request(A, "laptop")
    ref, service = _ref(admissions, "laptop"), Service()
    command = "0123456789abcdef"
    for junk in (b"not json", b"[]", b'{"approve": "%s"}' % ref.encode(),
                 json.dumps({"id": "x", "approve": ref}).encode(),
                 json.dumps({"id": command, "approve": ref, "kick": "w1"}).encode(),
                 json.dumps({"id": command, "approve": "../../x"}).encode(),
                 json.dumps({"id": command, "kick": "owner"}).encode(),
                 json.dumps({"id": command, "shutdown": "now"}).encode()):
        _decide(junk, admissions, service)
    assert service.allowed == [] and admissions.members() == frozenset()
    _decide(json.dumps({"id": command, "approve": ref}).encode(), admissions, service)
    assert service.allowed == [frozenset({"w1"})]
    assert read_json(tmp_path / "board.json")["results"][command] == "approved"


# ------------------------------------------------------------- over the network

@pytest.fixture
def service(tmp_path: Path):
    creds = tmp_path / "creds"
    credentials.create(creds, workers=["w1", "w2"], client="owner")
    admissions = enroll.Admissions(creds / "claims.json", ["w1", "w2"])
    token = enroll.new_token()
    server = enroll.serve(credentials=creds, admissions=admissions, token=token,
                          coordinator_port=8740, host="127.0.0.1", port=0)
    pin = enroll.fingerprint(ssl.PEM_cert_to_DER_cert((creds / "coordinator.pem").read_text()))
    yield {"code": {"host": "127.0.0.1", "port": server.server_address[1], "token": token,
                    "fingerprint": pin}, "admissions": admissions}
    server.shutdown()


@needs_openssl
def test_joining_waits_for_the_host_and_both_see_one_number(service, monkeypatch):
    monkeypatch.setattr(enroll, "POLL_INTERVAL", 0.1)
    shown, result = [], {}

    def join() -> None:
        try:
            result["bundle"] = enroll.fetch(service["code"], machine=A, name="laptop",
                                            waiting=shown.append)
        except Exception as error:  # pragma: no cover - reported below
            result["error"] = error

    joiner = threading.Thread(target=join)
    joiner.start()
    deadline = time.monotonic() + 10
    while not service["admissions"]._waiting:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    time.sleep(0.5)
    assert "bundle" not in result, "nothing may be handed out before approval"
    entry = next(iter(service["admissions"]._waiting.values()))
    assert shown == [entry["code"]]
    service["admissions"].approve(entry["ref"])
    joiner.join(10)
    assert result["bundle"]["id"] == "w1"


@needs_openssl
@pytest.mark.parametrize("decision, message", [("deny", "refused by the host")])
def test_a_denied_machine_is_told_so(service, monkeypatch, decision, message):
    monkeypatch.setattr(enroll, "POLL_INTERVAL", 0.1)
    errors = []

    def join() -> None:
        try:
            enroll.fetch(service["code"], machine=A, name="laptop")
        except enroll.JoinError as error:
            errors.append(str(error))

    joiner = threading.Thread(target=join)
    joiner.start()
    deadline = time.monotonic() + 10
    while not service["admissions"]._waiting:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    getattr(service["admissions"], decision)(next(iter(service["admissions"]._waiting.values()))["ref"])
    joiner.join(10)
    assert errors == [message]


@needs_openssl
def test_a_crowd_of_waiting_machines_is_turned_away(service):
    for index in range(enroll.MAX_WAITING):
        service["admissions"].request(f"{index:032x}", "crowd")
    with pytest.raises(enroll.JoinError, match="too many machines waiting"):
        enroll.fetch(service["code"], machine=A, name="late", patience=0)


# ------------------------------------------------ removal from a running cluster

@needs_openssl
def test_a_removed_worker_is_cut_off_and_cannot_come_back(tmp_path: Path, monkeypatch):
    from dpr.cluster import Hosting, Joined

    monkeypatch.setattr(enroll, "POLL_INTERVAL", 0.1)
    host_home, worker_home = Home(tmp_path / "host"), Home(tmp_path / "worker")
    for home in (host_home, worker_home):
        home.claim()
        home.prepare()
    monkeypatch.setenv("DPR_HOME", str(host_home.path))
    hosting = Hosting.start(host_home)
    worker = None
    try:
        def approve_when_asked() -> None:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                waiting = hosting.waiting()
                if waiting:
                    assert hosting.decide("approve", waiting[0]["ref"]) == "approved"
                    return
                time.sleep(0.1)

        approver = threading.Thread(target=approve_when_asked)
        approver.start()
        monkeypatch.setenv("DPR_HOME", str(worker_home.path))
        Joined.enroll(worker_home, hosting.code)
        approver.join(20)
        worker = Joined.start(worker_home)
        assert worker.state() == "connected"

        assert hosting.decide("kick", "w1") == "removed"
        deadline = time.monotonic() + 15
        while worker.child.alive():
            assert time.monotonic() < deadline, "a removed worker must stop"
            time.sleep(0.1)
        assert worker.state() == "removed"
        import asyncio
        assert not any(view.online for view in asyncio.run(hosting.machines()).workers)

        # Its saved credentials are refused, even after the host restarts.
        hosting.stop()
        monkeypatch.setenv("DPR_HOME", str(host_home.path))
        hosting = Hosting.start(host_home)
        monkeypatch.setenv("DPR_HOME", str(worker_home.path))
        with pytest.raises(RuntimeError, match="removed by the host"):
            Joined.start(worker_home)
        assert not worker_home.worker.exists(), "rejected credentials are forgotten"
        assert json.loads((host_home.host / "claims.json").read_text())["retired"] == ["w1"]
    finally:
        if worker is not None:
            worker.stop()
        hosting.stop()
