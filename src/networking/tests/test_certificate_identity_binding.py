from __future__ import annotations

from networking.coordinator_service import _peer_certificate_identities
from worker.data_plane import _peer_identities


class _SSLObject:
    def __init__(self, cert):
        self._cert = cert

    def getpeercert(self):
        return self._cert


class _Writer:
    def __init__(self, cert):
        self._ssl = _SSLObject(cert)

    def get_extra_info(self, name):
        return self._ssl if name == "ssl_object" else None


def _cert(*, san=(), cn="legacy"):
    return {
        "subjectAltName": tuple(san),
        "subject": ((('commonName', cn),),),
    }


def test_control_certificate_san_takes_precedence_over_conflicting_common_name():
    identities = _peer_certificate_identities(
        _Writer(_cert(san=(("DNS", "worker-A"),), cn="admin"))
    )
    assert identities == frozenset({"worker-A"})


def test_control_certificate_common_name_is_only_fallback_when_san_absent():
    assert _peer_certificate_identities(_Writer(_cert(san=(), cn="worker-A"))) == frozenset({"worker-A"})


def test_data_plane_certificate_san_takes_precedence_over_conflicting_common_name():
    identities = _peer_identities(
        _Writer(_cert(san=(("DNS", "worker-A"),), cn="worker-B"))
    )
    assert identities == frozenset({"worker-A"})


def test_data_plane_certificate_common_name_is_only_fallback_when_san_absent():
    assert _peer_identities(_Writer(_cert(san=(), cn="worker-A"))) == frozenset({"worker-A"})
