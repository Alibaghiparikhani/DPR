from __future__ import annotations

import ssl
from unittest.mock import Mock, patch

from runtime_security import TlsCredentials, TlsPolicy


def _credentials():
    return TlsCredentials("node.pem", "node.key", "cluster-ca.pem")


def test_server_policy_requires_mutual_tls_and_tls13():
    fake = Mock()
    fake.options = 0
    with patch("runtime_security.tls.ssl.SSLContext", return_value=fake) as constructor:
        context = TlsPolicy(_credentials()).build_server_context()
    assert context is fake
    constructor.assert_called_once_with(ssl.PROTOCOL_TLS_SERVER)
    assert fake.minimum_version == ssl.TLSVersion.TLSv1_3
    assert fake.verify_mode == ssl.CERT_REQUIRED
    fake.load_verify_locations.assert_called_once_with(cafile="cluster-ca.pem")
    fake.load_cert_chain.assert_called_once_with(certfile="node.pem", keyfile="node.key")


def test_client_policy_requires_hostname_certificate_validation_and_tls13():
    fake = Mock()
    fake.options = 0
    with patch("runtime_security.tls.ssl.SSLContext", return_value=fake) as constructor:
        context = TlsPolicy(_credentials()).build_client_context()
    assert context is fake
    constructor.assert_called_once_with(ssl.PROTOCOL_TLS_CLIENT)
    assert fake.minimum_version == ssl.TLSVersion.TLSv1_3
    assert fake.check_hostname is True
    assert fake.verify_mode == ssl.CERT_REQUIRED
    fake.load_verify_locations.assert_called_once_with(cafile="cluster-ca.pem")
    fake.load_cert_chain.assert_called_once_with(certfile="node.pem", keyfile="node.key")


def test_tls_hardening_disables_compression():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    TlsPolicy._harden(context)
    assert context.minimum_version == ssl.TLSVersion.TLSv1_3
    assert context.options & ssl.OP_NO_COMPRESSION
