from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess

import pytest


@dataclass(frozen=True)
class TestCertificates:
    root: Path
    ca: Path
    server_cert: Path
    server_key: Path
    rogue_ca: Path
    rogue_server_cert: Path
    rogue_server_key: Path

    def cert(self, node: str) -> Path:
        return self.root / f"{node}.pem"

    def key(self, node: str) -> Path:
        return self.root / f"{node}.key"


def _run(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["openssl", *args], cwd=cwd, check=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _ca(root: Path, name: str, cn: str) -> tuple[Path, Path]:
    key = root / f"{name}.key"
    cert = root / f"{name}.pem"
    _run(root, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", f"/CN={cn}", "-keyout", str(key), "-out", str(cert))
    return cert, key


def _signed(root: Path, stem: str, cn: str, ca: Path, ca_key: Path, ext: str) -> tuple[Path, Path]:
    key = root / f"{stem}.key"
    csr = root / f"{stem}.csr"
    cert = root / f"{stem}.pem"
    extfile = root / f"{stem}.ext"
    extfile.write_text(ext)
    _run(root, "req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={cn}",
         "-keyout", str(key), "-out", str(csr))
    _run(root, "x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key),
         "-CAcreateserial", "-days", "2", "-extfile", str(extfile), "-out", str(cert))
    return cert, key


@pytest.fixture(scope="session")
def tls_certs(tmp_path_factory: pytest.TempPathFactory) -> TestCertificates:
    if shutil.which("openssl") is None:
        pytest.skip("real TLS integration tests require openssl to generate temporary certificates")
    root = tmp_path_factory.mktemp("dpr-tls")
    ca, ca_key = _ca(root, "ca", "DPR Test CA")
    server, server_key = _signed(
        root, "server", "localhost", ca, ca_key,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    for node in ("W1", "W2", "W3", "W4"):
        _signed(
            root, node, node, ca, ca_key,
            f"subjectAltName=DNS:{node}\nextendedKeyUsage=clientAuth,serverAuth\n",
        )

    rogue_ca, rogue_ca_key = _ca(root, "rogue-ca", "Rogue Test CA")
    rogue_server, rogue_server_key = _signed(
        root, "rogue-server", "localhost", rogue_ca, rogue_ca_key,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    _signed(root, "rogue-client", "W1", rogue_ca, rogue_ca_key, "extendedKeyUsage=clientAuth\n")
    return TestCertificates(root, ca, server, server_key, rogue_ca, rogue_server, rogue_server_key)
