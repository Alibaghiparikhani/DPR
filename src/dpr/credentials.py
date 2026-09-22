"""Cluster credentials: a private CA plus the coordinator, owner and worker identities.

Everything is generated with the `openssl` tool into a staging directory and moved into
place only when complete, so an interrupted run never leaves a half-made cluster.  The
CA private key is discarded afterwards: nobody, including the host, can mint new
identities later.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time

# Every node verifies the coordinator under this name, whatever address it dials, so a
# host whose LAN address changes keeps a valid certificate.
SERVER_NAME = "dpr-coordinator"
DEFAULT_DAYS = 825

_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")

_CONFIG = """\
[req]
distinguished_name = dn
prompt = no
[dn]
CN = dpr
[ca]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
"""


def openssl() -> str | None:
    """The openssl executable, including Git for Windows' copy when it is not on PATH."""
    found = shutil.which("openssl")
    if found or os.name != "nt":
        return found
    bases = [os.environ.get("ProgramFiles"), os.environ.get("ProgramW6432"),
             os.environ.get("ProgramFiles(x86)")]
    local = os.environ.get("LOCALAPPDATA")
    if local:
        bases.append(os.path.join(local, "Programs"))
    for base in filter(None, bases):
        for relative in (("Git", "mingw64", "bin", "openssl.exe"),
                         ("Git", "usr", "bin", "openssl.exe")):
            candidate = Path(base).joinpath(*relative)
            if candidate.is_file():
                return str(candidate)
    return None


def validate_identity(value: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValueError(f"invalid identity {value!r}: use 1-64 letters, digits, '.', '_' or '-'")
    return value


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def create(directory: Path, *, workers: list[str], client: str,
           days: int = DEFAULT_DAYS) -> dict:
    """Create a complete credential set in `directory`, which must not hold files yet.

    Returns a summary: identities, and creation/expiry times (epoch seconds).
    """
    for identity in [*workers, client]:
        validate_identity(identity)
    if len(set(workers)) != len(workers):
        raise ValueError("worker identities must be unique")
    if client in workers or "coordinator" in [*workers, client]:
        raise ValueError("identities must be distinct from each other and from 'coordinator'")
    if type(days) is not int or days < 1:
        raise ValueError("days must be a positive integer")
    tool = openssl()
    if tool is None:
        raise RuntimeError("openssl not found")

    directory = Path(directory)
    if directory.exists() and any(directory.iterdir()):
        raise FileExistsError(f"not empty: {directory}")
    staging = directory.with_name(directory.name + ".partial")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, mode=0o700)
    try:
        _generate(tool, staging, workers=workers, client=client, days=days)
        if directory.exists():
            directory.rmdir()
        os.replace(staging, directory)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    created = time.time()
    return {"workers": list(workers), "client": client,
            "created": created, "expires": created + days * 86400}


def _generate(tool: str, root: Path, *, workers: list[str], client: str, days: int) -> None:
    def run(*args: str) -> None:
        kwargs: dict = {}
        if os.name == "posix":
            kwargs["umask"] = 0o077
        result = subprocess.run([tool, *args], cwd=root, capture_output=True, text=True,
                                check=False, **kwargs)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise RuntimeError(f"openssl failed: {detail[-1] if detail else result.returncode}")

    config = root / "openssl.cnf"
    config.write_text(_CONFIG, encoding="ascii")
    run("ecparam", "-name", "prime256v1", "-out", "ec.param")
    run("req", "-config", config.name, "-x509", "-extensions", "ca", "-newkey", "ec:ec.param",
        "-nodes", "-days", str(days), "-subj", "/CN=dpr-ca", "-keyout", "ca.key", "-out", "ca.pem")

    def issue(stem: str, names: list[str], usage: str) -> None:
        san = ",".join(f"IP:{name}" if _is_ip(name) else f"DNS:{name}" for name in names)
        (root / f"{stem}.ext").write_text(
            f"basicConstraints = critical, CA:FALSE\n"
            f"keyUsage = critical, digitalSignature\n"
            f"extendedKeyUsage = {usage}\n"
            f"subjectAltName = {san}\n", encoding="ascii")
        run("req", "-config", config.name, "-new", "-newkey", "ec:ec.param", "-nodes",
            "-subj", f"/CN={names[0]}", "-keyout", f"{stem}.key", "-out", f"{stem}.csr")
        run("x509", "-req", "-in", f"{stem}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-set_serial", str(secrets.randbits(63)), "-days", str(days),
            "-extfile", f"{stem}.ext", "-out", f"{stem}.pem")

    jobs = [("coordinator", [SERVER_NAME, "localhost", "127.0.0.1"], "serverAuth"),
            (client, [client], "clientAuth")]
    jobs += [(worker, [worker], "clientAuth, serverAuth") for worker in workers]
    with ThreadPoolExecutor(max_workers=min(8, len(jobs))) as pool:
        for future in [pool.submit(issue, *job) for job in jobs]:
            future.result()

    auth: dict[str, str] = {}
    for identity in [*workers, client]:
        secret = secrets.token_bytes(32).hex()
        auth[identity] = secret
        (root / f"{identity}.secret").write_text(secret + "\n", encoding="ascii")
    (root / "auth.json").write_text(json.dumps(auth, indent=2, sort_keys=True) + "\n",
                                    encoding="utf-8")

    for leftover in ("ca.key", "ec.param", "openssl.cnf", "ca.srl"):
        (root / leftover).unlink(missing_ok=True)
    for pattern in ("*.csr", "*.ext"):
        for path in root.glob(pattern):
            path.unlink(missing_ok=True)
    for path in root.iterdir():
        os.chmod(path, 0o600)
    os.chmod(root, 0o700)
