"""Persistent per-context Python execution process.

The worker controller communicates only through bounded JSON request/result files
and a dedicated control channel that is never exposed as user stdin.  Arbitrary
Python values are deserialized inside this user-code process, never in the worker
controller.
"""
from __future__ import annotations

import contextlib
import ctypes
import hashlib
import io
import json
_json_dumps = json.dumps
_json_loads = json.loads
import math
import os
from pathlib import Path
import pickle
import signal
import sys
import threading
import traceback
import types


class _CappedText(io.TextIOBase):
    def __init__(self, limit: int) -> None:
        self.limit = max(0, int(limit))
        self._data = bytearray()
        self.truncated = False

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            text = str(text)
        raw = text.encode("utf-8", "replace")
        remaining = self.limit - len(self._data)
        if remaining > 0:
            self._data.extend(raw[:remaining])
        if len(raw) > max(0, remaining):
            self.truncated = True
        return len(text)

    @property
    def text(self) -> str:
        return bytes(self._data).decode("utf-8", "replace")


def _future_flags(source):
    """Read compiler semantics from the original module, not a split fragment."""
    import __future__
    code = compile(source, "<dpr-module-flags>", "exec", dont_inherit=True)
    mask = 0
    for name in __future__.all_feature_names:
        mask |= getattr(__future__, name).compiler_flag
    return code.co_flags & mask


def _parent_death_guard() -> None:
    if sys.platform.startswith("linux"):
        try:
            libc = ctypes.CDLL(None)
            libc.prctl(1, signal.SIGKILL)  # PR_SET_PDEATHSIG
            if os.getppid() == 1:
                os._exit(127)
        except Exception:
            pass
        return
    if sys.platform == "darwin":
        # F61: macOS has no PR_SET_PDEATHSIG. Poll the inherited parent PID and
        # terminate promptly if launchd becomes our parent after worker death.
        parent_pid = os.getppid()
        def watch_parent_macos():
            import time
            while True:
                time.sleep(0.25)
                current = os.getppid()
                if current == 1 or current != parent_pid:
                    os._exit(127)
        threading.Thread(target=watch_parent_macos, name="dpr-parent-watch", daemon=True).start()
        return
    if os.name == "nt":
        try:
            kernel32 = ctypes.windll.kernel32
            parent = kernel32.OpenProcess(0x00100000, False, os.getppid())  # SYNCHRONIZE
            if not parent:
                os._exit(127)
            def watch_parent():
                try:
                    kernel32.WaitForSingleObject(parent, 0xFFFFFFFF)
                finally:
                    kernel32.CloseHandle(parent)
                os._exit(127)
            threading.Thread(target=watch_parent, daemon=True).start()
        except Exception:
            pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _decode_inert(node):
    if type(node) is not dict or set(node) != {"t", "v"}:
        raise ValueError("invalid isolated value encoding")
    kind, value = node["t"], node["v"]
    if kind == "none":
        if value is not None: raise ValueError("invalid none encoding")
        return None
    if kind == "bool":
        if type(value) is not bool: raise ValueError("invalid bool encoding")
        return value
    if kind == "int":
        if type(value) is not str or len(value.lstrip("-")) > 4096 or not value.lstrip("-").isdigit():
            raise ValueError("invalid int encoding")
        return int(value)
    if kind == "float":
        if type(value) is not str or len(value) > 128: raise ValueError("invalid float encoding")
        return float(value) if value in {"nan", "inf", "-inf"} else float.fromhex(value)
    if kind == "str":
        if type(value) is not str: raise ValueError("invalid str encoding")
        return value
    if kind == "bytes":
        if type(value) is not str or len(value) % 2: raise ValueError("invalid bytes encoding")
        return bytes.fromhex(value)
    if kind in {"list", "set"}:
        if type(value) is not list: raise ValueError("invalid container encoding")
        items = [_decode_inert(item) for item in value]
        return items if kind == "list" else set(items)
    if kind == "dict":
        if type(value) is not list: raise ValueError("invalid dictionary encoding")
        return {_decode_inert(key): _decode_inert(item) for key, item in value}
    if kind == "tuple":
        if type(value) is not list: raise ValueError("invalid tuple encoding")
        return tuple(_decode_inert(item) for item in value)
    if kind == "frozenset":
        if type(value) is not list: raise ValueError("invalid frozenset encoding")
        return frozenset(_decode_inert(item) for item in value)
    raise ValueError("unsupported isolated value encoding")


def _bounded_read(path: Path, size: int, sha256: str, maximum: int) -> bytes:
    if type(size) is not int or size < 0 or size > maximum:
        raise ValueError("input value size out of bounds")
    if type(sha256) is not str or len(sha256) != 64:
        raise ValueError("invalid input digest")
    hasher = hashlib.sha256()
    data = bytearray()
    with path.open("rb") as handle:
        while len(data) < size:
            chunk = handle.read(min(256 * 1024, size - len(data)))
            if not chunk:
                raise ValueError("input value truncated")
            data.extend(chunk); hasher.update(chunk)
        if handle.read(1):
            raise ValueError("input value larger than declared")
    if hasher.hexdigest() != sha256:
        raise ValueError("input value digest mismatch")
    return bytes(data)


def _load_input(desc: dict, maximum: int):
    required = {"path", "size_bytes", "sha256", "serialization"}
    if type(desc) is not dict or set(desc) != required:
        raise ValueError("invalid input descriptor")
    raw = _bounded_read(Path(desc["path"]), desc["size_bytes"], desc["sha256"], maximum)
    if desc["serialization"] == "dpr-json-v1":
        node = _json_loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        return _decode_inert(node)
    if desc["serialization"] == "pickle-v1":
        # This process is already the user-code isolation boundary.  Authenticated
        # peer bytes are never unpickled by the worker controller itself.
        return pickle.loads(raw)
    raise ValueError("unsupported runtime serialization")


def _write_payload(path: Path, value, maximum: int) -> tuple[int, str]:
    raw = pickle.dumps(value, protocol=5)
    if len(raw) > maximum:
        raise ValueError("context output exceeds configured value bound")
    digest = hashlib.sha256(raw).hexdigest()
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("xb") as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    os.replace(tmp, path)
    return len(raw), digest


def _write_result(path: Path, result: dict, maximum: int) -> None:
    raw = _json_dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if len(raw) > maximum:
        raw = _json_dumps({
            "status": "internal_error", "message": "context result exceeded configured bound"
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("xb") as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    os.replace(tmp, path)


# State is private to this context process, never inserted into user globals.
_binding_versions = {}
_applied_bindings = set()
_module_initialized = False
_module_future_flags = 0


def _handle(namespace: dict, request_path: Path) -> None:
    # The controller creates this file, but still bound the read before allocation:
    # local corruption must not turn context IPC into an unbounded memory read.
    hard_limit = 2 * 1024 * 1024
    with request_path.open("rb") as handle:
        raw = handle.read(hard_limit + 1)
    if len(raw) > hard_limit:
        raise ValueError("context request exceeds hard bootstrap bound")
    request = _json_loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    required = {
        "package_root", "source", "definitions", "inputs", "outputs", "snapshots",
        "result_path", "result_limit", "value_limit", "stdout_limit", "stderr_limit",
        "binding_events", "input_ids",
    }
    if type(request) is not dict or set(request) - {"filename"} != required:
        raise ValueError("invalid context request schema")
    result_path = Path(request["result_path"])
    result_limit = int(request["result_limit"])
    value_limit = int(request["value_limit"])
    package_root = Path(request["package_root"]).resolve()
    global _module_initialized, _module_future_flags
    if not _module_initialized:
        namespace["__file__"] = str(package_root / request.get("filename", "main.py"))
        sys.argv = [request.get("filename", "main.py")]
        # FIXES F42: the verified package already carries the exact full module.
        # Read its flags once rather than adding a second, mutable IPC flag source.
        _module_future_flags = _future_flags(Path(namespace["__file__"]).read_bytes())
        _module_initialized = True
    root_text = str(package_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)

    inputs = request["inputs"]
    if type(inputs) is not dict:
        raise ValueError("context inputs must be object")
    for name, desc in inputs.items():
        if type(name) is not str or not name.isidentifier():
            raise ValueError("context input name is invalid")
        ident = request["input_ids"][name]
        if _binding_versions.get(name) != ident:
            namespace[name] = _load_input(desc, value_limit)
            _binding_versions[name] = ident

    definitions = request["definitions"]
    source = request["source"]
    if type(definitions) is not list or not all(type(item) is str for item in definitions):
        raise ValueError("context definitions are invalid")
    if type(source) is not str:
        raise ValueError("context source is invalid")

    stdout = _CappedText(int(request["stdout_limit"]))
    stderr = _CappedText(int(request["stderr_limit"]))
    clean_exit = False
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            # Definition visibility precedes the owning statement. Applying it
            # after that statement (guide step 2) is too late for a direct call.
            for event in request["binding_events"]:
                if event["id"] in _applied_bindings or event["kind"] == "alias":
                    continue
                if event["kind"] == "definition":
                    exec(compile(event["source"], request.get("filename", "<dpr-context-definition>"), "exec", dont_inherit=True, flags=_module_future_flags), namespace, namespace)
                elif event["kind"] == "runtime_task_import":
                    namespace[event["name"]] = lambda function: function
                _applied_bindings.add(event["id"])
                _binding_versions[event["name"]] = event["id"]
            if definitions:
                code = compile("\n".join(definitions), "<dpr-context-definitions>", "exec", dont_inherit=True, flags=_module_future_flags)
                exec(code, namespace, namespace)
            code = compile(source, request.get("filename", "<dpr-context-task>"), "exec", dont_inherit=True, flags=_module_future_flags)
            try:
                exec(code, namespace, namespace)
            except SystemExit as exc:
                if exc.code not in (None, 0):
                    raise
                clean_exit = True
            for event in request["binding_events"]:
                if event["kind"] == "alias":
                    source_name = event["source_name"]
                    namespace[event["name"]] = (namespace[source_name] if source_name in namespace
                                                else getattr(__import__("builtins"), source_name))
                    _binding_versions[event["name"]] = event["id"]

        produced: dict[str, dict[str, object]] = {}
        outputs = request["outputs"]
        if type(outputs) is not list:
            raise ValueError("context outputs must be list")
        for item in outputs:
            if type(item) is not dict or set(item) != {"value_id", "name", "path"}:
                raise ValueError("invalid context output descriptor")
            value_id, name, path = item["value_id"], item["name"], Path(item["path"])
            if type(value_id) is not str or type(name) is not str or not name.isidentifier():
                raise ValueError("invalid context output identity")
            if name not in namespace:
                raise RuntimeError(f"expected context output {name!r} was not produced")
            _binding_versions[name] = value_id
            size, digest = _write_payload(path, namespace[name], value_limit)
            produced[value_id] = {"size_bytes": size, "sha256": digest, "serialization": "pickle-v1"}

        snapshots: list[dict[str, object]] = []
        requested_snapshots = request["snapshots"]
        if type(requested_snapshots) is not list:
            raise ValueError("context snapshots must be list")
        for item in requested_snapshots:
            if type(item) is not dict or set(item) != {"value_id", "object_state_id", "name", "path"}:
                raise ValueError("invalid context snapshot descriptor")
            name, path = item["name"], Path(item["path"])
            if type(name) is not str or not name.isidentifier() or name not in namespace:
                raise RuntimeError("context snapshot reference is unavailable")
            size, digest = _write_payload(path, namespace[name], value_limit)
            snapshots.append({
                "value_id": item["value_id"], "object_state_id": item["object_state_id"],
                "size_bytes": size, "sha256": digest, "serialization": "pickle-v1",
            })
        _write_result(result_path, {
            "status": "success", "clean_exit": clean_exit, "outputs": produced, "snapshots": snapshots,
            "stdout": stdout.text, "stderr": stderr.text,
            "stdout_truncated": stdout.truncated, "stderr_truncated": stderr.truncated,
        }, result_limit)
    except Exception as error:
        _write_result(result_path, {
            "status": "python_exception",
            "exception_type": f"{type(error).__module__}.{type(error).__qualname__}",
            "message": str(error), "traceback": traceback.format_exc(limit=50),
            "stdout": stdout.text, "stderr": stderr.text,
            "stdout_truncated": stdout.truncated, "stderr_truncated": stderr.truncated,
        }, result_limit)


def _control_stream():
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--control-fd" and os.name == "posix":
        try:
            fd = int(args[1])
        except ValueError as error:
            raise RuntimeError("invalid context control fd") from error
        if fd < 3:
            raise RuntimeError("context control fd must not alias stdio")
        return os.fdopen(fd, "rb", buffering=0, closefd=True)
    if args == ["--control-stdin"] and os.name == "nt":
        # Windows compatibility for F53: capture the bootstrap control stream, then
        # detach user code from it before executing any source.  POSIX uses a true
        # inherited extra fd and starts with fd 0 already attached to DEVNULL.
        control = sys.stdin.buffer
        sys.stdin = open(os.devnull, "r", encoding="utf-8")
        return control
    raise RuntimeError("missing context control channel")


def main() -> int:
    _parent_death_guard()
    try:
        control = _control_stream()
    except Exception:
        return 122
    stub = types.ModuleType("dag_runtime")
    stub.task = lambda function: function
    sys.modules["dag_runtime"] = stub
    module = types.ModuleType("__main__")
    namespace = module.__dict__
    namespace.update(__package__=None, task=lambda function: function)
    sys.modules["__main__"] = module
    with control:
        for raw_line in control:
            line = raw_line.rstrip(b"\r\n")
            if line == b"__DPR_SHUTDOWN__":
                return 0
            if not line or len(line) > 8192:
                return 120
            try:
                path = Path(line.decode("utf-8"))
                _handle(namespace, path)
            except Exception:
                # If even request/result routing is malformed there may be no safe result
                # path.  Treat it as a context-process failure rather than continuing in
                # an unknown IPC state.
                return 121
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
