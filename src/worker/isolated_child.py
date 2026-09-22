"""Stdlib-only bootstrap for one isolated task child process.

The worker controller creates the request. Network bytes are never deserialized here.
"""
from __future__ import annotations

import ctypes
import json
_json_dumps = json.dumps
_json_loads = json.loads
import math
import hashlib
import os
import pickle
from pathlib import Path
import signal
import sys
import threading
import traceback
import types


def _future_flags(source):
    """Read compiler semantics from the original module, not a split fragment."""
    import __future__
    code = compile(source, "<dpr-module-flags>", "exec", dont_inherit=True)
    mask = 0
    for name in __future__.all_feature_names:
        mask |= getattr(__future__, name).compiler_flag
    return code.co_flags & mask


def _parent_death_guard() -> None:
    """Best-effort immediate-child containment if the worker itself dies abruptly."""
    if sys.platform.startswith("linux"):
        try:
            libc = ctypes.CDLL(None)
            PR_SET_PDEATHSIG = 1
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
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
        # A Job Object would require the worker to own every descendant created by
        # arbitrary user code. Batch 2 promises only immediate-child containment.
        # Have this child hold a synchronization handle to its worker parent so an
        # abrupt worker process death also retires the executor on Windows.
        try:
            kernel32 = ctypes.windll.kernel32
            SYNCHRONIZE = 0x00100000
            INFINITE = 0xFFFFFFFF
            parent = kernel32.OpenProcess(SYNCHRONIZE, False, os.getppid())
            if not parent:
                os._exit(127)
            def watch_parent():
                try:
                    kernel32.WaitForSingleObject(parent, INFINITE)
                finally:
                    kernel32.CloseHandle(parent)
                os._exit(127)
            threading.Thread(target=watch_parent, name="dpr-parent-watch", daemon=True).start()
        except Exception:
            pass


def _decode(node):
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
        if type(value) is not str: raise ValueError("invalid float encoding")
        return float.fromhex(value) if value not in {"nan", "inf", "-inf"} else float(value)
    if kind == "str":
        if type(value) is not str: raise ValueError("invalid str encoding")
        return value
    if kind == "bytes":
        if type(value) is not str or len(value) % 2: raise ValueError("invalid bytes encoding")
        return bytes.fromhex(value)
    if kind in {"list", "set"}:
        if type(value) is not list: raise ValueError("invalid container encoding")
        items = [_decode(item) for item in value]
        return items if kind == "list" else set(items)
    if kind == "dict":
        if type(value) is not list: raise ValueError("invalid dictionary encoding")
        return {_decode(key): _decode(item) for key, item in value}
    if kind == "tuple":
        if type(value) is not list: raise ValueError("invalid tuple encoding")
        return tuple(_decode(item) for item in value)
    if kind == "frozenset":
        if type(value) is not list: raise ValueError("invalid frozenset encoding")
        return frozenset(_decode(item) for item in value)
    raise ValueError("unsupported isolated value encoding")


def _load_descriptor(desc, maximum):
    required = {"path", "size_bytes", "sha256", "serialization"}
    if type(desc) is not dict or set(desc) != required:
        raise ValueError("invalid runtime input descriptor")
    size = desc["size_bytes"]
    digest = desc["sha256"]
    if type(size) is not int or size < 0 or size > maximum:
        raise ValueError("runtime input size out of bounds")
    if type(digest) is not str or len(digest) != 64:
        raise ValueError("invalid runtime input digest")
    path = Path(desc["path"])
    hasher = hashlib.sha256(); data = bytearray()
    with path.open("rb") as handle:
        while len(data) < size:
            chunk = handle.read(min(256 * 1024, size - len(data)))
            if not chunk: raise ValueError("runtime input truncated")
            data.extend(chunk); hasher.update(chunk)
        if handle.read(1): raise ValueError("runtime input larger than declared")
    if hasher.hexdigest() != digest:
        raise ValueError("runtime input digest mismatch")
    raw = bytes(data)
    if desc["serialization"] == "dpr-json-v1":
        return _decode(_json_loads(raw.decode("utf-8")))
    if desc["serialization"] == "pickle-v1":
        # Deserialization occurs only inside this already-isolated user-code child.
        return pickle.loads(raw)
    raise ValueError("unsupported runtime input serialization")


def _encode(value, *, depth=0):
    if depth > 32:
        raise ValueError("isolated result nesting limit exceeded")
    if value is None: return {"t": "none", "v": None}
    if type(value) is bool: return {"t": "bool", "v": value}
    if type(value) is int:
        text = str(value)
        if len(text.lstrip("-")) > 4096:
            raise ValueError("isolated integer result exceeds digit limit")
        return {"t": "int", "v": text}
    if type(value) is float:
        text = "nan" if math.isnan(value) else ("inf" if value == math.inf else ("-inf" if value == -math.inf else value.hex()))
        return {"t": "float", "v": text}
    if type(value) is str: return {"t": "str", "v": value}
    if type(value) is bytes: return {"t": "bytes", "v": value.hex()}
    if type(value) in {list, set}:
        return {"t": type(value).__name__, "v": [_encode(item, depth=depth + 1) for item in value]}
    if type(value) is dict:
        return {"t": "dict", "v": [[_encode(key, depth=depth + 1), _encode(item, depth=depth + 1)]
                                   for key, item in value.items()]}
    if type(value) is tuple:
        return {"t": "tuple", "v": [_encode(item, depth=depth + 1) for item in value]}
    if type(value) is frozenset:
        items = [_encode(item, depth=depth + 1) for item in value]
        items.sort(key=lambda item: _json_dumps(item, sort_keys=True, separators=(",", ":")))
        return {"t": "frozenset", "v": items}
    raise TypeError(f"unsupported isolated result type: {type(value).__name__}")


def _write_result(path: Path, record: dict, limit: int) -> None:
    data = _json_dumps(record, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    if len(data) > limit:
        record = {"status": "internal_error", "message": "executor result exceeded configured bound"}
        data = _json_dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def main() -> int:
    _parent_death_guard()
    stub = types.ModuleType("dag_runtime")
    stub.task = lambda function: function
    sys.modules["dag_runtime"] = stub
    if len(sys.argv) != 3:
        return 120
    request_path, result_path = map(Path, sys.argv[1:])
    try:
        request_bytes = request_path.read_bytes()
        if len(request_bytes) > 2 * 1024 * 1024:
            return 121
        request = _json_loads(request_bytes.decode("utf-8"))
        required = {"package_root", "source", "definitions", "inputs", "outputs", "result_limit"}
        if type(request) is not dict or set(request) - {"filename", "value_limit"} != required:
            return 122
        package_root = Path(request["package_root"]).resolve()
        sys.path.insert(0, str(package_root))
        module = types.ModuleType("__main__")
        namespace = module.__dict__
        namespace.update(__file__=str(package_root / request.get("filename", "main.py")),
                         __package__=None, task=lambda function: function)
        sys.modules["__main__"] = module
        sys.argv = [request.get("filename", "main.py")]
        if type(request["inputs"]) is not dict:
            return 123
        for name, node in request["inputs"].items():
            if type(name) is not str or not name.isidentifier():
                return 124
            if type(node) is dict and set(node) == {"path", "size_bytes", "sha256", "serialization"}:
                namespace[name] = _load_descriptor(
                    node, int(request.get("value_limit", 64 * 1024 * 1024)))
            else:
                namespace[name] = _decode(node)
        definitions = request["definitions"]
        if type(definitions) is not list or not all(type(item) is str for item in definitions):
            return 125
        source = request["source"]
        if type(source) is not str:
            return 126
        future_flags = _future_flags(Path(namespace["__file__"]).read_bytes())
        combined = "\n".join([*definitions, source])
        code = compile(combined, request.get("filename", "<dpr-isolated-task>"), "exec", dont_inherit=True, flags=future_flags)
        clean_exit = False
        try:
            exec(code, namespace, namespace)
        except SystemExit as exc:
            if exc.code not in (None, 0):
                raise
            clean_exit = True
        outputs = request["outputs"]
        if type(outputs) is not list:
            return 127
        encoded = {}
        for pair in outputs:
            if type(pair) is not list or len(pair) != 2 or not all(type(x) is str for x in pair):
                return 128
            value_id, name = pair
            if name not in namespace:
                raise RuntimeError(f"expected task output {name!r} was not produced")
            # FIXES F13/F21: the existing wire codec name is pickle-v1, whose
            # payload already uses protocol 5. Keep that negotiated tag; a new
            # pickle-v5/raw tag would also require out-of-scope data-plane changes.
            # Binary pickle preserves nested alias identity and stores bytes with
            # constant framing overhead rather than hex expansion.
            payload = pickle.dumps(namespace[name], protocol=5)
            # Values go to their own files, so they are bounded by the value limit
            # (as in context children); result_limit bounds only the small metadata
            # record below.  Bounding values by it too capped every task's output
            # at 1 MiB.
            if len(payload) > int(request.get("value_limit", 64 * 1024 * 1024)):
                # F11 code-reality note: the bound is detected in the child before
                # result metadata exists, so runtime.py cannot distinguish it from a
                # user ValueError unless the child emits the existing internal
                # resource marker explicitly.  Keep user exceptions semantically
                # separate and let the controller map this marker to non-retryable
                # RESOURCE_EXHAUSTED.
                _write_result(result_path, {
                    "status": "internal_error",
                    "message": "executor result exceeded configured bound",
                }, int(request["result_limit"]))
                return 0
            name_on_disk = f"output-{value_id}.bin"
            path = result_path.parent / name_on_disk
            with path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            encoded[value_id] = {"file": name_on_disk, "size_bytes": len(payload),
                                 "sha256": hashlib.sha256(payload).hexdigest(),
                                 "serialization": "pickle-v1"}
        _write_result(result_path, {"status": "success", "clean_exit": clean_exit, "outputs": encoded}, int(request["result_limit"]))
        return 0
    except Exception as error:
        tb = traceback.format_exc(limit=50)
        try:
            _write_result(result_path, {
                "status": "python_exception",
                "exception_type": f"{type(error).__module__}.{type(error).__qualname__}",
                "message": str(error),
                "traceback": tb,
            }, int(request.get("result_limit", 1024 * 1024)) if 'request' in locals() and type(request) is dict else 1024 * 1024)
        except Exception:
            return 129
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
