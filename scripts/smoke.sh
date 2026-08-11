#!/usr/bin/env bash
set -Eeuo pipefail

# The Python snippet is intentionally dependency-free so the smoke test works
# without redis-cli. Override these variables for a non-Compose deployment.
export RAFTKV_HOST=${RAFTKV_HOST:-127.0.0.1}
export RAFTKV_PORTS=${RAFTKV_PORTS:-6379,6380,6381}
export RAFTKV_STARTUP_TIMEOUT=${RAFTKV_STARTUP_TIMEOUT:-30}

python3 - <<'PY'
import os
import socket
import sys
import time


class ServerError(RuntimeError):
    pass


def read_response(stream):
    marker = stream.read(1)
    if not marker:
        raise EOFError("server closed the connection")
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise ValueError("malformed RESP response")
    payload = line[:-2]
    if marker == b"+":
        return payload.decode("utf-8", "replace")
    if marker == b"-":
        raise ServerError(payload.decode("utf-8", "replace"))
    if marker == b":":
        return int(payload)
    if marker == b"$":
        length = int(payload)
        if length == -1:
            return None
        data = stream.read(length)
        if len(data) != length or stream.read(2) != b"\r\n":
            raise ValueError("truncated RESP bulk string")
        return data
    raise ValueError(f"unsupported RESP marker {marker!r}")


def command(port, *parts):
    encoded = [str(part).encode() if not isinstance(part, bytes) else part for part in parts]
    request = [f"*{len(encoded)}\r\n".encode()]
    for part in encoded:
        request.extend((f"${len(part)}\r\n".encode(), part, b"\r\n"))
    with socket.create_connection((host, port), timeout=1.5) as sock:
        sock.settimeout(2.0)
        sock.sendall(b"".join(request))
        with sock.makefile("rb") as stream:
            return read_response(stream)


host = os.environ["RAFTKV_HOST"]
try:
    ports = [int(value.strip()) for value in os.environ["RAFTKV_PORTS"].split(",") if value.strip()]
    timeout = float(os.environ["RAFTKV_STARTUP_TIMEOUT"])
except ValueError as exc:
    print(f"invalid smoke-test configuration: {exc}", file=sys.stderr)
    raise SystemExit(2)

if not ports or timeout <= 0:
    print("RAFTKV_PORTS must be non-empty and RAFTKV_STARTUP_TIMEOUT must be positive", file=sys.stderr)
    raise SystemExit(2)

deadline = time.monotonic() + timeout
key = f"raftkv:smoke:{int(time.time())}:{os.getpid()}"
value = "quorum-ok"
last_errors = {}

while time.monotonic() < deadline:
    for port in ports:
        try:
            pong = command(port, "PING")
            if pong != "PONG":
                raise RuntimeError(f"PING returned {pong!r}")
            result = command(port, "SET", key, value)
            if result != "OK":
                raise RuntimeError(f"SET returned {result!r}")
            actual = command(port, "GET", key)
            if actual != value.encode():
                raise RuntimeError(f"GET returned {actual!r}")
            print(f"PASS: node at {host}:{port} accepted a quorum-backed SET and returned the value")
            print(f"leader client endpoint: {host}:{port}")
            raise SystemExit(0)
        except (OSError, EOFError, ValueError, RuntimeError, ServerError) as exc:
            last_errors[port] = str(exc)
    time.sleep(0.25)

print(f"FAIL: no writable leader found within {timeout:g}s", file=sys.stderr)
for port in ports:
    print(f"  {host}:{port}: {last_errors.get(port, 'not attempted')}", file=sys.stderr)
raise SystemExit(1)
PY
