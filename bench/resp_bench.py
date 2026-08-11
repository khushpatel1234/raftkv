#!/usr/bin/env python3
"""Small, dependency-free RESP benchmark client for RaftKV.

The client uses one blocking connection per worker and one in-flight request
per connection. It is intended for reproducible comparisons, not as a claim
that the Python driver can saturate every server or network.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Sequence


class RespProtocolError(RuntimeError):
    """The peer returned a malformed or unsupported RESP response."""


class RespServerError(RuntimeError):
    """The peer returned a RESP error response."""


class RespConnection:
    def __init__(self, host: str, port: int, timeout: float) -> None:
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._socket.settimeout(timeout)
        self._stream = self._socket.makefile("rb")

    def close(self) -> None:
        try:
            self._stream.close()
        finally:
            self._socket.close()

    def execute(self, *parts: bytes | str | int) -> object:
        encoded = [part if isinstance(part, bytes) else str(part).encode() for part in parts]
        request = [f"*{len(encoded)}\r\n".encode()]
        for part in encoded:
            request.extend((f"${len(part)}\r\n".encode(), part, b"\r\n"))
        self._socket.sendall(b"".join(request))
        return _read_response(self._stream)


def _read_line(stream: BinaryIO) -> bytes:
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise RespProtocolError("truncated RESP line")
    return line[:-2]


def _read_response(stream: BinaryIO) -> object:
    marker = stream.read(1)
    if not marker:
        raise RespProtocolError("server closed the connection")
    payload = _read_line(stream)
    if marker == b"+":
        return payload.decode("utf-8", "replace")
    if marker == b"-":
        raise RespServerError(payload.decode("utf-8", "replace"))
    if marker == b":":
        return int(payload)
    if marker == b"$":
        length = int(payload)
        if length == -1:
            return None
        if length < -1:
            raise RespProtocolError(f"invalid bulk-string length {length}")
        data = stream.read(length)
        if len(data) != length or stream.read(2) != b"\r\n":
            raise RespProtocolError("truncated RESP bulk string")
        return data
    if marker == b"*":
        count = int(payload)
        if count == -1:
            return None
        if count < -1:
            raise RespProtocolError(f"invalid array length {count}")
        return [_read_response(stream) for _ in range(count)]
    raise RespProtocolError(f"unsupported RESP marker {marker!r}")


@dataclass
class WorkerStats:
    latencies_ns: list[int] = field(default_factory=list)
    successes: int = 0
    errors: int = 0
    finished_ns: int = 0
    message: str = ""


def percentile(sorted_values: Sequence[int], quantile: float) -> float | None:
    if not sorted_values:
        return None
    index = max(0, math.ceil(quantile * len(sorted_values)) - 1)
    return sorted_values[index] / 1_000_000.0


def validate_response(operation: str, response: object, value_size: int) -> None:
    if operation == "set" and response != "OK":
        raise RespProtocolError(f"SET returned {response!r}")
    if operation == "get":
        if not isinstance(response, bytes):
            raise RespProtocolError(f"GET returned {response!r}")
        if len(response) != value_size:
            raise RespProtocolError(
                f"GET returned {len(response)} bytes, expected {value_size}"
            )


def preload(args: argparse.Namespace, value: bytes) -> None:
    print(f"preloading {args.keyspace} keys through {args.host}:{args.port}...", file=sys.stderr)
    connection = RespConnection(args.host, args.port, args.timeout)
    try:
        for index in range(args.keyspace):
            response = connection.execute("SET", f"{args.key_prefix}:get:{index}", value)
            validate_response("set", response, args.value_size)
    finally:
        connection.close()
    print("preload complete", file=sys.stderr)


def run_worker(
    worker_id: int,
    args: argparse.Namespace,
    value: bytes,
    barrier: threading.Barrier,
    timing: dict[str, int],
    stats: WorkerStats,
) -> None:
    rng = random.Random(args.seed + worker_id)
    connection: RespConnection | None = None
    sequence = 0

    def invoke() -> object:
        nonlocal sequence
        if args.operation == "get":
            key = f"{args.key_prefix}:get:{rng.randrange(args.keyspace)}"
            return connection.execute("GET", key)  # type: ignore[union-attr]
        key = f"{args.key_prefix}:set:{worker_id}:{sequence % args.keyspace}"
        sequence += 1
        return connection.execute("SET", key, value)  # type: ignore[union-attr]

    try:
        connection = RespConnection(args.host, args.port, args.timeout)
        for _ in range(args.warmup):
            validate_response(args.operation, invoke(), args.value_size)

        barrier.wait(timeout=max(10.0, args.timeout * 2.0))
        deadline_ns = timing["deadline_ns"]
        while time.perf_counter_ns() < deadline_ns:
            started_ns = time.perf_counter_ns()
            response = invoke()
            latency_ns = time.perf_counter_ns() - started_ns
            validate_response(args.operation, response, args.value_size)
            stats.latencies_ns.append(latency_ns)
            stats.successes += 1
    except threading.BrokenBarrierError:
        stats.errors += 1
        stats.message = "worker start barrier was broken"
    except (OSError, RespProtocolError, RespServerError, ValueError) as exc:
        stats.errors += 1
        stats.message = str(exc)
        try:
            barrier.abort()
        except threading.BrokenBarrierError:
            pass
    finally:
        stats.finished_ns = time.perf_counter_ns()
        if connection is not None:
            connection.close()


def write_result(args: argparse.Namespace, row: dict[str, object]) -> None:
    fieldnames = list(row)
    if args.output == "-":
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerow(row)
        return

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    has_content = args.append and output.exists() and output.stat().st_size > 0
    mode = "a" if args.append else "w"
    with output.open(mode, newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, lineterminator="\n")
        if not has_content:
            writer.writeheader()
        writer.writerow(row)
    print(f"wrote {output}", file=sys.stderr)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return parsed


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=("get", "set"), required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=positive_int, default=6379)
    parser.add_argument("--clients", type=positive_int, default=50)
    parser.add_argument("--duration", type=positive_float, default=10.0)
    parser.add_argument("--warmup", type=nonnegative_int, default=100)
    parser.add_argument("--keyspace", type=positive_int, default=10_000)
    parser.add_argument("--value-size", type=positive_int, default=128)
    parser.add_argument("--timeout", type=positive_float, default=5.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--key-prefix", default="raftkv:bench")
    parser.add_argument("--output", default="-", help="CSV path, or - for stdout")
    parser.add_argument("--append", action="store_true", help="append to --output")
    parser.add_argument("--run-label", default="")
    parser.add_argument("--git-commit", default=os.environ.get("GIT_COMMIT", ""))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    value = b"x" * args.value_size

    try:
        if args.operation == "get":
            preload(args, value)
    except (OSError, RespProtocolError, RespServerError, ValueError) as exc:
        print(f"preload failed: {exc}", file=sys.stderr)
        return 2

    timing: dict[str, int] = {}

    def begin_measurement() -> None:
        started_ns = time.perf_counter_ns()
        timing["started_ns"] = started_ns
        timing["deadline_ns"] = started_ns + int(args.duration * 1_000_000_000)

    barrier = threading.Barrier(args.clients + 1, action=begin_measurement)
    stats = [WorkerStats() for _ in range(args.clients)]
    threads = [
        threading.Thread(
            target=run_worker,
            name=f"resp-worker-{worker_id}",
            args=(worker_id, args, value, barrier, timing, stats[worker_id]),
            daemon=True,
        )
        for worker_id in range(args.clients)
    ]
    for thread in threads:
        thread.start()

    try:
        barrier.wait(timeout=max(15.0, args.timeout * 3.0))
    except threading.BrokenBarrierError:
        for thread in threads:
            thread.join(timeout=args.timeout + 1.0)
        messages = sorted({item.message for item in stats if item.message})
        print("benchmark could not start: " + "; ".join(messages), file=sys.stderr)
        return 2

    started_ns = timing["started_ns"]

    for thread in threads:
        thread.join()

    successes = sum(item.successes for item in stats)
    errors = sum(item.errors for item in stats)
    latencies = sorted(latency for item in stats for latency in item.latencies_ns)
    finished_ns = max((item.finished_ns for item in stats), default=time.perf_counter_ns())
    elapsed_s = max((finished_ns - started_ns) / 1_000_000_000.0, 1e-9)

    def format_ms(value_ms: float | None) -> str:
        return "" if value_ms is None else f"{value_ms:.3f}"

    row: dict[str, object] = {
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "run_label": args.run_label,
        "git_commit": args.git_commit,
        "operation": args.operation.upper(),
        "host": args.host,
        "port": args.port,
        "clients": args.clients,
        "pipeline": 1,
        "duration_s": f"{elapsed_s:.3f}",
        "keyspace": args.keyspace,
        "value_bytes": args.value_size,
        "successes": successes,
        "errors": errors,
        "ops_per_sec": f"{successes / elapsed_s:.1f}",
        "p50_ms": format_ms(percentile(latencies, 0.50)),
        "p95_ms": format_ms(percentile(latencies, 0.95)),
        "p99_ms": format_ms(percentile(latencies, 0.99)),
    }
    write_result(args, row)

    for message in sorted({item.message for item in stats if item.message}):
        print(f"worker error: {message}", file=sys.stderr)
    return 0 if errors == 0 and successes > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
