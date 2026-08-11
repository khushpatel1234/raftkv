# Benchmarking

The repository includes a small standard-library Python RESP client so a run's
inputs and output schema can live beside the code. Its purpose is repeatable
comparison and regression detection. It is not a substitute for a calibrated
load generator when the Python process or loopback networking becomes the
bottleneck.

## Provenance of the reference numbers

These measurements were supplied in the project brief and are preserved here
without reinterpretation:

| Measurement | Recorded result | Known conditions | Missing provenance |
| --- | ---: | --- | --- |
| GET throughput | 84.7K ops/s | Netty RESP server, 50 concurrent clients | Hardware, topology, payload/keyspace, duration, tool, commit |
| GET p50 | 0.55 ms | Same run as above | p95 and p99 |
| SET throughput | 924 ops/s | 3 Raft nodes, quorum-durable | Concurrency, hardware, payload, duration, tool, commit |
| Group-commit throughput change | 8.4× | Single-node comparison | Absolute rates and exact batching/workload |
| Group-commit p99 change | 85.8% lower | Same comparison | Absolute latency and exact batching/workload |

These are historical, supplied reference measurements. They were not produced
by the included Python client, have not been independently reproduced while
assembling this repository, and are not asserted by CI. Missing parameters make
them unsuitable as a baseline for another machine.

## Included driver

`bench/resp_bench.py` opens one TCP connection per client thread and keeps one
request in flight per connection. For each successful measured operation it
records end-to-end round-trip latency. The output is RFC-compatible CSV with:

```text
timestamp_utc,run_label,git_commit,operation,host,port,clients,pipeline,duration_s,keyspace,value_bytes,successes,errors,ops_per_sec,p50_ms,p95_ms,p99_ms
```

Percentiles use the nearest-rank method across successful requests. Throughput
is successful operations divided by the actual measurement interval. Server
errors, protocol errors, and connection failures contribute to one aggregate
`errors` field and are printed to standard error; each affected worker stops at
its first error, and a run with errors exits non-zero. For `GET`, the driver
first writes every key with a fixed-size value. `SET` workers cycle through one
shared configured keyspace. Preload and warmup operations are not included in
the row.

The driver validates every response. This catches accidentally targeting a
follower, missing preloaded data, and malformed protocol results instead of
quietly counting them as throughput.

## Reproducible local run

Start the same three-node topology and discover its leader:

```bash
./scripts/cluster.sh up
./scripts/smoke.sh
```

Use the leader port printed by the smoke test:

```bash
RAFTKV_BENCH_PORT=6379 ./bench/run.sh
```

The wrapper runs `GET` and then quorum-durable `SET` with these defaults:

| Parameter | Default |
| --- | ---: |
| Clients | 50 |
| Timed duration per operation | 30 seconds |
| Warmup operations per client | 100 |
| Keyspace | 10,000 |
| Value size | 128 bytes |
| Pipeline depth | 1 |

It writes a new timestamped file under `bench/results/` and refuses to overwrite
an existing file. The commit SHA is captured when the directory is a Git
checkout; the wrapper adds a `dirty` suffix when tracked or untracked changes
are present. All controls can be explicit for an archived run:

```bash
RAFTKV_BENCH_HOST=127.0.0.1 \
RAFTKV_BENCH_PORT=6379 \
RAFTKV_BENCH_CLIENTS=50 \
RAFTKV_BENCH_DURATION=60 \
RAFTKV_BENCH_WARMUP=200 \
RAFTKV_BENCH_KEYSPACE=10000 \
RAFTKV_BENCH_VALUE_SIZE=128 \
RAFTKV_BENCH_LABEL="m3-max-docker-3node" \
RAFTKV_BENCH_OUTPUT="bench/results/m3-max-docker-3node.csv" \
./bench/run.sh
```

To run only one operation or emit CSV to standard output:

```bash
python3 bench/resp_bench.py \
  --operation get --host 127.0.0.1 --port 6379 \
  --clients 50 --duration 30 --warmup 100 \
  --keyspace 10000 --value-size 128
```

## Comparing group commit

Use a single-member Raft group to isolate local WAL batching. Run the batched
and unbatched configurations as separate processes, with separate empty data
directories, on the same machine. The default batched configuration is:

```bash
java -jar target/raftkv.jar \
  --node-id=1 --client-host=127.0.0.1 --client-port=6379 \
  --raft-host=127.0.0.1 --raft-port=7000 \
  --peers=1=127.0.0.1:7000 --data-dir=data/bench-batched \
  --group-commit-batch=64 --group-commit-delay-ms=2
```

The no-batching comparison is:

```bash
java -jar target/raftkv.jar \
  --node-id=1 --client-host=127.0.0.1 --client-port=6379 \
  --raft-host=127.0.0.1 --raft-port=7000 \
  --peers=1=127.0.0.1:7000 --data-dir=data/bench-unbatched \
  --group-commit-batch=1 --group-commit-delay-ms=0
```

After each process elects itself, run the exact same `SET` command and archive
both CSV rows. Stop one process before starting the other so they never share a
port. Multiple repetitions with alternating configuration order reduce bias
from thermal state and filesystem cache history.

## Measurement checklist

Record enough context to explain or reproduce a result:

- exact Git commit and whether the worktree was dirty;
- JDK vendor/version and JVM options;
- CPU model/count, RAM, storage medium/filesystem, and OS/kernel;
- bare-metal versus container topology and Docker resource limits;
- client/server placement and network path;
- node count, durability mode, group-commit batch/delay;
- operation mix, key distribution, keyspace, value size, clients, pipelining;
- warmup, measured duration, repetitions, and errors;
- CPU saturation, GC activity, disk latency, and host contention.

Do not compare an in-process or single-node run with the quorum-durable
three-node path. Do not compare a pipeline depth of one with a pipelined tool as
though they measure the same latency/throughput tradeoff.

## CI policy

CI compiles the Python driver, runs Java tests, and verifies the container
build. It does not fail on throughput or latency thresholds. Shared CI runners
are too noisy for credible microbenchmark gates, and the supplied reference
measurements do not contain enough provenance to recreate their environment.
