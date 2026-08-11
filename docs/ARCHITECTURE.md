# Architecture

RaftKV is deliberately narrow: one replicated in-memory keyspace, one Raft
group, a small RESP2 command surface, and local durable logs. That keeps the
networking, consensus, storage, and overload boundaries visible instead of
hiding them behind a framework.

## Components and ownership

| Component | Responsibility | Concurrency model |
| --- | --- | --- |
| RESP decoder/encoder | Incremental binary-safe framing and response serialization | Netty event loop |
| Command execution handler | Validation, offload, in-order completion, backpressure | Event loop plus bounded worker handoff |
| Command executor | Starts command futures without blocking I/O threads | Fixed pool; 16 threads by default |
| Raft node | Term, role, voting, replication, commit index, apply order | One scheduled executor thread per node |
| Raft transport | Multiplexed request/response RPCs between members | Separate Netty listener and connections |
| Group-commit log | Coalesces append requests and forces them to stable storage | Dedicated storage worker |
| State machine | Binary key/value application of committed commands | Mutated in Raft apply order; concurrent reads are safe |

The separation matters. A slow disk cannot block a client socket's event loop,
and a slow client cannot mutate Raft state directly. Transport callbacks are
marshalled back to the owning Raft executor before they touch consensus state.

## Client request path

1. A Netty event loop incrementally decodes one RESP array of bulk strings.
   Fragmented frames and multiple pipelined frames are supported.
2. Protocol limits reject a request with more than 1,024 arguments, a bulk
   string over 16 MiB, or a complete frame over 32 MiB.
3. The request is parsed into a typed command and submitted to a bounded
   command queue. Saturation returns `BUSY command queue is full`.
4. `PING` and `ECHO` complete locally. Data commands create asynchronous Raft
   operations; worker threads do not wait for disk or quorum I/O.
5. Completions return to the channel's event loop. A per-connection queue emits
   responses in request order, even if the underlying futures finish in a
   different order.

Backpressure has five layers:

- The fixed command executor has a bounded queue.
- Each Raft node admits at most 8,192 client operations across its connections
  and at most 8,192 uncommitted mutation entries while it is leader.
- At 512 pending responses on a connection, automatic reads pause; they resume
  at 256.
- Netty's outbound write-buffer watermarks pause reads while the channel is not
  writable (64 KiB high, 32 KiB low by default).
- The group-commit worker has a bounded 8,192-request admission queue. If it is
  full, a leader rejects the new mutation with `TRYAGAIN` before publishing a
  log entry; saturation is not treated as a disk failure.

Together these controls bound admitted data operations and pending responses on
each connection. Open connections remain an operator-level resource limit and
are not globally capped by the server.

## Mutation path

`SET` and `DEL` are leader-only operations.

1. The leader assigns the next log index and its current term.
2. Its local group-commit log appends the frame and includes it in an `fsync`.
3. The leader sends `AppendEntries` batches to followers. A follower reports a
   match only after its corresponding append is durable.
4. Once the entry is durably matched by a majority, the leader advances its
   commit index, persists metadata, and applies entries in index order.
5. Only then does the client future complete.

The default group-commit policy forces when either 64 pending entries have been
collected or the oldest has waited 2 ms. Those controls are exposed as
`--group-commit-batch` and `--group-commit-delay-ms`. Group commit amortizes
storage flush cost; it does not weaken the acknowledgement rule.

## Read path

`GET` is also leader-only. Before returning a value, the leader verifies that
it has committed an entry in its current term and obtains same-term heartbeat
acknowledgements from a majority. The value then comes from the locally applied
state machine. This quorum confirmation prevents an isolated former leader from
serving a stale result.

This implementation favors a direct, understandable read barrier over lease
reads. Consequently, healthy `GET` latency includes a quorum network round trip.
No follower or stale-read mode is exposed.

## Elections and replication

The default randomized election timeout is 450–900 ms; leaders send heartbeats
every 120 ms. A candidate persists its term and vote before participating in an
election. Log freshness is checked against the last durable index and term.

Append replication is bounded to batches of 64 entries. Followers reject gaps
or term mismatches and return a conflict index so the leader can backtrack.
Entries are applied strictly through the commit index; an uncommitted suffix may
be replaced after a leadership change.

The membership is static. `--peers` is the complete `id=host:port` map and must
include the local `--node-id`. Every member must start with the same map.

## Storage and recovery

Each Raft log entry is stored as a versioned frame with a length and CRC32C
checksum. The group-commit layer writes one or more frames, calls
`FileChannel.force(false)`, and then completes their durability futures.

At startup, the WAL is scanned from the beginning:

- complete frames must have valid headers, checksums, payloads, and contiguous
  indexes;
- an incomplete or checksum-bad final frame is treated as a torn tail and
  truncated to the last valid boundary;
- corruption before the final frame is fatal and requires operator action.

Term, vote, and commit metadata are persisted separately through an atomic
replacement; both the temporary file and its containing directory are forced
before the transition is accepted. A node fails closed if its filesystem cannot
provide those primitives. Recovery also rejects metadata whose term is behind
the last retained log term. The state machine is in memory and is reconstructed
by replaying committed log entries. There is no snapshot or prefix compaction
yet, so the complete retained log remains part of restart cost.

## Network layout

Client RESP and internal Raft RPCs use separate listeners. In Compose every
container uses `6379` for RESP and `7000` for Raft; only the RESP listener is
published to the host. Docker DNS names form the static membership:

```text
1=raftkv-1:7000,2=raftkv-2:7000,3=raftkv-3:7000
```

On a single host without containers, use distinct Raft ports because all
processes share one network namespace. The examples in the main README use
`7001`, `7002`, and `7003`.

## Intentional boundaries

RaftKV currently has no membership changes, snapshots, sharding, follower
reads, authentication, TLS, request deduplication, or multi-command
transactions. These are meaningful distributed-systems features rather than
small switches; leaving them explicit makes the guarantees of the implemented
core easier to audit.
