# Consistency and failure semantics

This document describes observable behavior for the default three-member
cluster. It distinguishes an operation's **state outcome** from the response a
client happened to receive—an essential distinction whenever connections and
processes can fail independently.

## Guarantees

- `SET` and `DEL` are linearized in the leader's committed Raft log.
- A successful mutation response is sent only after the entry is durable on a
  majority and applied by the leader.
- A successful `GET` is served by the leader after a same-term quorum
  confirmation, from state applied through its commit index.
- Followers do not serve data operations. Local `PING`, `ECHO`, `INFO`, and
  `GROUP.STATS` do not carry a quorum guarantee.
- Acknowledged entries survive the loss of any one member in a three-node
  cluster, assuming the remaining storage behaves as specified by the host OS
  and hardware.

These are linearizability and durability guarantees, not exactly-once request
processing. The protocol has no client operation ID or deduplication table.

## Failure matrix

| Event | Data-command availability | State and client-visible outcome |
| --- | --- | --- |
| One follower stops | Available through the leader and remaining follower | A majority still exists; acknowledged writes remain durable on two members. |
| Leader stops | Temporarily unavailable | A follower normally wins an election after the randomized timeout. Clients must discover it and reconnect. |
| One node is isolated from the other two | Available only on the two-node side after/while it has a leader | The isolated node cannot confirm reads or commit writes. An isolated old leader steps down after it loses recent quorum contact, or immediately when it observes a higher term. |
| No two healthy members can communicate | Unavailable | No new quorum-confirmed read or committed mutation can complete. Local health/status commands may still answer. |
| Client connection drops during a mutation | Outcome is unknown | The entry may have committed even though the response was lost, or may remain uncommitted and later be replaced. |
| Process crashes after replying `OK` | Available after failover if two members remain | The acknowledged entry already existed durably on a majority. |
| Local disk append/force fails | That node becomes unhealthy and stops participating | Pending proposals fail; redundancy may permit the other two nodes to continue. Repair the storage before restarting it. |
| Torn final WAL write | Node unavailable during restart only | Recovery truncates the incomplete/corrupt final frame and replays the valid prefix. Raft can refill a missing uncommitted suffix. |
| Corruption before the WAL tail | Node fails recovery | Earlier corruption is not skipped. Restore or rebuild the affected member; do not edit the log in place. |

Election timing is not a service-level guarantee. The implementation uses a
450–900 ms randomized election timer and 120 ms heartbeats by default, but OS
scheduling, storage, TCP timeouts, and repeated split votes can extend a
failover.

## Retry guidance

The server returns a RESP error when a data command reaches a follower. If the
leader is known, the error includes its node ID; it does not include a routable
client address and the server does not proxy the request.

| Operation | Automatic retry guidance |
| --- | --- |
| `PING`, `ECHO`, `INFO`, `RAFT.INFO`, `GROUP.STATS` | Safe, but these are local observations. |
| `GET` | Safe to retry; the later successful call is a new linearizable read. |
| `SET key same-value` | State is idempotent in isolation, but retries can interact with concurrent writers. |
| `DEL` | Final absent state is idempotent, but the returned removal count is not. |

For business operations such as counters, transfers, or job claims, add a
stable request ID and a replicated deduplication record at the application
layer before retrying an unknown outcome.

## Committed versus uncommitted data

A leader can have a durable local suffix that is not committed. That is normal:
durability on one node is not consensus. Following a leadership change, Raft
may preserve that suffix if it becomes part of the new leader's log, or replace
it with the elected leader's history. Clients are never told an entry succeeded
until the majority commit and local apply steps finish.

The same uncertainty explains why a timed-out client cannot infer failure. The
server might have committed just before the timeout but lost the response.

## Recovery and operations

- Preserve a node's ID together with its data directory.
- Never run two processes against one data directory.
- Never copy a live data directory into another member and change its node ID.
- Back up stopped, mutually consistent nodes; Raft replication is not a backup
  against operator error or application-level deletion.
- `scripts/cluster.sh down` retains Compose volumes. `docker compose down -v`
  removes them and is intentionally not wrapped by the helper.
- Membership changes are offline and unsupported. Editing `--peers` on part of
  a live cluster can create incompatible configurations.

## Out of scope

The current system does not claim Byzantine fault tolerance, cross-region
latency bounds, protection from faulty storage that lies about `fsync`, online
backup consistency, rolling membership changes, or exactly-once client
semantics. Authentication and transport encryption are also absent, so deploy
only on trusted networks.
