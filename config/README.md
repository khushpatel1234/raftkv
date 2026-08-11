# Configuration

RaftKV is configured through command-line flags. Every node in a cluster must
receive the same complete static membership list; the list includes the local
node.

| Flag | Meaning | Compose value |
| --- | --- | --- |
| `--node-id` | Unique positive integer node identity | `1`, `2`, or `3` |
| `--client-host` | RESP bind address | `0.0.0.0` inside the container |
| `--client-port` | RESP client listener | `6379` |
| `--raft-host` | Internal Raft bind address | `0.0.0.0` inside the container |
| `--raft-port` | Internal Raft RPC listener | `7000` |
| `--data-dir` | Node-local durable state | `/data` |
| `--peers` | Comma-separated `id=host:port` membership | `1=raftkv-1:7000,2=raftkv-2:7000,3=raftkv-3:7000` |

The Docker Compose host ports can be overridden with the variables in
[`cluster.env.example`](cluster.env.example). These variables affect only the
host-to-container client-port mappings; Raft members still communicate over
the private Compose network on port `7000`.

For a direct host launch, explicitly bind both listeners to `127.0.0.1` unless
you have a trusted private network. RaftKV does not provide authentication or
transport encryption.

`scripts/cluster.sh up` prints the actual published endpoints. If they differ
from the defaults, pass them to the standalone smoke test explicitly:

```bash
RAFTKV_PORTS=7379,7380,7381 ./scripts/smoke.sh
```

Node IDs and data directories are identities, not disposable settings. Never
start a different node ID against an existing node's data directory. Changing
the membership string is not an online reconfiguration mechanism; stop the
cluster and preserve backups before changing a deployment.
