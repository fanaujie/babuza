# Distributed Lock

A distributed lock service built with the Babuza Raft framework.

## Overview

This example demonstrates how to build a distributed lock service using Babuza. It provides:

- Distributed lock acquisition with lease-based TTL
- Fencing tokens for safe lock ownership
- Reentrant locks (same owner can re-acquire)
- Automatic lock expiration via lease mechanism
- Wait queue for fair lock acquisition

## Features

| Feature | Description |
|---------|-------------|
| **Lease** | Time-bounded session that can hold multiple locks |
| **Acquire** | Acquire a lock bound to a lease |
| **Release** | Release a lock using fencing token |
| **Wait** | Wait in queue until lock becomes available |
| **Status** | Query lock or lease status |
| **Fencing Token** | Monotonic token for safe distributed operations |
| **Reentrant** | Same owner can re-acquire to refresh binding |
| **FIFO Queue** | Fair ordering for waiting clients |

## Lease-based Architecture

Unlike TTL-based lock systems, this implementation uses a **lease abstraction** (similar to etcd):

1. **Grant a Lease**: Client creates a lease with a TTL
2. **Bind Locks to Lease**: Locks are acquired with a lease ID, not a TTL
3. **Keep Lease Alive**: Client periodically extends the lease TTL
4. **Automatic Cleanup**: When lease expires, all bound locks are released

Benefits:
- Single TTL management point for multiple locks
- Consistent expiration across related locks
- Reduced network traffic (one keepalive for many locks)
- Deterministic cleanup through Raft consensus

## Quick Start

### Build

```bash
cd examples/distlock
go build -o distlock .
```

### Start a 3-Node Cluster

```bash
# Terminal 1 - Node 1
./distlock server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=1 \
  --raft-local-peer-address=localhost:14201 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --http-address=localhost:24201 \
  --raft-storage-dir=./data/node1

# Terminal 2 - Node 2
./distlock server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=2 \
  --raft-local-peer-address=localhost:14202 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --http-address=localhost:24202 \
  --raft-storage-dir=./data/node2

# Terminal 3 - Node 3
./distlock server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=3 \
  --raft-local-peer-address=localhost:14203 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --http-address=localhost:24203 \
  --raft-storage-dir=./data/node3
```

### Use the CLI Client

```bash
./distlock client --server=localhost:24201

# First, create a lease
> lease-grant 30
Lease granted: lease_id=1, ttl=30s, expires_in=30s

# Acquire a lock using the lease
> acquire my-lock client-1 1
Lock acquired: fencing_token=1, lease_id=1

# Check lock status
> status my-lock
Lock: my-lock, Owner: client-1, Token: 1, Lease: 1, Waiters: 0

# Keep the lease alive (extends TTL)
> lease-keepalive 1
Lease renewed, expires_in=30s

# Release the lock
> release my-lock client-1 1
Lock released

# Or revoke the lease to release all locks
> lease-revoke 1
Lease revoked, released locks: [my-lock]

> help
Available commands:

Lease operations:
  lease-grant <ttl_seconds>           - Create a new lease
  lease-revoke <lease_id>             - Revoke a lease (releases all locks)
  lease-keepalive <lease_id>          - Extend lease TTL
  lease-status <lease_id>             - Check lease status

Lock operations:
  acquire <lock_name> <owner_id> <lease_id>  - Acquire a lock
  release <lock_name> <owner_id> <fencing_token> - Release a lock
  wait <lock_name> <owner_id> <lease_id> [timeout] - Wait to acquire lock
  status <lock_name>                  - Check lock status

Other:
  help                                - Show this help
  exit                                - Exit the client
```

### Wait for Lock Example

```bash
# Terminal 1 - Client 1 holds the lock
> lease-grant 30
Lease granted: lease_id=1, ttl=30s, expires_in=30s
> acquire my-lock client-1 1
Lock acquired: fencing_token=1, lease_id=1

# Terminal 2 - Client 2 waits for the lock
# NOTE: lease TTL must be >= wait timeout, otherwise lease expires while waiting
> lease-grant 60
Lease granted: lease_id=2, ttl=60s, expires_in=60s
> wait my-lock client-2 2 30
Waiting for lock my-lock (request_id: abc12345, timeout: 30s)...
# (blocks until client-1 releases or timeout)

# Terminal 1 - Client 1 releases
> release my-lock client-1 1
Lock released

# Terminal 2 - Client 2 automatically acquires
Lock acquired: fencing_token=2, lease_id=2
```

**Important**: When using wait, ensure your lease TTL is longer than the wait timeout. If the lease expires while waiting, the waiter loses their queue position.

### Use the REST API

```bash
# Grant a lease (30 seconds TTL)
curl -X POST http://localhost:24201/leases \
  -H "Content-Type: application/json" \
  -d '{"ttl_seconds":30}'

# Response: {"command":10,"lease_id":1,"ttl":30,"expires_at":...}

# Acquire a lock with the lease
curl -X POST http://localhost:24201/locks \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-lock","owner_id":"client-1","lease_id":1}'

# Query lock status (local read, may be stale on followers)
curl "http://localhost:24201/locks?name=my-lock"

# Query lock status with linearizable read (consistent on any node)
curl -H "X-Linearizable: true" "http://localhost:24201/locks?name=my-lock"

# Keep lease alive
curl -X PUT http://localhost:24201/leases \
  -H "Content-Type: application/json" \
  -d '{"lease_id":1}'

# Release a lock
curl -X DELETE http://localhost:24201/locks \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-lock","owner_id":"client-1","fencing_token":1}'

# Revoke a lease (releases all associated locks)
curl -X DELETE http://localhost:24201/leases \
  -H "Content-Type: application/json" \
  -d '{"lease_id":1}'

# Query lease status
curl "http://localhost:24201/leases?id=1"

# Acquire with wait (long polling - waits if lock is held)
# NOTE: lease TTL must be >= wait timeout
curl -X POST http://localhost:24201/locks \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-lock","owner_id":"client-2","lease_id":2,"wait_timeout_seconds":30,"request_id":"unique-id"}'
```

## Server Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--http-address` | `localhost:24200` | HTTP API listen address |
| `--raft-cluster-id` | `100` | Cluster ID |
| `--raft-local-peer-id` | `1` | Unique node ID |
| `--raft-local-peer-address` | `localhost:14200` | Raft communication address |
| `--raft-cluster-peers-address` | `1=localhost:14200` | All cluster peers |
| `--raft-storage-dir` | `./raft_storage` | Data storage directory |
| `--raft-join-cluster` | `false` | Join existing cluster |

## REST API Reference

### Lease Operations

| Method | Path | Description | Request Body |
|--------|------|-------------|--------------
| `POST` | `/leases` | Grant new lease | `{"ttl_seconds":30}` |
| `DELETE` | `/leases` | Revoke lease | `{"lease_id":1}` |
| `PUT` | `/leases` | Keep lease alive | `{"lease_id":1}` |
| `GET` | `/leases?id=<id>` | Query lease status (add `X-Linearizable: true` header for consistent read) | - |

### Lock Operations

| Method | Path | Description | Request Body |
|--------|------|-------------|--------------
| `POST` | `/locks` | Acquire lock (with optional wait) | `{"lock_name":"...","owner_id":"...","lease_id":1,"wait_timeout_seconds":60}` |
| `DELETE` | `/locks` | Release lock | `{"lock_name":"...","owner_id":"...","fencing_token":1}` |
| `GET` | `/locks?name=<name>` | Query status (add `X-Linearizable: true` header for consistent read) | - |

## Fencing Tokens

Each successful lock acquisition returns a monotonically increasing fencing token. This token must be provided when releasing the lock, ensuring that stale lock holders cannot accidentally release locks they no longer own.

Example scenario:
1. Client A acquires lock, gets token=1
2. Client A's network partitions, lease expires
3. Client B acquires lock, gets token=2
4. Client A reconnects and tries to release with token=1
5. Release fails because token doesn't match (expected 2)

## Lease Expiration

Leases automatically expire after the specified TTL. When a lease expires:
1. All locks bound to the lease are released
2. Waiters in the queue are notified and may acquire the lock

Clients should:
1. Create a lease with reasonable TTL (e.g., 30 seconds)
2. Keep the lease alive periodically (e.g., every 10 seconds)
3. Handle lease loss gracefully

The leader node runs a ticker that checks for expired leases every second. When expired leases are detected, it proposes a `Tick` command through Raft consensus, ensuring deterministic and consistent lease expiration across all nodes. This optimization avoids unnecessary Raft proposals when no leases need cleanup.

## Wait Queue

The wait feature provides a fair, FIFO-ordered queue for clients waiting to acquire a lock. Add `wait_timeout_seconds` to the acquire request to enable waiting:

1. Client calls `POST /locks` with `wait_timeout_seconds` parameter
2. If lock is available, it's acquired immediately
3. If lock is held, client automatically joins the wait queue
4. When lock is released or lease expires, the first waiter in queue automatically acquires it
5. If timeout expires, client is removed from queue and receives 408 timeout

The wait queue is replicated through Raft consensus, ensuring:
- **Consistency**: All nodes agree on queue order
- **Fairness**: First-come, first-served ordering
- **Durability**: Queue survives leader changes

## Linearizable Reads

By default, GET requests (`/locks?name=...` and `/leases?id=...`) return local state which may be stale on follower nodes. For strongly consistent reads, add the `X-Linearizable: true` header:

```bash
# Linearizable read (consistent across all nodes)
curl -H "X-Linearizable: true" "http://localhost:24201/locks?name=my-lock"

# Works on both leader and follower nodes
curl -H "X-Linearizable: true" "http://localhost:24202/locks?name=my-lock"
```

When `X-Linearizable: true` is set:
1. The node contacts the leader to get the current commit index
2. Waits until local state machine has applied up to that index
3. Returns the result

This ensures read-after-write consistency even when requests go to different nodes.

## Leader Failover

When waiting for a lock, the client may experience connection loss due to leader changes. The `request_id` parameter enables safe reconnection:

```bash
# Acquire with wait and request_id for failover support
# NOTE: lease TTL must be >= wait timeout
curl -X POST http://localhost:24201/locks \
  -H "Content-Type: application/json" \
  -d '{"lock_name":"my-lock","owner_id":"client-2","lease_id":2,"wait_timeout_seconds":30,"request_id":"unique-request-id"}'
```

When client reconnects with the same `request_id`:
- **Already acquired**: Returns the lock result immediately
- **Still waiting**: Re-registers for notification and continues waiting
- **New request**: Joins the queue as a new waiter

The CLI client automatically handles reconnection:
```bash
# Assuming lease_id=2 was granted with TTL >= 30s
> wait my-lock client-2 2 30
Waiting for lock my-lock (request_id: abc12345, timeout: 30s)...
Leader changed, retrying...
In queue (position: 1), connection lost, retrying...
Lock acquired: fencing_token=2, lease_id=2
```
