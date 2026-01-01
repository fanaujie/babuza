# KvStore

A distributed key-value store built with the Babuza Raft framework, demonstrating how to build production-ready distributed systems.

## Overview

KvStore provides a simple REST API for storing and retrieving key-value pairs across a distributed cluster. It showcases all major Babuza components including configurable transport protocols, WAL implementations, session management, and snapshot storage.

## Features

- Distributed key-value storage with strong consistency
- REST API for CRUD operations
- Interactive CLI client
- Configurable transport (TCP, HTTP, gRPC)
- Multiple WAL backends (Babuza, etcd, Badger)
- Multiple snapshot backends (local disk, S3)
- TLS/mTLS support for both HTTP API and Raft communication
- Docker support for easy deployment

## Quick Start

### Build

```bash
cd examples/kvstore
go build -o kvstore .
```

### Start a 3-Node Cluster

```bash
# Terminal 1 - Node 1
./kvstore server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=1 \
  --raft-local-peer-address=localhost:14201 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --kv-http-address=localhost:24201 \
  --raft-storage-dir=./data/node1

# Terminal 2 - Node 2
./kvstore server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=2 \
  --raft-local-peer-address=localhost:14202 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --kv-http-address=localhost:24202 \
  --raft-storage-dir=./data/node2

# Terminal 3 - Node 3
./kvstore server \
  --raft-cluster-id=100 \
  --raft-local-peer-id=3 \
  --raft-local-peer-address=localhost:14203 \
  --raft-cluster-peers-address=1=localhost:14201,2=localhost:14202,3=localhost:14203 \
  --kv-http-address=localhost:24203 \
  --raft-storage-dir=./data/node3
```

### Use the CLI Client

```bash
./kvstore client --cluster-members=1=localhost:24201,2=localhost:24202,3=localhost:24203

# Interactive commands:
> set mykey myvalue
Successfully set key 'mykey' to value 'myvalue'
> get mykey
myvalue
> delete mykey
Successfully deleted key 'mykey'
> help
Available commands: exit, join, set, get, delete, append, remove, cluster, help
```

### Use the REST API

The KV Store uses JSON for request/response bodies. All write operations require session information for idempotency support.

```bash
# Set a key (POST)
curl -X POST http://localhost:24201/kv \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"myvalue","sessionID":1,"sequenceNumber":1}'

# Get a key (GET with query parameter)
curl "http://localhost:24201/kv?key=mykey"

# Get a key with linearizable read (strongly consistent)
curl "http://localhost:24201/kv?key=mykey" -H "X-Linearizable: true"

# Append to a key (PUT)
curl -X PUT http://localhost:24201/kv \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"_appended","sessionID":1,"sequenceNumber":2}'

# Delete a key (DELETE)
curl -X DELETE http://localhost:24201/kv \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","sessionID":1,"sequenceNumber":3}'

# Get cluster peers
curl http://localhost:24201/peers
```

## Server Configuration

### Basic Options

| Flag | Default | Description |
|------|---------|-------------|
| `--kv-http-address` | `localhost:24200` | HTTP API listen address |
| `--raft-cluster-id` | `100` | Cluster ID (same for all nodes) |
| `--raft-local-peer-id` | `1` | Unique node ID |
| `--raft-local-peer-address` | `localhost:14200` | Raft communication address |
| `--raft-cluster-peers-address` | `1=localhost:14200` | All cluster peers (ID=address format) |
| `--raft-storage-dir` | `./raft_storage` | Data storage directory |
| `--raft-join-cluster` | `false` | Join existing cluster |
| `--raft-voter` | `true` | Start as voter (false = learner) |

### Component Selection

| Flag | Options | Default |
|------|---------|---------|
| `--transport-protocol` | `tcp`, `http`, `grpc` | `tcp` |
| `--wal-type` | `babuza-wal`, `etcd-wal`, `badger-wal`, `badger-wal-memory` | `babuza-wal` |
| `--snapshot-type` | `durable`, `volatile`, `s3` | `durable` |
| `--session-type` | `noop`, `expire`, `lru` | `noop` |
| `--state-machine` | `memory`, `memory-concurrent`, `disk` | `memory` |

### TLS Configuration

```bash
# Enable Raft TLS
./kvstore server \
  --raft-encrypt \
  --raft-peer-cert=/path/to/cert.pem \
  --raft-peer-key=/path/to/key.pem \
  --raft-peer-root-ca=/path/to/ca.pem

# Enable HTTP TLS
./kvstore server \
  --http-cert=/path/to/cert.pem \
  --http-key=/path/to/key.pem
```

### S3 Snapshot Storage

```bash
./kvstore server \
  --snapshot-type=s3 \
  --s3-endpoint=http://s3.amazonaws.com \
  --s3-region=us-east-1 \
  --s3-access-key=YOUR_ACCESS_KEY \
  --s3-secret-key=YOUR_SECRET_KEY \
  --s3-bucket=raft-snapshots \
  --s3-path-style=false
```

For S3-compatible services (RustFS, MinIO, etc.), use path-style addressing:

```bash
./kvstore server \
  --snapshot-type=s3 \
  --s3-endpoint=http://localhost:9000 \
  --s3-region=us-east-1 \
  --s3-access-key=YOUR_ACCESS_KEY \
  --s3-secret-key=YOUR_SECRET_KEY \
  --s3-bucket=raft-snapshots \
  --s3-path-style=true
```

## Client Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-members` | `1=localhost:24201` | Cluster members (ID=address format) |
| `--enable-tls` | `false` | Enable TLS for client connections |
| `--auto-sync-interval` | `5s` | Cluster membership sync interval |

## REST API Reference

### KV Store Endpoints (`/kv`)

| Method | Description | Request Body |
|--------|-------------|--------------|
| `GET` | Get value by key | Query: `?key=<key>` |
| `POST` | Set key-value pair | `{"key":"...", "value":"...", "sessionID":..., "sequenceNumber":...}` |
| `PUT` | Append to existing key | `{"key":"...", "value":"...", "sessionID":..., "sequenceNumber":...}` |
| `DELETE` | Delete key | `{"key":"...", "sessionID":..., "sequenceNumber":...}` |

**Headers:**
- `X-Linearizable: true` - Enable linearizable read (GET only, must be sent to leader)

### Cluster Management Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/peers` | `GET` | Get all cluster peers |
| `/peers` | `POST` | Add a new peer (voter or learner) |
| `/peers` | `PUT` | Update peer configuration |
| `/peers` | `DELETE` | Remove a peer from cluster |
| `/promote-learner` | `POST` | Promote learner to voter |
| `/transfer-leader` | `POST` | Transfer leadership to another node |
| `/sessions` | `GET/POST/DELETE` | Session management |
| `/metrics` | `GET` | Prometheus metrics |

## Advanced Server Options

| Flag | Default | Description |
|------|---------|-------------|
| `--disable-forwarding` | `false` | Disable automatic proposal forwarding from followers to leader |
| `--wal-no-sync` | `false` | Disable fsync on WAL writes (faster but less durable) |
| `--s3-prefix` | `""` | Object prefix (folder path) for S3 snapshots |

## Docker Deployment

The Docker setup includes a 3-node KVStore cluster with Prometheus and Grafana for monitoring.

### Build the Docker Image

```bash
cd examples/kvstore/docker
./build-docker.sh
```

This script builds the `babuza-kvstore:latest` image with all required dependencies.

### Start the Cluster

```bash
docker-compose up -d
```

### Services

| Service | URL | Description |
|---------|-----|-------------|
| KVStore Node 1 | http://localhost:24201 | HTTP API |
| KVStore Node 2 | http://localhost:24202 | HTTP API |
| KVStore Node 3 | http://localhost:24203 | HTTP API |
| Prometheus | http://localhost:9090 | Metrics collection |
| Grafana | http://localhost:3000 | Metrics dashboard (admin/admin) |

Grafana comes pre-configured with a Babuza Raft dashboard for monitoring cluster health and performance.
