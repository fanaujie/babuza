# Babuza

A Go framework built on [etcd Raft](https://github.com/etcd-io/raft) for building distributed consensus-based systems with simplified APIs and some features.

## Features

- **Simplified Raft Integration** - Abstracts etcd Raft complexity with clean Go interfaces
- **Pluggable Components** - Interchangeable WAL, snapshot, transport, and session implementations
- **Multiple State Machine Types** - Support for memory, disk, and concurrent snapshot state machines
- **Dynamic Cluster Management** - Add/remove peers, learner nodes, and leader transfer
- **Production Ready** - TLS/mTLS support, linearizable reads, configurable snapshots
- **Disaster Recovery** - Standalone node restoration from existing WAL/snapshots
- **Cloud-Native Snapshots** - AWS S3 and S3-compatible storage for snapshots
- **Observable** - Built-in Prometheus and OpenTelemetry metrics support

## Key Innovations vs etcd

Babuza introduces several architectural improvements over the standard etcd Raft usage:

### Memory-Efficient WAL with Index-Based Caching

Unlike etcd which caches **all log entries in memory**, Babuza's WAL uses an **index + cache** architecture:

- Only stores entry metadata (term, index, type) in memory
- Entry data is cached in a ring buffer with configurable size
- On cache miss, reads entry data from WAL storage
- Significantly reduces memory footprint for large log histories

### Chunked Snapshot Transfer

Transport layer supports **chunked snapshot transfer** with configurable chunk sizes:

- Large snapshots are split into manageable chunks
- Rate limiting prevents network saturation
- Resumable transfers on network interruption
- Reduces memory pressure during snapshot installation

### Proposal Idempotency via Sessions

Built-in **session management** ensures exactly-once semantics:

- Client registers a session before proposing
- Each proposal includes session ID and sequence number
- Duplicate proposals return cached results
- Multiple eviction strategies: time-based expiration or LRU

### Experimental Multi-Raft (without modifying etcd Raft)

The [experimental](./raft/experimental/README.md) package implements multi-Raft group support:

- **Coalesced Heartbeats** - Merge heartbeats from multiple Raft groups to reduce network overhead
- **Shared WAL** - Multiple Raft groups share a single WAL instance
- **Sharded Scheduling** - Efficient processing across many Raft groups
- All implemented without any modifications to the etcd Raft library

## Architecture

![architecture](images/babuza_architecture.svg)


## Quick Start

See [examples/simple](./examples/simple/README.md) for simple example code.

## Examples

| Example | Description |
|---------|-------------|
| [KV Store](./examples/kvstore/README.md) | Single-raft distributed key-value store with REST API |
| [Redis Cluster](./examples/redis-cluster/README.md) | Multi-raft Redis-compatible distributed cache |

## Component Documentation

### Core Packages

| Package | Description |
|---------|-------------|
| [ibabuza](./ibabuza/README.md) | Core interfaces for all pluggable components |
| [raft](./raft/README.md) | Consensus layer, cluster bootstrap, and Raft API |
| [raft/experimental](./raft/experimental/README.md) | Multi-Raft with coalesced heartbeats and shared WAL (experimental) |
| [pkg/builder](./pkg/builder/README.md) | Component builder pattern for easy assembly |

### Infrastructure Packages

| Package | Description |
|---------|-------------|
| [pkg/cluster](./pkg/cluster/README.md) | Cluster membership and peer management |
| [pkg/transport](./pkg/transport/README.md) | Network transport layer (TCP, HTTP, gRPC) |
| [pkg/session](./pkg/session/README.md) | Client session management for idempotency |
| [pkg/snapshot](./pkg/snapshot/README.md) | Snapshot creation, storage, and restoration |
| [pkg/wal](./pkg/wal/README.md) | Write-ahead log implementations |

## Configuration

### Component Types

| Component | Available Types |
|-----------|-----------------|
| **Session** | `noop`, `expire`, `lru` |
| **Transport** | `tcp`, `tcp-memory`, `http`, `grpc` |
| **WAL** | `babuza-wal`, `etcd-wal`, `badger-wal`, `badger-wal-memory`, `pebble-wal`, `pebble-wal-memory` |
| **Snapshot** | `durable`, `volatile`, `s3` |
| **Metrics** | `otel`, `prometheus` |


## Test Cluster Framework

Babuza provides a [testcluster](./test/testcluster/README.md) framework for testing distributed system failure scenarios:


**Supported Failure Scenarios:**

| Scenario | Description |
|----------|-------------|
| **Node Disconnect** | Simulate single node network failure |
| **Network Partition** | Split cluster into isolated groups |
| **Leader Failure** | Stop/restart leader node |
| **Quorum Loss** | Disconnect majority of nodes |
| **Node Restart** | Stop and restart with WAL/snapshot recovery |
| **Disaster Recovery** | Recover standalone from lost cluster |



## Contributing

Contributions are welcome! Please ensure:

1. Tests are included for new functionality
2. Documentation is updated as needed

## License

Apache License 2.0. See [LICENSE](./LICENSE) for details.

Copyright 2025 Chen Chunchieh
