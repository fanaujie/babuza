# pkg/builder

Component builder pattern for assembling Babuza infrastructure components.

## Overview

The `builder` package provides a fluent API for constructing all the components needed to run a Babuza Raft cluster. Instead of manually wiring together transport, WAL, snapshot, and session managers, use the builder to configure and instantiate them with sensible defaults.

## Key Types

| Type | Description |
|------|-------------|
| `BabuzaComponentBuilder` | Fluent builder for creating components |
| `BabuzaComponentConfig` | Configuration struct for all component types |
| `BabuzaComponent` | Output struct containing all instantiated components |
| `TransportAssets` | Optional transport-related resources (limiters, breakers) |

## Important Notes

- **Single-use builder**: Each `BabuzaComponentBuilder` can only call `Build()` once. Attempting to modify or build again will panic.
- **S3 requirement**: When using `S3Snapshot`, you must provide `S3Config` via `SetS3Config()`, otherwise `Build()` will panic.

## Default Behaviors

When component types are not specified, the builder uses these defaults:

| Component | Default |
|-----------|---------|
| SessionType | `NoOpSession` (no idempotency tracking) |
| WalType | `BabuzaWal` (native WAL) |
| SnapshotType | `DurableSnapshot` (filesystem-based) |
| TransportType | `TcpTransport` (physical TCP) |
| MetricType | Mock metrics collector |

## Usage

### Basic Setup

```go
import "github.com/fanaujie/babuza/pkg/builder"

component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    ClusterId:      1,
    StorageRootDir: "/var/lib/babuza",
    SessionType:    builder.LRUSession,
    SnapshotType:   builder.DurableSnapshot,
    TransportType:  builder.TcpTransport,
    WalType:        builder.BabuzaWal,
    MetricType:     builder.MetricsPrometheus,
}).Build()

// Use the components
fmt.Printf("Cluster: %v\n", component.Cluster)
fmt.Printf("Transport: %v\n", component.Transport)
fmt.Printf("WAL: %v\n", component.WalManager)
fmt.Printf("Snapshot: %v\n", component.SnapshotManager)
fmt.Printf("Session: %v\n", component.SessionManager)
```

### Fluent Configuration with S3 Cloud Storage

```go
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
}).
    SetClusterId(1).
    SetSnapshotType(builder.S3Snapshot).
    SetS3Config(&cloudstorage.S3Config{
        Endpoint:        "http://localhost:9000",  // S3-compatible endpoint
        Region:          "us-east-1",              // Required for signature calculation
        AccessKeyID:     "access",
        SecretAccessKey: "secret",
        UsePathStyle:    true,                     // Required for S3-compatible services
        Bucket:          "snapshots",
        Prefix:          "babuza/",
    }).
    Build()
```

### Custom Logger and Metrics

```go
zapLogger := zap.NewProduction()
customLogger := logger.NewRaftLogger(zapLogger.Sugar())

component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
}).
    SetCustomLogger(customLogger).
    SetCustomEtcdZapLogger(zapLogger).  // For etcd WAL
    SetMetricsCollector(myMetricsCollector).
    Build()
```

### Transport Options

```go
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    TransportType:  builder.TcpTransport,
}).
    SetTransportAssets(builder.TransportAssets{
        TransportMemoryLimiter:   myLimiter,
        SnapshotChuckRateLimiter: myRateLimiter,
        PeerCircuitBreaker:       myBreaker,
    }).
    AddTcpOptions(protocol.WithTcpDialTimeout(time.Second * 5)).
    Build()
```

### HTTP Transport Options

```go
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    TransportType:  builder.HttpTransport,
}).
    AddHttpOptions(protocol.WithHttpDialTimeout(time.Second * 5)).
    Build()
```

### gRPC Transport Options

```go
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    TransportType:  builder.GRPCTransport,
}).
    AddGrpcOptions(protocol.WithGrpcDialTimeout(time.Second * 5)).
    Build()
```

### Custom TCP Network

```go
// Use custom TCP network implementation
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    TransportType:  builder.TcpTransport,
}).
    SetTransportTcpNetwork(myCustomNetwork).
    AddTransportOptions(transport.WithSomeOption()).
    Build()
```

### Session Options

```go
// LRU session with custom max size
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    SessionType:    builder.LRUSession,
}).
    AddLruSessionOptions(session.WithMaxSessions(10000)).
    Build()

// Expire session with custom TTL
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    SessionType:    builder.ExpireSession,
}).
    AddExpireSessionOptions(session.WithSessionTTL(time.Hour)).
    Build()
```

## Builder Methods

| Method | Description |
|--------|-------------|
| `SetClusterId(uint64)` | Set the cluster ID |
| `SetStorageRootDir(string)` | Set the root directory for storage |
| `SetSessionType(string)` | Set session manager type |
| `SetSnapshotType(string)` | Set snapshot manager type |
| `SetTransportType(string)` | Set transport protocol type |
| `SetWalType(string)` | Set WAL implementation type |
| `SetCustomLogger(ibabuza.Logger)` | Use a custom logger |
| `SetCustomEtcdZapLogger(*zap.Logger)` | Set zap logger for etcd WAL |
| `SetMetricsCollector(ibabuza.MetricsCollector)` | Use a custom metrics collector |
| `SetS3Config(*cloudstorage.S3Config)` | Configure S3/S3-compatible storage for snapshots |
| `SetTransportAssets(TransportAssets)` | Set transport limiters and breakers |
| `SetTransportTcpNetwork(tcp.NetworkIO)` | Set custom TCP network implementation |
| `AddTransportOptions(...transport.SetTransportOptions)` | Add transport configuration options |
| `AddTcpOptions(...protocol.SetTcpOptions)` | Add TCP protocol options |
| `AddHttpOptions(...protocol.SetHttpOptions)` | Add HTTP protocol options |
| `AddGrpcOptions(...protocol.SetGrpcOptions)` | Add gRPC protocol options |
| `AddLruSessionOptions(...session.SetLruMgrOptions)` | Add LRU session options |
| `AddExpireSessionOptions(...session.SetExpiredMgrOptions)` | Add expire session options |
| `Build()` | Build and return the component (single-use) |

## Available Constants

### Session Types

| Constant | Value | Description |
|----------|-------|-------------|
| `NoOpSession` | `noop` | No session tracking (no idempotency) |
| `ExpireSession` | `expire` | Time-based session expiration |
| `LRUSession` | `lru` | LRU-based session eviction |

### Transport Types

| Constant | Value | Description |
|----------|-------|-------------|
| `TcpTransport` | `tcp` | TCP with physical network |
| `TcpMemoryTransport` | `tcp-memory` | TCP with in-memory network (testing) |
| `HttpTransport` | `http` | HTTP/1.1 transport |
| `GRPCTransport` | `grpc` | gRPC transport |

### WAL Types

| Constant | Value | Description |
|----------|-------|-------------|
| `BabuzaWal` | `babuza-wal` | Babuza native WAL |
| `ETCDWal` | `etcd-wal` | etcd-compatible WAL |
| `BadgerWalDisk` | `badger-wal` | Badger LSM-based WAL (disk) |
| `BadgerWalMemory` | `badger-wal-memory` | Badger LSM-based WAL (memory) |
| `PebbleWalDisk` | `pebble-wal` | Pebble LSM-based WAL (disk) |
| `PebbleWalMemory` | `pebble-wal-memory` | Pebble LSM-based WAL (memory) |

### Snapshot Types

| Constant | Value | Description |
|----------|-------|-------------|
| `DurableSnapshot` | `durable` | Filesystem-based snapshots |
| `VolatileSnapshot` | `volatile` | In-memory snapshots (testing) |
| `S3Snapshot` | `s3` | AWS S3/S3-compatible storage (uses aws-sdk-go-v2) |

### Metrics Types

| Constant | Value | Description |
|----------|-------|-------------|
| `MetricsOtel` | `otel` | OpenTelemetry metrics |
| `MetricsPrometheus` | `prometheus` | Prometheus metrics |

## BabuzaComponent Output

```go
type BabuzaComponent struct {
    Cluster           ibabuza.Cluster
    RaftNode          ibabuza.RaftNode
    SessionManager    ibabuza.SessionManager
    SnapshotManager   ibabuza.SnapshotManager
    WalManager        ibabuza.WalManager
    Transport         ibabuza.Transport
    Logger            ibabuza.Logger
    MetricsController ibabuza.MetricsCollector
}
```

