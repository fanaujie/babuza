# Memory Usage Comparison: etcd vs Babuza

This report compares memory usage between **etcd MemoryStorage** and **Babuza EntryStorage** implementations.

## Test Environment

| Component | Specification |
|-----------|---------------|
| **OS** | Ubuntu 22.04.1 (Linux 6.8.0-90-generic) |
| **CPU** | Intel Core i5-8400 @ 2.80GHz (6 cores) |
| **Memory** | 32 GB DDR4 |
| **Go Version** | go1.24.11 linux/amd64 |
| **etcd Raft** | v3.5.12 |

## Background

### The Problem with etcd MemoryStorage

etcd's `MemoryStorage` keeps **all log entries in memory**, including the full entry data:

```go
// etcd raft/storage.go
type MemoryStorage struct {
    sync.Mutex
    hardState pb.HardState
    snapshot  pb.Snapshot
    ents      []pb.Entry  // ALL entries with data in RAM
}
```

This design causes memory usage to grow linearly with:
- Number of entries
- Size of entry data

For applications with large entry payloads or long log histories, this can lead to **excessive memory consumption** and even **OOM errors**.

### Babuza's Solution: Index + Ring Buffer

Babuza's `EntryStorage` uses a memory-efficient architecture:

```go
// Babuza walbase/storage.go
type EntryStorage[T any] struct {
    hardState raftpb.HardState
    snapshot  raftpb.Snapshot
    cache     *Cache           // Ring Buffer (fixed size, default 128)
    ents      []EntryIndex[T]  // Only metadata, NO data
    reader    EntryDataReader[T]
    mu        sync.Mutex
}
```

- **Entry Index**: Stores only metadata (term, index, type, file offset) - ~56 bytes per entry
- **Ring Buffer Cache**: Fixed-size cache for recent entries (default 128 entries)
- **On-demand Read**: Cache miss reads entry data from WAL on disk

## Benchmark Results

### Memory Usage Comparison

| Entries | Data Size | etcd Memory | Babuza Memory | Saved |
|---------|-----------|-------------|---------------|-------|
| 1,000   | 100 B     | 123.79 KB   | 61.05 KB      | 50.7% |
| 1,000   | 1 KB      | 1.02 MB     | 61.03 KB      | **94.2%** |
| 1,000   | 10 KB     | 9.81 MB     | 61.12 KB      | **99.4%** |
| 10,000  | 100 B     | 1.53 MB     | 557.03 KB     | 64.4% |
| 10,000  | 1 KB      | 10.23 MB    | 557.03 KB     | **94.7%** |
| 10,000  | 10 KB     | 98.12 MB    | 557.03 KB     | **99.4%** |
| 100,000 | 100 B     | 15.26 MB    | 5.35 MB       | 64.9% |
| 100,000 | 1 KB      | 102.23 MB   | 5.35 MB       | **94.8%** |
| 100,000 | 10 KB     | 981.14 MB   | 5.35 MB       | **99.5%** |

### Babuza Memory Independence from Data Size

With 10,000 entries, Babuza memory usage remains constant regardless of data size:

| Data Size | Babuza Memory |
|-----------|---------------|
| 100 B     | 523.34 KB     |
| 1 KB      | 555.87 KB     |
| 10 KB     | 555.96 KB     |
| 100 KB    | 555.88 KB     |

This proves that Babuza's index-based architecture is **independent of entry data size**.

### etcd Memory Linear Growth

With 10,000 entries, etcd memory usage grows linearly with data size:

| Data Size | etcd Memory | Growth Factor |
|-----------|-------------|---------------|
| 100 B     | 1.49 MB     | 1x            |
| 1 KB      | 10.23 MB    | 6.9x          |
| 10 KB     | 98.12 MB    | 65.9x         |

## Memory Layout Comparison

```
etcd MemoryStorage:
┌─────────────────────────────────────────────────────────┐
│ ents []pb.Entry                                         │
│ ┌──────────┬──────────┬──────────┬───────────────────┐  │
│ │ Entry[0] │ Entry[1] │ Entry[2] │       ...         │  │
│ │ Term     │ Term     │ Term     │                   │  │
│ │ Index    │ Index    │ Index    │                   │  │
│ │ Type     │ Type     │ Type     │                   │  │
│ │ Data[N]  │ Data[N]  │ Data[N]  │  ALL IN MEMORY    │  │
│ └──────────┴──────────┴──────────┴───────────────────┘  │
└─────────────────────────────────────────────────────────┘
Memory = N × (48 bytes + data_size)

Babuza EntryStorage:
┌─────────────────────────────────────────────────────────┐
│ ents []EntryIndex (metadata only)                       │
│ ┌──────────┬──────────┬──────────┬───────────────────┐  │
│ │ Index[0] │ Index[1] │ Index[2] │       ...         │  │
│ │ Term     │ Term     │ Term     │                   │  │
│ │ Index    │ Index    │ Index    │                   │  │
│ │ Type     │ Type     │ Type     │                   │  │
│ │ FileId   │ FileId   │ FileId   │  ONLY METADATA    │  │
│ │ Offset   │ Offset   │ Offset   │  (56 bytes each)  │  │
│ └──────────┴──────────┴──────────┴───────────────────┘  │
├─────────────────────────────────────────────────────────┤
│ cache *Cache (Ring Buffer, fixed 128 entries)           │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ [Recent 128 entries with actual Data]               │ │
│ └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
Memory = N × 56 bytes + Fixed Cache Size
```

## Trade-offs

| Aspect | etcd MemoryStorage | Babuza EntryStorage |
|--------|-------------------|---------------------|
| **Memory Usage** | High (all data in RAM) | Low (index + fixed cache) |
| **Read Latency (cache hit)** | O(1) memory access | O(1) memory access |
| **Read Latency (cache miss)** | N/A (always in memory) | Disk I/O required |
| **Scalability** | Limited by RAM | Scales with disk |
| **Best For** | Small clusters, low latency | Large clusters, big payloads |

## Running the Benchmark

```bash
# Run memory comparison test
go test -v -run TestMemoryUsageComparison ./pkg/wal/walbase/

# Run independence verification
go test -v -run TestBabuzaMemoryIndependentOfDataSize ./pkg/wal/walbase/

# Run linear growth verification
go test -v -run TestEtcdMemoryLinearWithDataSize ./pkg/wal/walbase/
```

## Conclusion

Babuza's index-based WAL architecture provides **94-99% memory savings** compared to etcd's MemoryStorage for typical workloads (1KB+ entry data). This makes Babuza particularly suitable for:

- Applications with large entry payloads
- Long-running clusters with extensive log histories
- Multi-Raft deployments where multiple groups share node resources
- Memory-constrained environments
