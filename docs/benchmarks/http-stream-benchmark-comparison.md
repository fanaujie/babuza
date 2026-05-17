# HTTP Stream Benchmark Comparison

This report compares the HTTP transport's default short-request mode with the HTTP stream mode enabled by `protocol.SetHttpOptsWithMessageStreamEnabled(true)`.

## Test Environment

| Component | Specification |
|-----------|---------------|
| OS | darwin |
| Architecture | arm64 |
| CPU | Apple M4 |
| Date | 2026-05-17 |
| Package | `github.com/fanaujie/babuza/pkg/transport/protocol/http` |

## Benchmark Command

```bash
env GOCACHE=/private/tmp/babuza-gocache go test ./pkg/transport/protocol/http \
  -run '^$' \
  -bench 'BenchmarkHTTP(BatchMessage|Snapshot)' \
  -benchmem \
  -count=5
```

The numbers below use the median value from 5 runs.

## Summary

HTTP stream mode reduces per-message HTTP request overhead by reusing a long-lived request body for framed messages. The effect is largest for small, frequent Raft messages and snapshots split into many small chunks.

| Workload | Short Request | HTTP Stream | Improvement |
|----------|---------------|-------------|-------------|
| Batch message | 26.806 us/op | 2.349 us/op | 11.4x faster |
| Snapshot, 32 x 256 B chunks | 990.327 us/op | 148.066 us/op | 6.7x faster |
| Snapshot, 4 x 8 KiB chunks | 175.458 us/op | 90.616 us/op | 1.9x faster |

## Detailed Results

### Batch Messages

| Mode | ns/op | B/op | allocs/op |
|------|------:|-----:|----------:|
| Short request | 26,806 | 5,827 | 69 |
| HTTP stream | 2,349 | 320 | 2 |

HTTP stream mode improves batch-message latency by 11.4x, reduces allocated bytes by 94.5%, and reduces allocation count by 97.1%.

### Snapshot Transfer: Small Chunks

This benchmark transfers one snapshot as metadata, 32 chunk messages of 256 B each, and a finish message.

| Mode | ns/op | B/op | allocs/op |
|------|------:|-----:|----------:|
| Short request | 990,327 | 218,790 | 2,486 |
| HTTP stream | 148,066 | 40,943 | 292 |

HTTP stream mode improves small-chunk snapshot latency by 6.7x, reduces allocated bytes by 81.3%, and reduces allocation count by 88.3%.

### Snapshot Transfer: Large Chunks

This benchmark transfers one snapshot as metadata, 4 chunk messages of 8 KiB each, and a finish message.

| Mode | ns/op | B/op | allocs/op |
|------|------:|-----:|----------:|
| Short request | 175,458 | 93,869 | 455 |
| HTTP stream | 90,616 | 48,426 | 124 |

HTTP stream mode improves large-chunk snapshot latency by 1.9x, reduces allocated bytes by 48.4%, and reduces allocation count by 72.7%.

## Interpretation

HTTP stream mode is most valuable when many small transport messages are sent to the same peer:

- Regular Raft batch messages avoid creating one HTTP request per batch.
- Snapshot transfer avoids one HTTP request per metadata, chunk, and finish message.
- Allocation pressure drops because frame writes reuse the active stream instead of rebuilding request state each send.

For larger snapshot chunks, the payload transfer cost becomes a bigger part of total time, so the relative latency gain is smaller, but HTTP stream still cuts allocations substantially.

## Related Benchmarks

The benchmark implementations live in [`pkg/transport/protocol/http/http_test.go`](../../pkg/transport/protocol/http/http_test.go):

- `BenchmarkHTTPBatchMessageShortRequest`
- `BenchmarkHTTPBatchMessageStream`
- `BenchmarkHTTPSnapshotShortRequestSmallChunks`
- `BenchmarkHTTPSnapshotStreamSmallChunks`
- `BenchmarkHTTPSnapshotShortRequestLargeChunks`
- `BenchmarkHTTPSnapshotStreamLargeChunks`
