# HTTP Stream Benchmark Comparison

This report compares the HTTP transport's default short-request mode with the opt-in Raft message stream mode enabled by `protocol.SetHttpOptsWithMessageStreamEnabled(true)`.

## Current stream model

HTTP message streaming uses receiver-initiated response streams:

```text
receiver -- GET /raft/messages/stream?from=<receiverID> --> sender
receiver <-- framed BatchMessage response stream -------- sender
```

Regular Raft batch messages use the stream when the receiver has an active GET stream registered with the sender. If no stream is active, the sender falls back to `POST /raft/messages`.

Snapshot transfer is intentionally not part of `MessageStreamEnabled`; it remains on the regular `POST /raft/snapshot` request-response path so metadata, chunk, and finish messages keep synchronous `SnapshotMessageResponse` semantics.

## Latest results

Run on 2026-05-19:

```text
goos: darwin
goarch: arm64
cpu: Apple M4
```

Command:

```bash
go test ./pkg/transport/protocol/http \
  -run '^$' \
  -bench 'BenchmarkHTTP(BatchMessage|Snapshot)' \
  -benchmem \
  -count=1
```

| Workload | Short request | Message stream enabled | Result |
|----------|---------------|------------------------|--------|
| Batch message | 26.918 us/op, 5,828 B/op, 69 allocs/op | 1.625 us/op, 685 B/op, 7 allocs/op | 16.6x faster, 8.5x fewer bytes, 9.9x fewer allocs |
| Snapshot, 32 x 256 B chunks | 940.638 us/op, 219,549 B/op, 2,486 allocs/op | 938.864 us/op, 219,235 B/op, 2,486 allocs/op | effectively unchanged; snapshot stays short-request |
| Snapshot, 4 x 8 KiB chunks | 177.084 us/op, 94,057 B/op, 455 allocs/op | 176.838 us/op, 93,382 B/op, 454 allocs/op | effectively unchanged; snapshot stays short-request |

Raw output:

```text
BenchmarkHTTPBatchMessageShortRequest-10                                42934    26918 ns/op    5828 B/op      69 allocs/op
BenchmarkHTTPBatchMessageStream-10                                     737863     1625 ns/op     685 B/op       7 allocs/op
BenchmarkHTTPSnapshotShortRequestSmallChunks-10                          1270   940638 ns/op  219549 B/op    2486 allocs/op
BenchmarkHTTPSnapshotShortRequestSmallChunksMessageStreamEnabled-10       1275   938864 ns/op  219235 B/op    2486 allocs/op
BenchmarkHTTPSnapshotShortRequestLargeChunks-10                          6716   177084 ns/op   94057 B/op     455 allocs/op
BenchmarkHTTPSnapshotShortRequestLargeChunksMessageStreamEnabled-10       6699   176838 ns/op   93382 B/op     454 allocs/op
```

## Benchmark coverage

Benchmark implementations live in `pkg/transport/protocol/http/http_test.go`:

- `BenchmarkHTTPBatchMessageShortRequest`
- `BenchmarkHTTPBatchMessageStream`
- `BenchmarkHTTPSnapshotShortRequestSmallChunks`
- `BenchmarkHTTPSnapshotShortRequestSmallChunksMessageStreamEnabled`
- `BenchmarkHTTPSnapshotShortRequestLargeChunks`
- `BenchmarkHTTPSnapshotShortRequestLargeChunksMessageStreamEnabled`

The snapshot benchmarks with message streaming enabled are expected to remain short-request snapshot transfers; they guard that enabling Raft message streaming does not silently switch snapshot transport semantics.
