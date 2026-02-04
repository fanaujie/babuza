# ibabuza

Core interface definitions for all Babuza pluggable components.

## Overview

The `ibabuza` package defines the contracts that all Babuza components must implement. By programming to interfaces, you can swap implementations (e.g., different WAL backends) without changing application code.

## Key Interfaces

### State Machine

| Interface | Description |
|-----------|-------------|
| `BaseStateMachine` | Core state machine interface with Apply, SaveSnapshot, RestoreFromSnapshot, Query, Close |
| `MemoryStateMachine` | Alias for BaseStateMachine (in-memory state) |
| `DiskStateMachine` | Extends BaseStateMachine with Open() and concurrent snapshot support |
| `ConcurrentSnapshotStateMachine` | Prepare/release snapshot context for concurrent snapshots |
| `SessionEnabledStateMachine` | State machines that support client sessions |
| `StateMachineSnapshotWriter` | Write state machine files and external file references during snapshot |
| `StateMachineSnapshotReader` | Read state machine files during restore |
| `ResponseSerializer` | Serialize/deserialize responses for session replay |

### Cluster Management

| Interface | Description |
|-----------|-------------|
| `Cluster` | Manage cluster membership: Add, Remove, Update, Promote peers |

### Transport

| Interface | Description |
|-----------|-------------|
| `Transport` | Send Raft messages and snapshots between peers |
| `TransportProtocol` | Protocol implementation (TCP, HTTP, gRPC) |
| `TransportClient` | Client-side message sending |
| `TransportServer` | Server-side message receiving |
| `RaftMessageHandler` | Process incoming Raft messages |
| `RaftStatusReporter` | Report unreachable peers and snapshot status |
| `RaftNodeHandler` | Combined handler for Raft node operations |
| `TransportResolver` | Resolve peer ID to network address |

### Session Management

| Interface | Description |
|-----------|-------------|
| `Session` | Individual client session tracking |
| `SessionManager` | Manage all sessions: Register, Unregister, Expire |
| `ApplyResultSerializer` | Serialize/deserialize ApplyResult for session persistence |

### Snapshot

| Interface | Description |
|-----------|-------------|
| `SnapshotManager` | Create, load, and purge snapshots; manage external file handlers |
| `SnapshotReader` | Read snapshot data and metadata |
| `AtomicSnapshotWriter` | Write snapshot atomically, including external file references |
| `AtomicSnapshotReceiver` | Receive snapshot chunks from leader |
| `SnapshotPurger` | Automatic snapshot purging |
| `ExternalFileHandler` | Callback for external file notification on snapshot install |

### Write-Ahead Log

| Interface | Description |
|-----------|-------------|
| `Wal` | Write and sync log entries |
| `WalManager` | Create, replay, and manage WAL lifecycle |
| `EntryStorage` | In-memory storage implementing raft.Storage |
| `ReplayWalResult` | WAL replay result with metadata and entries |
| `WalPurger` | Automatic WAL file purging |

### Raft Node

| Interface | Description |
|-----------|-------------|
| `RaftNode` | Start or restart a Raft node |
| `RaftListener` | Listen for Raft events (leader changes, membership changes) |

### Event Constants

| Constant | Description |
|----------|-------------|
| `MemberJoined` | A new member joined the cluster |
| `MemberUpdated` | A member was updated |
| `MemberRemoved` | A member was removed |
| `LeanerAdded` | A learner was added |
| `LeanerPromoted` | A learner was promoted to voter |
| `AcquiredLeader` | This node became the leader |
| `LostLeader` | This node lost leadership |

### Multi-Raft

| Interface | Description |
|-----------|-------------|
| `MultiRaftListener` | Listen for events across multiple Raft groups |
| `MultiRaftTransport` | Transport layer for multi-raft messages |
| `MultiRaftWalManager` | Manage WAL for multiple Raft groups |
| `MultiRaftSnapshotManager` | Manage snapshots for multiple Raft groups |
| `MultiRaftStoreHandler` | Handle multi-raft store operations |

## Usage

### Implementing a State Machine

```go
type MyStateMachine struct {
    data map[string]string
}

func (m *MyStateMachine) Apply(entry ibabuza.Entry) ibabuza.ApplyResult {
    // Decode and apply the command
    cmd := decodeCommand(entry.Command)
    m.data[cmd.Key] = cmd.Value
    return ibabuza.ApplyResult{
        LogIndex: entry.Index,
        Response: "OK",
    }
}

func (m *MyStateMachine) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, w ibabuza.StateMachineSnapshotWriter) error {
    writer, _ := w.CreateStateMachineFile("data", babuzapb.SnapshotFileCompression_Snappy)
    defer writer.Close()
    // Serialize m.data to writer

    // Optionally register external file references (e.g., large files on S3)
    // w.AddExternalFile(ibabuza.ExternalFileDescriptor{
    //     FileTag: "large_blob", LocationUri: "s3://bucket/blob",
    // })
    return nil
}

func (m *MyStateMachine) RestoreFromSnapshot(r ibabuza.StateMachineSnapshotReader) error {
    reader, _, _ := r.Open("data")
    // Deserialize m.data from reader
    return nil
}

func (m *MyStateMachine) Query(key any) (any, error) {
    return m.data[key.(string)], nil
}

func (m *MyStateMachine) Close() error {
    return nil
}
```

### ResponseSerializer and Session Integration

When using client sessions for exactly-once semantics, responses must be cached to handle duplicate requests. Since `ApplyResult.Response` is `any` type, the system needs a way to serialize/deserialize responses for persistence.

```go
// 1. Implement ResponseSerializer for your response types
type MyResponseSerializer struct{}

func (s *MyResponseSerializer) Serialize(w io.Writer, response any) error {
    // Serialize your response type (e.g., using gob, json, protobuf)
    return gob.NewEncoder(w).Encode(response)
}

func (s *MyResponseSerializer) Deserialize(r io.Reader) (any, error) {
    var response MyResponse
    err := gob.NewDecoder(r).Decode(&response)
    return response, err
}

// 2. Implement SessionEnabledStateMachine on your state machine
type MyStateMachine struct {
    data       map[string]string
    serializer *MyResponseSerializer
}

func (m *MyStateMachine) GetResponseSerializer() ibabuza.ResponseSerializer {
    return m.serializer
}
```

**How it works:**

1. Client sends a request with `(ClientID, SequenceNum)`
2. State machine applies the command and returns `ApplyResult`
3. SessionManager caches the result using `ResponseSerializer.Serialize()`
4. If the same `(ClientID, SequenceNum)` arrives again (duplicate):
   - SessionManager finds the cached result
   - Uses `ResponseSerializer.Deserialize()` to reconstruct the response
   - Returns the cached response without re-applying

This ensures exactly-once semantics even with network retries or leader changes.

### Using TLS Configuration

```go
tlsConfig := ibabuza.TLSConfig{
    EnableTLS: true,
    MutualTLS: true,
    TLSCert:   "/path/to/cert.pem",
    TLSKey:    "/path/to/key.pem",
    TLSRootCA: "/path/to/ca.pem",
}

transportConfig := ibabuza.TransportConfig{
    LocalNodeID: 1,
    PeerAddress: "127.0.0.1:7001",
    TLSConfig:   tlsConfig,
}
```

## Core Types

### Entry

```go
type Entry struct {
    Term    uint64  // Raft term when entry was created
    Index   uint64  // Log index
    Command []byte  // Serialized command
}
```

### ApplyResult

```go
type ApplyResult struct {
    LogIndex uint64  // Index of applied entry
    Response any     // Result to return to client
    Error    error   // Error if apply failed
}
```
