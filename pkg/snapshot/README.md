# pkg/snapshot

Snapshot management for state machine persistence and recovery in Babuza clusters.

## Overview

The `snapshot` package handles creating, storing, loading, and purging snapshots. Snapshots capture the state machine state at a point in time, enabling faster recovery and log compaction.

## Key Types

| Type | Description |
|------|-------------|
| `Snapshotor` | Core snapshot manager implementation |
| `MultiRaftSnapshotManager` | Multi-raft group snapshot management |
| `io.Writer` | Atomic snapshot writer |
| `io.Reader` | Snapshot reader |
| `io.Receiver` | Snapshot chunk receiver (from leader) |

## Snapshot Backends

| Backend | Constant | Use Case |
|---------|----------|----------|
| Durable | `builder.DurableSnapshot` | Production: persistent filesystem storage |
| Volatile | `builder.VolatileSnapshot` | Testing: in-memory, non-persistent |
| S3 | `builder.S3Snapshot` | Cloud: AWS S3 or S3-compatible object storage |

## Usage

### Create via Builder

```go
import "github.com/fanaujie/babuza/pkg/builder"

// Durable snapshots
component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    SnapshotType:   builder.DurableSnapshot,
}).Build()

snapshotMgr := component.SnapshotManager
```

### S3 Configuration

```go
import "github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"

component := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
    StorageRootDir: "/var/lib/babuza",
    SnapshotType:   builder.S3Snapshot,
}).SetS3Config(&cloudstorage.S3Config{
    Endpoint:        "http://s3.amazonaws.com",  // or S3-compatible endpoint
    Region:          "us-east-1",
    AccessKeyID:     "access-key",
    SecretAccessKey: "secret-key",
    UsePathStyle:    false,                      // true for S3-compatible services
    Bucket:          "babuza-snapshots",
    Prefix:          "snapshots/",               // optional: object key prefix
}).Build()
```

### Direct Creation

```go
import "github.com/fanaujie/babuza/pkg/snapshot"

// Durable manager (filesystem-based)
durableMgr := snapshot.NewDurableSnapshotManager("/var/lib/babuza/snap", logger)

// Volatile manager (in-memory, for testing)
volatileMgr := snapshot.NewVolatileSnapshotManager("/tmp/snap", logger)

// S3 manager (AWS S3 or S3-compatible cloud storage)
s3Mgr := snapshot.NewS3SnapshotManager("/var/lib/babuza/snap", s3Config, logger)

// With options
durableMgr := snapshot.NewDurableSnapshotManager("/var/lib/babuza/snap", logger,
    snapshot.SetOptsWithMaxKeepSnapFiles(5),    // keep up to 5 snapshots (default: 3)
    snapshot.SetOptsWithSnapshotVersion(2),     // snapshot format version (default: 1)
)
```

### Create Snapshot

```go
// Create atomic snapshot writer
writer, err := snapshotMgr.CreateAtomicSnapshotWriter(term, index)
if err != nil {
    return err
}

// Write state machine data
fileWriter, err := writer.CreateStateMachineFile("data", babuzapb.SnapshotFileCompressionType_SNAPPY)
if err != nil {
    return err
}
// Write data to fileWriter...
fileWriter.Close()

// Commit snapshot
metadata, err := writer.Commit(raftSnapshot)
```

### Load Snapshot

```go
// Scan installed snapshots
err := snapshotMgr.ScanInstalledSnapshots(true)

// Load last valid snapshot
walSnapshots, _ := walMgr.FindSnapshot()
snap, err := snapshotMgr.LoadLastValidSnapshot(walSnapshots)

// Create reader for snapshot
reader, err := snapshotMgr.CreateInstalledSnapshotReader(snap.Metadata.Index, true)
defer reader.Close()

// Read state machine data
dataReader, fileDesc, err := reader.Open("data")
// Read from dataReader...

// Get snapshot metadata
metadata := reader.Metadata()

// Iterate all files
err = reader.ForEachFile(func(r io.Reader, desc babuzapb.SnapshotFileDesc) error {
    // Process each file...
    return nil
})

// Create tar archive for transfer
tarReader, err := reader.CreateTarArchiveReader()
defer tarReader.Close()
```

### Receive Snapshot from Leader

```go
// Create receiver for incoming snapshot
receiver, err := snapshotMgr.CreateAtomicSnapshotReceiver(metadata)

// Receive chunks
for _, chunk := range chunks {
    err := receiver.SaveChunk(snapshotIndex, chunk)
}

// Commit received snapshot
err = receiver.Commit(snapshotIndex)
```

### Purge Old Snapshots

```go
// Start automatic purger
snapshotMgr.Purger().Start()

// Manual purge
err := snapshotMgr.Purge(currentSnapshot)
```

### MultiRaft Snapshot Management

For systems running multiple Raft groups, use `MultiRaftSnapshotManager`:

```go
import (
    "github.com/fanaujie/babuza/pkg/snapshot"
    "github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
)

// Create multi-raft snapshot manager
config := snapshot.Config{
    SnapshotVersion: 1,
    MaxSnapFiles:    3,
    SnapshotDir:     "/var/lib/babuza/snap",
}
fs := durable.NewSnapshotFS()
multiMgr := snapshot.NewMultiRaftSnapshotManager(config, fs, logger)

// Scan existing snapshots for multiple groups
groupIDs := []ibabuza.RaftGroupID{1, 2, 3}
managers, err := multiMgr.ScanInstalledSnapshots(groupIDs, true)

// Create snapshot manager for a specific group
groupMgr := multiMgr.CreateSnapshotManager(groupID)

// Start shared purger (handles all groups)
multiMgr.Purger().Start()

// Remove all data for a group
err = multiMgr.RemoveData(groupID)
```

## Interfaces

### SnapshotManager

```go
type SnapshotManager interface {
    ScanInstalledSnapshots(removeUnfinishedSnapshotDir bool) error
    LoadLastValidSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
    CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex uint64) (AtomicSnapshotWriter, error)
    CreateInstalledSnapshotReader(snapshotIndex uint64, validateFsmFiles bool) (SnapshotReader, error)
    CreateAtomicSnapshotReceiver(metadata babuzapb.SnapshotMetadata) (AtomicSnapshotReceiver, error)
    Purger() SnapshotPurger
    Purge(snapshot raftpb.Snapshot) error
    Close() error
}
```

### AtomicSnapshotWriter

```go
type AtomicSnapshotWriter interface {
    CreateStateMachineFile(fileTag string, compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
    AddStateMachineFileMetadata(fileTag string, metadata []byte) error
    CreateClusterFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
    CreateSessionFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
    Commit(raftpb.Snapshot) (babuzapb.SnapshotMetadata, error)
}
```

### SnapshotReader

```go
type SnapshotReader interface {
    Open(fileTag string) (io.Reader, StateMachineFileDesc, error)
    Close() error
    ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error
    Metadata() babuzapb.SnapshotMetadata
    Cluster() (io.Reader, error)
    Session() (io.Reader, error)
    CreateTarArchiveReader() (io.ReadCloser, error)
}
```

### AtomicSnapshotReceiver

```go
type AtomicSnapshotReceiver interface {
    SaveChunk(snapshotIndex uint64, msg babuzapb.SnapshotChunkMessage) error
    DeleteDir() error
    Commit(snapshotIndex uint64) error
}
```

## Snapshot File Compression

| Type | Description |
|------|-------------|
| `SnapshotFileCompression_None` | No compression |
| `SnapshotFileCompression_Snappy` | Snappy compression |

