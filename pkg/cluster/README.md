# pkg/cluster

Cluster membership management for tracking peers in a Babuza Raft cluster.

## Overview

The `cluster` package implements the `ibabuza.Cluster` interface, managing cluster metadata including peer information, cluster ID, and local peer ID. It handles membership changes (add, remove, update, promote) and supports snapshot/restore for persistence.

## Key Types

| Type | Description |
|------|-------------|
| `Cluster` | Implementation of ibabuza.Cluster interface |

## Usage

### Create a Cluster

```go
import "github.com/fanaujie/babuza/pkg/cluster"

c := cluster.NewCluster()
c.SetClusterID(1)
c.SetLocalPeerID(1)
```

### Manage Peers

```go
// Add a peer
err := c.Add(babuzapb.RaftPeerAttribute{
    PeerID:         2,
    RaftListenAddr: "127.0.0.1:7002",
    IsLearner:      false,
})

// Get peer info
peer, err := c.Peer(2)

// List all peers
peers := c.Peers()

// Update peer
err = c.Update(2, babuzapb.RaftPeerAttribute{
    PeerID:         2,
    RaftListenAddr: "127.0.0.1:7003",  // new address
    IsLearner:      false,
})

// Promote learner to voter
err = c.Promote(2)

// Remove peer
err = c.Remove(2)

// Update application service addresses
err = c.UpdateAppServiceAddresses(2, []string{"http://127.0.0.1:8002"})
```

### Snapshot and Restore

```go
// Snapshot cluster state
var buf bytes.Buffer
err := c.Snapshot(&buf)

// Restore from snapshot
err = c.Restore(&buf)
```

## Interface

```go
type Cluster interface {
    SetClusterID(clusterID uint64)
    SetGroupID(groupID RaftGroupID)
    SetLocalPeerID(localPeerID uint64)
    Peer(peerID uint64) (babuzapb.Peer, error)
    Snapshot(io.Writer) error
    Restore(io.Reader) error
    Peers() []babuzapb.Peer
    ClusterID() uint64
    GroupID() RaftGroupID
    LocalPeerID() uint64
    Add(babuzapb.RaftPeerAttribute) error
    Update(peerID uint64, attr babuzapb.RaftPeerAttribute) error
    Remove(peerID uint64) error
    Promote(peerID uint64) error
    UpdateAppServiceAddresses(peerID uint64, addresses []string) error
}
```

## Error Types

| Error | Description |
|-------|-------------|
| `ErrPeerIDRemoved` | Peer ID was previously removed and cannot be reused |
| `ErrPeerIDExists` | Peer ID already exists in the cluster |
| `ErrPeerIDNotFound` | Peer ID not found in the cluster |
| `ErrPeerNotLearner` | Cannot promote a peer that is not a learner |
| `ErrPeerRaftListenAddrExists` | Raft listen address already exists |

## Implementation Notes

- **Thread Safety**: All methods are thread-safe using `sync.RWMutex`
- **Removed ID Tracking**: Once a peer is removed, its ID cannot be reused (`Add` will return `ErrPeerIDRemoved`)
- **Peers() Ordering**: Returns peers sorted by PeerID in ascending order
- **Update() Behavior**: Only updates `RaftListenAddr`, `IsLearner`, and `StoreID` fields; use `UpdateAppServiceAddresses` for application addresses
