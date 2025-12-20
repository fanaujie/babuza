# testcluster

Test cluster framework for orchestrating Babuza integration tests.

## Overview

The `testcluster` package provides a `BabuzaCluster` abstraction that simplifies creating, managing, and testing multi-node Raft clusters in integration tests. It supports both direct network connections and proxy network mode for simulating network partitions and fault injection.

## Key Types

| Type | Description |
|------|-------------|
| `BabuzaCluster` | Orchestrates a test cluster with multiple nodes |
| `Peer` | Interface representing a cluster peer |
| `ProxyPeer` | Extended peer interface with proxy network support |
| `StandardPeer` | Peer implementation for direct network tests |
| `BabuzaPeer` | Peer implementation for proxy network tests |
| `ConnectedGroup` | Tracks peers in a connected network partition |
| `EmbeddedApp` | Interface for embedded applications (e.g., kvstore) |
| `EmbeddedClient` | Interface for cluster management operations |
| `CreateEmbeddedApp` | Factory function for creating embedded apps |

## Peer Interfaces

### Peer

The `Peer` interface represents a cluster node:

```go
type Peer interface {
    ID() uint64
    IsPeerLearner() bool
    RaftListenAddress(useProxyNetwork bool) string
    ApplicationServiceAddresses() []string
    RaftTLSConfig() ibabuza.TLSConfig
    SetAppServiceAddresses([]string)
    SetRaftListenAddress(string)
}
```

### ProxyPeer

The `ProxyPeer` interface extends `Peer` with proxy network capabilities:

```go
type ProxyPeer interface {
    Peer
    ProxyListenAddress() string
    ProxyConfig() ibabuza.ProxyConfig
    SetProxyListenAddress(string)
}
```

### Peer Implementations

| Type | Use Case |
|------|----------|
| `StandardPeer` | Direct network tests without fault simulation |
| `BabuzaPeer` | Proxy network tests with fault simulation (implements `ProxyPeer`) |

## ConnectedGroup

`ConnectedGroup` tracks which peers are in the same network partition. When simulating network failures, update the connected group to reflect connectivity changes:

```go
// Create connected group with initial peers
connectedGroup := testcluster.NewConnectedGroup([]uint64{1, 2, 3})

// Remove peer from connected group (simulating disconnect)
connectedGroup.Remove(peerID)

// Add peer back to connected group (simulating reconnect)
connectedGroup.Add(peerID)

// Get current connected peer IDs for cluster operations
peerIDs := connectedGroup.GetIDs()
```

## Usage

### Create a Test Cluster

```go
import (
    "github.com/fanaujie/babuza/test/testcluster"
    "github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
)

// For direct network tests (no fault simulation)
cluster := testcluster.CreateTestCluster(
    clusterID,         // uint64
    storageRootDir,    // string
    nil,               // no proxy network
    createEmbeddedApp, // factory function
)

// For proxy network tests (with fault simulation)
proxyNetwork := proxynetwork.New()
cluster := testcluster.CreateTestCluster(
    clusterID,
    storageRootDir,
    proxyNetwork,
    createEmbeddedApp,
)
```

### Create Peers

```go
// Standard peer for direct network
peer := &testcluster.StandardPeer{
    Id:                  1,
    RaftListenAddr:      "127.0.0.1:14201",
    AppServiceAddresses: []string{"127.0.0.1:10001"},
    IsLearner:           false,
}

// Proxy peer for fault simulation
proxyPeer := &testcluster.BabuzaPeer{
    Id:                  1,
    RaftListenAddr:      "127.0.0.1:14201",
    ProxyListenAddr:     "127.0.0.1:24201",
    AppServiceAddresses: []string{"127.0.0.1:10001"},
    IsLearner:           false,
}
```

### Start Cluster

```go
wait := cluster.RaftElectionTimeout() * 3

// Create peers
peers := []testcluster.Peer{
    &testcluster.BabuzaPeer{Id: 1, RaftListenAddr: "127.0.0.1:14201", ProxyListenAddr: "127.0.0.1:24201", ...},
    &testcluster.BabuzaPeer{Id: 2, RaftListenAddr: "127.0.0.1:14202", ProxyListenAddr: "127.0.0.1:24202", ...},
    &testcluster.BabuzaPeer{Id: 3, RaftListenAddr: "127.0.0.1:14203", ProxyListenAddr: "127.0.0.1:24203", ...},
}
connectedGroup := testcluster.NewConnectedGroup([]uint64{1, 2, 3})

// Start cluster
err := cluster.MakeCluster(wait, peers)

// Wait for leader election
leaderID, err := cluster.CheckOneLeader(wait, connectedGroup.GetIDs())
```

### Cluster Operations

```go
// Join a new peer (learner or voter)
newPeer := &testcluster.BabuzaPeer{Id: 4, IsLearner: true, ...}
connectedGroup.Add(4)
err := cluster.JoinPeerToCluster(wait, embeddedClient, newPeer, connectedGroup.GetIDs())

// Promote learner to voter (via EmbeddedClient)
err := embeddedClient.PromoteLearner(ctx, 4)

// Remove a peer
connectedGroup.Remove(4)
err := cluster.RemovePeerFromCluster(wait, embeddedClient, 4)

// Shutdown a peer
err := cluster.ShutdownPeer(peerID)

// Restart a peer
err := cluster.RestartPeer(wait, peer, connectedGroup.GetIDs())

// Transfer leadership (via EmbeddedClient)
err := embeddedClient.TransferLeader(ctx, targetPeerID)

// Check state machine consistency
err := cluster.CheckPeersConsistency(wait, connectedGroup.GetIDs())

// Cleanup
err := cluster.Teardown()
```

### Disaster Recovery

```go
// Recover a peer as standalone node (single-node cluster)
err := cluster.RecoverPeerAsStandalone(wait, peer)
```

## Network Fault Simulation

testcluster uses [TCP Proxy Network](../../pkg/transport/README.md#tcp-proxy-network) to simulate network failures. By inserting TCP proxies between Raft nodes, you can programmatically control:

- **Node Disconnection**: Simulate node failures or network unreachability
- **Network Partitions**: Simulate split-brain scenarios to test majority election
- **Connection Recovery**: Test log replication when nodes rejoin

### Disconnect and Reconnect Peers

```go
// Disconnect a peer (requires proxy network)
err := cluster.DisconnectPeer(peerID)
connectedGroup.Remove(peerID)

// Wait and verify no leader in minority partition
err := cluster.CheckNoLeader(wait, []uint64{peerID})

// Reconnect the peer
err := cluster.ConnectPeer(peerID)
connectedGroup.Add(peerID)

// Verify leader election in restored cluster
leaderID, err := cluster.CheckOneLeader(wait, connectedGroup.GetIDs())
```

### Simulate Network Partition

```go
// Create partition: nodes 1,2 connected; node 3 isolated
cluster.SetPartition([]uint64{1, 2})
connectedGroup = testcluster.NewConnectedGroup([]uint64{1, 2})

// Verify leader elected in majority partition
leaderID, err := cluster.CheckOneLeader(wait, connectedGroup.GetIDs())

// Heal partition (restore full connectivity)
cluster.SetPartition([]uint64{1, 2, 3})
connectedGroup.Add(3)
```

## BabuzaCluster Methods

### Cluster Lifecycle

| Method | Description |
|--------|-------------|
| `CreateTestCluster(clusterID, storageDir, proxyNetwork, createApp)` | Create a new test cluster |
| `MakeCluster(wait, peers)` | Initialize and start all peers |
| `Teardown()` | Stop all nodes and cleanup resources |

### Peer Management

| Method | Description |
|--------|-------------|
| `JoinPeerToCluster(wait, client, peer, connectedGroup)` | Add a new peer (learner or voter) |
| `RemovePeerFromCluster(wait, client, peerID)` | Remove a peer from the cluster |
| `ShutdownPeer(peerID)` | Stop a specific peer |
| `RestartPeer(wait, peer, connectedGroup)` | Restart a stopped peer |
| `RecoverPeerAsStandalone(wait, peer)` | Recover peer as single-node cluster |

### Network Control (Proxy Network Only)

| Method | Description |
|--------|-------------|
| `DisconnectPeer(peerID)` | Disconnect a peer from the network |
| `ConnectPeer(peerID)` | Reconnect a peer to the network |
| `SetPartition(peerIDs)` | Define which peers can communicate |
| `IsUseProxyNetwork()` | Check if proxy network is enabled |

### Cluster Verification

| Method | Description |
|--------|-------------|
| `CheckOneLeader(wait, connectedGroup)` | Wait for exactly one leader |
| `CheckNoLeader(wait, connectedGroup)` | Verify no leader exists |
| `CheckPeersConsistency(wait, connectedGroup)` | Verify state machine consistency |
| `CheckStatus(wait, peerID, matchFunc)` | Check peer status with custom matcher |
| `CheckPeerExists(ctx, clusterID, peer)` | Verify peer exists in cluster config |

### Utilities

| Method | Description |
|--------|-------------|
| `RaftElectionTimeout()` | Get Raft election timeout duration |
| `GetAllRaft()` | Get all Raft instances |
| `GetAllAppServiceAddresses()` | Get all application service addresses |
| `GetAppServiceAddresses(peerIDs)` | Get service addresses for specific peers |
| `ExecutePeerRaftOperation(peerID, func)` | Execute operation on peer's Raft |

## EmbeddedApp Interface

```go
type EmbeddedApp interface {
    PublishService(context.Context) chan error
    StartService() error
    Stop() error
    Raft() *babuza.Raft
    StateMachineHash() uint32
}
```

## EmbeddedClient Interface

```go
type EmbeddedClient interface {
    Join(ctx context.Context, peerID uint64, raftListenAddr string, isLearner bool) error
    Update(ctx context.Context, peerID uint64, raftListenAddr string) error
    Remove(ctx context.Context, peerID uint64) error
    PromoteLearner(ctx context.Context, peerID uint64) error
    TransferLeader(ctx context.Context, transferee uint64) error
}
```

## Example Test

```go
func TestReElection(t *testing.T) {
    storageDir := t.TempDir()
    proxyNetwork := proxynetwork.New()

    cluster := testcluster.CreateTestCluster(1, storageDir, proxyNetwork, createKvApp)
    defer cluster.Teardown()

    wait := cluster.RaftElectionTimeout() * 3

    // Create 3 voting peers with proxy support
    peers := []testcluster.Peer{
        &testcluster.BabuzaPeer{Id: 1, RaftListenAddr: "127.0.0.1:14201", ProxyListenAddr: "127.0.0.1:24201"},
        &testcluster.BabuzaPeer{Id: 2, RaftListenAddr: "127.0.0.1:14202", ProxyListenAddr: "127.0.0.1:24202"},
        &testcluster.BabuzaPeer{Id: 3, RaftListenAddr: "127.0.0.1:14203", ProxyListenAddr: "127.0.0.1:24203"},
    }
    connectedGroup := testcluster.NewConnectedGroup([]uint64{1, 2, 3})

    require.NoError(t, cluster.MakeCluster(wait, peers))

    // Wait for initial leader
    leader1, err := cluster.CheckOneLeader(wait, connectedGroup.GetIDs())
    require.NoError(t, err)

    // Disconnect leader to trigger re-election
    require.NoError(t, cluster.DisconnectPeer(leader1))
    connectedGroup.Remove(leader1)

    // Verify new leader elected
    leader2, err := cluster.CheckOneLeader(wait, connectedGroup.GetIDs())
    require.NoError(t, err)
    require.NotEqual(t, leader1, leader2)

    // Reconnect old leader
    require.NoError(t, cluster.ConnectPeer(leader1))
    connectedGroup.Add(leader1)
}
```
