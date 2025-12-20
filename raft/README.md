# raft

Core consensus layer wrapping etcd Raft with cluster bootstrap and management APIs.

## Overview

The `raft` package provides the main entry point for building distributed systems with Babuza. It handles cluster bootstrapping, log proposal, linearizable reads, cluster membership changes, and disaster recovery.

## Key Types

| Type | Description |
|------|-------------|
| `Raft` | Main Raft consensus engine with Propose, Read, and cluster operations |
| `BabuzaConfig` | Configuration for Raft node (cluster ID, peer ID, timeouts, TLS) |
| `RaftConfig` | Low-level Raft parameters (ticks, message sizes, quorum checks) |
| `PeersConfiguration` | Initial cluster peer configuration |
| `BootstrapRaftCluster` | Bootstrap context for starting/restarting a node |
| `ClientSession` | Session tracking for idempotent proposals |
| `Status` | Current Raft state (leader, term, indices) |
| `RaftState` | Enum: Follower, Candidate, Leader, PreCandidate, Stop |

## Usage

### Bootstrap a New Cluster

```go
cfg := raft.DefaultBabuzaConfig(1, 1, "127.0.0.1:7001")

peers := raft.NewPeersConfiguration()
peers.AddPeer(1, "127.0.0.1:7001", false)  // voting peer
peers.AddPeer(2, "127.0.0.1:7002", false)
peers.AddPeer(3, "127.0.0.1:7003", false)

bootstrap, err := raft.NewBootstrapRaftCluster(
    cfg, peers, stateMachine,
    component.Cluster, component.RaftNode, component.SessionManager,
    component.SnapshotManager, component.WalManager, component.Transport,
    component.Logger, component.MetricsController,
)
if err != nil {
    log.Fatal(err)
}

r, err := raft.NewRaft(cfg, bootstrap, nil)
if err != nil {
    log.Fatal(err)
}
defer r.Shutdown().Wait()
```

### Join an Existing Cluster

```go
cfg := raft.DefaultBabuzaConfig(1, 4, "127.0.0.1:7004")
cfg.Join = true  // Join mode

peers := raft.NewPeersConfiguration()
peers.AddPeer(1, "127.0.0.1:7001", false)
peers.AddPeer(2, "127.0.0.1:7002", false)
peers.AddPeer(3, "127.0.0.1:7003", false)
peers.AddPeer(4, "127.0.0.1:7004", true)  // new node as learner

bootstrap, _ := raft.NewBootstrapRaftCluster(cfg, peers, stateMachine, ...)
r, _ := raft.NewRaft(cfg, bootstrap, nil)
```

### Propose a Command

```go
// Register a session first
result := r.RegisterSession(ctx)
defer result.Release()
applyResult := result.WaitForApplyResult()
if applyResult.Error != nil {
    return applyResult.Error
}
clientSession := raft.ClientSession{
    SessionID:      applyResult.Response.(uint64),
    SequenceNumber: 1,
}

// Propose a command
data := []byte("SET key value")
proposeResult := r.Propose(ctx, clientSession, data)
defer proposeResult.Release()
ar := proposeResult.WaitForApplyResult()
if ar.Error != nil {
    return ar.Error
}
// ar.Response contains the result from state machine
// ar.LogIndex contains the applied log index
```

### Linearizable Read

```go
// Wait for read index to be applied
if err := r.LinearizableRead(ctx); err != nil {
    return err
}
// Now safe to read from state machine
value, err := r.GetStateMachine().Query("key")
```

### Cluster Operations

```go
// Add a voting peer
r.AddVotingPeer(ctx, session, babuzapb.RaftPeerAttribute{
    PeerID:         5,
    RaftListenAddr: "127.0.0.1:7005",
})

// Add a learner
r.AddLearner(ctx, session, babuzapb.RaftPeerAttribute{
    PeerID:         6,
    RaftListenAddr: "127.0.0.1:7006",
    IsLearner:      true,
})

// Promote learner to voter
r.PromoteLearner(ctx, session, 6)

// Remove a peer
r.RemovePeer(ctx, session, 5)

// Transfer leadership
r.TransferLeader(ctx, 2)
```

### Disaster Recovery

```go
// Recover a standalone node from existing WAL/snapshot
// when other cluster nodes are permanently lost
bootstrap, err := raft.RecoverAsStandalone(
    cfg, stateMachine,
    component.Cluster, component.RaftNode, component.SessionManager,
    component.SnapshotManager, component.WalManager, component.Transport,
    component.Logger, component.MetricsController,
)
```

### Manual Snapshot

```go
result := r.ManualSnapshot(ctx)
if err := result.Wait(); err != nil {
    return err
}
metadata, err := result.SnapshotMetadata()
if err != nil {
    return err
}
fmt.Printf("Snapshot created at index %d\n", metadata.Snapshot.Metadata.Index)
```

## Configuration

### BabuzaConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ClusterID` | uint64 | - | Unique cluster identifier |
| `LocalPeerID` | uint64 | - | This node's peer ID |
| `RaftListenAddress` | string | - | Address for Raft communication |
| `Join` | bool | false | Join existing cluster vs. bootstrap new |
| `EnableWalNoSync` | bool | false | Disable fsync (for testing only) |
| `SnapshotCount` | uint64 | 10000 | Entries before triggering snapshot |
| `TLSConfig` | TLSConfig | - | TLS/mTLS configuration |
| `LearnerReadyPercent` | float64 | 0.95 | Threshold for learner promotion |
| `LinearizedReadRequestTimeout` | Duration | 3s | Read request timeout |
| `LinearizedReadRetryTimeout` | Duration | 500ms | Read retry interval |

### RaftConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `LogicalTickMs` | int | 100 | Tick interval in milliseconds |
| `ElectionTicks` | int | 10 | Election timeout in ticks |
| `HeartbeatTicks` | int | 1 | Heartbeat interval in ticks |
| `MaxSizePerMsg` | uint64 | 1MB | Max bytes per Raft message |
| `MaxCommittedSizePerReady` | uint64 | MaxUint64 | Max committed bytes per ready |
| `MaxUncommittedEntriesSize` | uint64 | 1GB | Max uncommitted entries bytes |
| `MaxInflightMsgs` | int | 512 | Max in-flight messages per peer |
| `CheckQuorum` | bool | true | Leader steps down without quorum |
| `PreVote` | bool | true | Enable pre-vote protocol |
| `DisableProposalForwarding` | bool | true | Reject proposals on followers |

## Raft API Methods

| Method | Description |
|--------|-------------|
| `RegisterSession(ctx)` | Create a new client session |
| `UnregisterSession(ctx, sessionID)` | Remove a client session |
| `Propose(ctx, session, data)` | Propose a log entry |
| `ProposeThenWaitResponse(ctx, session, data)` | Propose and wait for apply result (convenience method) |
| `LinearizableRead(ctx)` | Wait for linearizable read point |
| `AddVotingPeer(ctx, session, attr)` | Add a voting member |
| `AddLearner(ctx, session, attr)` | Add a learner (non-voting) |
| `PromoteLearner(ctx, session, id)` | Promote learner to voter |
| `RemovePeer(ctx, session, id)` | Remove a cluster member |
| `UpdatePeer(ctx, session, attr)` | Update peer attributes |
| `TransferLeader(ctx, transferee)` | Transfer leadership |
| `ManualSnapshot(ctx)` | Trigger manual snapshot |
| `ApplicationServiceStart(ctx, addresses)` | Publish application service addresses to cluster |
| `Status()` | Get current Raft status |
| `ClusterInfo()` | Get cluster information |
| `GetStateMachine()` | Access the state machine |
| `Shutdown()` | Gracefully shutdown |

## Error Handling

Common errors returned by Raft operations:

| Error | Description |
|-------|-------------|
| `ErrNoLeader` | Cluster has no elected leader |
| `ErrNotLeader` | Current node is not the leader (proposal rejected) |
| `ErrStopped` | Raft node has been shut down |
| `ErrLearnerNotReady` | Learner is not synced with leader, cannot promote |
| `ErrNotLearner` | Node is not a learner, cannot promote |
| `ErrLearnerCanNotVote` | Learner cannot become a voting member directly |
| `ErrVotingMemberCanNotPromote` | Voting member cannot be promoted (already a voter) |

## Session Management

Sessions provide **exactly-once semantics** for proposals, preventing duplicate execution on retries.

### ClientSession Fields

```go
type ClientSession struct {
    SessionID                         uint64  // Unique session identifier
    SequenceNumber                    uint64  // Must increment for each proposal
    LowestSequenceNumberNotYetReplied uint64  // For response cache cleanup
}
```

### Usage Pattern

```go
// 1. Register session once per client
result := r.RegisterSession(ctx)
defer result.Release()
ar := result.WaitForApplyResult()
if ar.Error != nil {
    return ar.Error
}
sessionID := ar.Response.(uint64)

// 2. Create session with sequence number starting at 1
session := raft.ClientSession{
    SessionID:      sessionID,
    SequenceNumber: 1,
}

// 3. Increment SequenceNumber for each proposal
for _, cmd := range commands {
    result := r.Propose(ctx, session, cmd)
    defer result.Release()
    ar := result.WaitForApplyResult()
    if ar.Error != nil {
        return ar.Error
    }
    session.SequenceNumber++  // Important: increment after each proposal
}

// 4. Unregister session when done (optional)
r.UnregisterSession(ctx, sessionID)
```

**Note:** If the same `SessionID + SequenceNumber` is proposed twice, the second proposal returns the cached result instead of re-executing.

## Application Service Discovery

`ApplicationServiceStart` publishes the node's application service addresses (e.g., HTTP API endpoints) to the Raft cluster, enabling service discovery across all nodes.

### Use Case

When your application exposes services (REST API, gRPC, etc.) on top of Raft, other nodes and clients need to discover these endpoints. This method replicates the addresses through Raft consensus so all nodes have a consistent view of the cluster's service topology.

### Usage

```go
// After Raft node starts, publish application service addresses
errCh := r.ApplicationServiceStart(ctx, []string{
    "http://127.0.0.1:8001",  // REST API
    "grpc://127.0.0.1:9001",  // gRPC endpoint
})

// Wait for publication to complete
if err := <-errCh; err != nil {
    log.Fatalf("Failed to publish service addresses: %v", err)
}
```

### Retrieving Service Addresses

Other nodes can retrieve all application service addresses via `ClusterInfo()`:

```go
info := r.ClusterInfo()
for _, peer := range info.Peers {
    fmt.Printf("Peer %d services: %v\n", peer.RaftPeerAttr.PeerID, peer.AppServiceAddresses)
}
```

This enables clients to:
- Discover all available service endpoints in the cluster
- Implement client-side load balancing
- Redirect requests to the appropriate node
