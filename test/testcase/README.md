# testcase

Integration test cases for verifying Babuza Raft behavior.

## Overview

This package contains comprehensive integration tests that verify cluster operations, failure handling, session management, snapshot operations, and disaster recovery scenarios.

## Running Tests

```bash
# All tests
go test -v ./...

# Specific test
go test -v -run TestBasicCluster

# With timeout
go test -v -timeout 5m ./...

# With race detection
go test -race ./...
```

## Test Categories

### Basic Cluster Operations

| File | Test | Description |
|------|------|-------------|
| `basic_cluster_test.go` | `TestBasicCluster` | Cluster startup and basic proposal |
| `basic_join_peer_test.go` | `TestBasicJoinPeer` | Add voting peer |
| `basic_join_learner_test.go` | `TestBasicJoinLearner` | Add learner node |
| `basic_promote_learner_test.go` | `TestBasicPromoteLearner` | Promote learner to voter |
| `basic_remove_follower_test.go` | `TestBasicRemoveFollower` | Remove follower |
| `basic_remove_leader_test.go` | `TestBasicRemoveLeader` | Remove leader, verify reelection |
| `basic_update_peer_test.go` | `TestBasicUpdatePeer` | Update peer address |
| `basic_transfer_leader_test.go` | `TestBasicTransferLeader` | Transfer leadership |
| `basic_follower_forward_proposal_test.go` | `TestFollowerForwardProposal` | Proposal forwarding |
| `basic_multi_client_proposal_test.go` | `TestMultiClientProposal` | Concurrent proposals |
| `basic_multi_client_follower_forward_test.go` | `TestMultiClientFollowerForward` | Concurrent forwarding |

### Session Management

| File | Test | Description |
|------|------|-------------|
| `basic_cluster_client_Idempotency_test.go` | `TestIdempotency` | Duplicate request handling |
| `basic_cluster_client_session_time_expired_test.go` | `TestSessionTimeExpired` | Time-based expiration |
| `basic_cluster_client_session_lru_expired_test.go` | `TestSessionLruExpired` | LRU eviction |
| `basic_cluster_client_session_leadership_test.go` | `TestSessionLeadership` | Session across leader change |

### Snapshot Operations

| File | Test | Description |
|------|------|-------------|
| `basic_snapshot_manual_trigger_test.go` | `TestManualSnapshot` | Trigger snapshot manually |
| `basic_cluster_restart_from_snapshot_test.go` | `TestRestartFromSnapshot` | Node restart with snapshot |
| `basic_cluster_snapshot_to_follower_test.go` | `TestSnapshotToFollower` | Snapshot transfer |

### Network Partition (Proxy Tests)

| File | Test | Description |
|------|------|-------------|
| `proxy_reelection_test.go` | `TestProxyReelection` | Reelection after partition |
| `proxy_leader_restart_reelection_test.go` | `TestLeaderRestartReelection` | Leader restart |
| `proxy_one_follower_disconnection_test.go` | `TestOneFollowerDisconnect` | Single follower disconnect |
| `proxy_quorum_follower_disconnection_test.go` | `TestQuorumDisconnect` | Majority disconnect |
| `proxy_two_partition_test.go` | `TestTwoPartition` | Network split |
| `proxy_linearizability_test.go` | `TestLinearizability` | Read consistency |

### Fault Injection (Proxy Tests)

| File | Test | Description |
|------|------|-------------|
| `proxy_fault_injection_test.go` | `TestFaultInjection` | Cluster resilience under network faults (delay, loss, reorder) |

### Disaster Recovery

| File | Test | Description |
|------|------|-------------|
| `disaster_recovery_standalone_test.go` | `TestDisasterRecoveryStandalone` | Recover as standalone |
| `disaster_recovery_snapshot_test.go` | `TestDisasterRecoverySnapshot` | Recover with snapshot |

## Test Patterns

### Basic Test Structure

```go
func TestExample(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    storageDir := t.TempDir()
    cluster := testcluster.CreateTestCluster(1, storageDir, nil, createApp)
    defer cluster.Shutdown()

    // Setup cluster
    cluster.AddVotingPeer(1, "127.0.0.1:14201")
    cluster.AddVotingPeer(2, "127.0.0.1:14202")
    cluster.AddVotingPeer(3, "127.0.0.1:14203")
    require.NoError(t, cluster.StartCluster(ctx))

    // Wait for leader
    leaderID, err := cluster.WaitForLeader(ctx)
    require.NoError(t, err)

    // Test operations
    // ...

    // Verify results
    // ...
}
```

### Network Partition Test

```go
func TestPartition(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    proxyNetwork := proxy.NewProxyNetwork()
    cluster := testcluster.CreateTestCluster(1, t.TempDir(), proxyNetwork, createApp)
    defer cluster.Shutdown()

    // ... setup cluster ...

    // Create partition
    proxyNetwork.Partition([]uint64{1}, []uint64{2, 3})

    // Verify behavior during partition
    // ...

    // Heal partition
    proxyNetwork.Heal()

    // Verify recovery
    // ...
}
```

### Fault Injection Test

```go
func TestFaultInjection(t *testing.T) {
    proxyNetwork := proxynetwork.New()
    cluster := testcluster.CreateTestCluster(1, t.TempDir(), proxyNetwork, createApp)
    defer cluster.Teardown()

    // ... setup cluster ...

    // Test delay fault
    cluster.SetPeerFault(1, proxynetwork.FaultConfig{
        DelayMin: 30 * time.Millisecond,
        DelayMax: 50 * time.Millisecond,
    })
    // Execute operations...
    cluster.ClearPeerFault(1)

    // Test packet loss
    cluster.SetPeerFault(2, proxynetwork.FaultConfig{
        LossRate: 0.3,
    })
    // Execute operations...
    cluster.ClearPeerFault(2)

    // Test reordering
    cluster.SetPeerFault(3, proxynetwork.FaultConfig{
        ReorderBufferSize:    5,
        ReorderFlushInterval: 100 * time.Millisecond,
    })
    // Execute operations...
    cluster.ClearPeerFault(3)

    // Test combined faults on all peers
    combinedConfig := proxynetwork.FaultConfig{
        LossRate:             0.2,
        DelayMin:             20 * time.Millisecond,
        DelayMax:             40 * time.Millisecond,
        ReorderBufferSize:    3,
        ReorderFlushInterval: 80 * time.Millisecond,
    }
    cluster.SetPeerFault(1, combinedConfig)
    cluster.SetPeerFault(2, combinedConfig)
    cluster.SetPeerFault(3, combinedConfig)
    // Execute operations...
    cluster.ClearPeerFault(1)
    cluster.ClearPeerFault(2)
    cluster.ClearPeerFault(3)

    // Verify cluster consistency
    cluster.CheckPeersConsistency(wait, connectedGroup.GetIDs())
}
```

## Related Packages

| Package | Description |
|---------|-------------|
| [testcluster/](../testcluster/) | Test cluster framework |
| [examples/kvstore/](../../examples/kvstore/) | KV store used in tests |
| [raft/](../../raft/) | Raft consensus being tested |
