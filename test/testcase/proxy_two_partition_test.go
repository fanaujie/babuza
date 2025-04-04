package testcase

import (
    "context"
    "fmt"
    "github.com/fanaujie/babuza/examples/kvstore/client"
    "github.com/fanaujie/babuza/examples/kvstore/embedapp"
    "github.com/fanaujie/babuza/test/testcluster"
    "github.com/stretchr/testify/assert"
    "testing"
)

type TwoPartitionCluster struct {
    t *testing.T
}

func (c *TwoPartitionCluster) Log(s string) {
    c.t.Log(s)
}

func (c *TwoPartitionCluster) CreateTestComponents() []BabuzaComponent {
    var r []BabuzaComponent
    r = append(r, proxyClusterComponents(true, true)...)
    r = append(r, proxyClusterComponents(true, false)...)
    return r
}

func (c *TwoPartitionCluster) Run(tc *testcluster.BabuzaCluster, a any) {
    // Set wait time to 9 times the Raft election timeout
    // This is to ensure that we have enough time for the leader to be elected
    wait := tc.RaftElectionTimeout() * 9

    // Create 5 voting proxy peers
    peers, connectGroup := makeVotingProxyPeers(5)
    assert.Nil(c.t, tc.MakeCluster(wait, peers))

    // Create first partition: nodes 1,2,3 in one group, nodes 4,5 in another
    partition1 := []uint64{1, 2, 3}
    partition2 := []uint64{4, 5}

    // Connect each partition internally
    assert.Nil(c.t, tc.SetPartition(partition1))
    assert.Nil(c.t, tc.SetPartition(partition2))

    // Check that partition1 has a leader (it's the majority)
    partition1LeaderId, err := tc.CheckOneLeader(wait, partition1)
    assert.Nil(c.t, err)

    // Verify leader is in partition1
    findLeader := false
    for _, id := range partition1 {
        if id == partition1LeaderId {
            findLeader = true
            break
        }
    }
    assert.True(c.t, findLeader)

    // Check that partition2 has no leader (it's not a minority)
    assert.Nil(c.t, tc.CheckNoLeader(wait, partition2))

    // Create client to the cluster (connected to partition1)
    kvClient, err := embedapp.NewKvStoreClient(tc.GetAppServiceAddresses(partition1), client.NewNoOpSession())
    assert.Nil(c.t, err)
    defer func() {
        _ = kvClient.Close()
    }()

    // Execute commands on the leader in partition1
    for i := 0; i < 8; i++ {
        assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
            _, err = kvClient.Set(ctx, fmt.Sprintf("foo-%d", i), "foo")
            return err
        }))
    }
    // Check consistency within partition1
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, partition1))

    // Reconnect all nodes - heal the partition
    assert.Nil(c.t, tc.SetPartition(connectGroup.GetIds()))

    // Check leader after healing the partition
    lastLeader, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
    assert.Nil(c.t, err)

    // Leader should still be in partition1 (unchanged)
    leaderInPartition1 := false
    for _, id := range partition1 {
        if id == lastLeader {
            leaderInPartition1 = true
            break
        }
    }
    assert.True(c.t, leaderInPartition1)

    // Check consistency across the entire cluster
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

    // Create second partition: nodes 1,5 in one group, nodes 2,3,4 in another
    partition1 = []uint64{1, 5}
    partition2 = []uint64{2, 3, 4}

    assert.Nil(c.t, tc.SetPartition(partition1))
    assert.Nil(c.t, tc.SetPartition(partition2))

    // Check that partition2 has a leader (it's the majority now)
    partition2LeaderId, err := tc.CheckOneLeader(wait, partition2)
    assert.Nil(c.t, err)

    // Verify leader is in partition2
    findLeader = false
    for _, id := range partition2 {
        if id == partition2LeaderId {
            findLeader = true
            break
        }
    }
    assert.True(c.t, findLeader)

    // Reconnect all peers - heal the partition again
    assert.Nil(c.t, tc.SetPartition(connectGroup.GetIds()))
    _, err = tc.CheckOneLeader(wait, connectGroup.GetIds())
    assert.Nil(c.t, err)

    // Check consistency across the entire cluster
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestTwoPartitionCluster(t *testing.T) {
    RunTests(&TwoPartitionCluster{t: t})
}
