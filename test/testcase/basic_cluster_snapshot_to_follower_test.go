package testcase

import (
    "context"
    "crypto/rand"
    "fmt"
    "github.com/fanaujie/babuza/examples/kvstore/client"
    "github.com/fanaujie/babuza/examples/kvstore/embedapp"
    babuza "github.com/fanaujie/babuza/raft"
    "github.com/fanaujie/babuza/test/testcluster"
    "github.com/stretchr/testify/assert"
    "testing"
    "time"
)

type BasicSendSnapshotToFollower struct {
    t             *testing.T
    snapshotCount uint64
}

func (c *BasicSendSnapshotToFollower) Log(s string) {
    c.t.Log(s)
}

func (c *BasicSendSnapshotToFollower) CreateTestComponents() []BabuzaComponent {
    return basicSnapshotTestComponents(c.snapshotCount)
}

func (c *BasicSendSnapshotToFollower) Run(tc *testcluster.BabuzaCluster, testParams any) {
    wait := tc.RaftElectionTimeout() * 3
    peers, connectGroup := makeVotingStandardPeers(3)
    assert.Nil(c.t, tc.MakeCluster(wait, peers))
    // Identify the current leader
    leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
    assert.Nil(c.t, err)

    // Create a client with automatic incrementing session
    kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
    assert.Nil(c.t, err)
    defer func() {
        _ = kvClient.Close()
    }()

    // Write enough data to trigger snapshot creation
    // We write (snapshotCount + 10) entries to ensure snapshot is created
    data := make([]byte, 10*1024) // 10KB
    _, err = rand.Read(data)
    assert.Nil(c.t, err)
    sData := string(data)
    //test exceeding snapshot size of 5MB
    // 10KB * (500 + 10) > 5MB
    for i := uint64(0); i < c.snapshotCount+10; i++ {
        assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
            v := fmt.Sprintf("%d", i)

            res, cErr := kvClient.Set(ctx, v, sData)
            assert.Equal(c.t, v, res.Key)
            //assert.Equal(c.t, sData, res.Value)
            return cErr
        }))
    }

    // Verify all peers have consistent state
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

    // Record snapshot metadata from leader for later verification
    lastSnapshotIndex := uint64(0)
    lastSnapshotTerm := uint64(0)
    assert.Nil(c.t, tc.CheckStatus(wait, leaderId, func(s babuza.Status) bool {
        lastSnapshotIndex = s.LastSnapshotIndex
        lastSnapshotTerm = s.LastSnapshotTerm
        return s.LastSnapshotIndex >= c.snapshotCount &&
            s.LastSnapshotTerm > 0
    }))

    // Add a new follower to the cluster that will receive a snapshot
    newFollowerId := uint64(4)
    newFollower := makeSingleStandardPeer(newFollowerId, false)
    connectGroup.Add(newFollowerId)

    // Join the new node to the cluster - it should receive a snapshot
    assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, newFollower, connectGroup.GetIds()))

    // Check if the new follower properly joined
    assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
        return tc.CheckPeerExists(ctx, leaderId, newFollower)
    }))

    // Wait a bit for snapshot transfer to complete
    time.Sleep(time.Second)

    // Verify the snapshot was transferred properly to the new follower
    assert.Nil(c.t, tc.CheckStatus(wait, newFollowerId, func(s babuza.Status) bool {
        return s.LastSnapshotIndex == lastSnapshotIndex &&
            s.LastSnapshotTerm == lastSnapshotTerm
    }))

    // Write additional data with the new follower in the cluster
    for i := uint64(100); i < 108; i++ {
        assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
            v := fmt.Sprintf("%d", i)
            res, cErr := kvClient.Set(ctx, v, v)
            assert.Equal(c.t, v, res.Key)
            assert.Equal(c.t, v, res.Value)
            return cErr
        }))
    }

    // Final verification that all nodes (including the new follower) have identical state
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestSendSnapshotToFollower(t *testing.T) {
    RunTests(&BasicSendSnapshotToFollower{
        t:             t,
        snapshotCount: 500,
    })
}
