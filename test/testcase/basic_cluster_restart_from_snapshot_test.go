package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type BasicRestartFromSnapshot struct {
	t             *testing.T
	snapshotCount uint64
}

func (c *BasicRestartFromSnapshot) Log(s string) {
	c.t.Log(s)
}

func (c *BasicRestartFromSnapshot) CreateTestComponents() []BabuzaComponent {
	return basicSnapshotTestComponents(c.snapshotCount)
}

func (c *BasicRestartFromSnapshot) Run(tc *testcluster.BabuzaCluster, testParams any) {
	wait := tc.RaftElectionTimeout() * 3
	clusterNodeSize := 5
	peers, connectGroup := makeVotingStandardPeers(clusterNodeSize)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	oldLeaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Create a client with automatic incrementing session
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Write enough data to trigger snapshot creation
	// We write (snapshotCount + 10) entries to ensure snapshot is created
	for i := uint64(0); i < c.snapshotCount+10; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return cErr
		}))
	}

	// Verify all peers have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))

	// Record snapshot metadata from leader for later verification
	lastSnapshotIndex := uint64(0)
	lastSnapshotTerm := uint64(0)
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderID, func(s babuza.Status) bool {
		lastSnapshotIndex = s.LastSnapshotIndex
		lastSnapshotTerm = s.LastSnapshotTerm
		return s.LastSnapshotIndex >= c.snapshotCount &&
			s.LastSnapshotTerm > 0
	}))

	// Part 1: Test restart after snapshot creation
	// Shutdown leader to trigger leadership change
	assert.Nil(c.t, tc.ShutdownPeer(oldLeaderID))
	// Shutdown a follower
	// Peer IDs are 1-indexed
	followerID := oldLeaderID%uint64(clusterNodeSize) + 1
	assert.Nil(c.t, tc.ShutdownPeer(followerID))

	time.Sleep(time.Second * 5) // Wait for election to complete
	connectGroup.Remove(oldLeaderID)
	connectGroup.Remove(followerID)

	// Ensure new leader is elected
	newLeaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Write more data with the new leader
	for i := uint64(100); i < 108; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, err := kvClient.Set(ctx, v, v)
			if err != nil {
				return err
			}
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return nil
		}))
	}

	// Restart the old leader and verify it:
	// 1. Rejoins the cluster as a follower
	// 2. Has the correct snapshot information
	connectGroup.Add(oldLeaderID)
	connectGroup.Add(followerID)
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(oldLeaderID, false), connectGroup.GetIDs()))
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(followerID, false), connectGroup.GetIDs()))
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderID, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))
	assert.Nil(c.t, tc.CheckStatus(wait, followerID, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))

	// Verify all peers (including restarted old leader) have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
	assert.NotEqual(c.t, oldLeaderID, newLeaderId)

	// Part 2: Test restart again to verify consistency is maintained
	// Shutdown old leader again
	assert.Nil(c.t, tc.ShutdownPeer(oldLeaderID))
	// Shutdown the follower again
	assert.Nil(c.t, tc.ShutdownPeer(followerID))

	// Write different data with entirely new keys
	for i := uint64(1000); i < 1008; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, err := kvClient.Set(ctx, v, v)
			if err != nil {
				return err
			}
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return nil
		}))
	}

	// Restart old leader again and verify it still:
	// 1. Rejoins as a follower
	// 2. Has the same snapshot information as before
	// 3. Successfully catches up with new data
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(oldLeaderID, false), connectGroup.GetIDs()))
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(followerID, false), connectGroup.GetIDs()))
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderID, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))
	assert.Nil(c.t, tc.CheckStatus(wait, followerID, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))

	// Final verification that all nodes have identical state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestRestartFromSnapshot(t *testing.T) {
	RunTests(&BasicRestartFromSnapshot{
		t:             t,
		snapshotCount: 50})
}
