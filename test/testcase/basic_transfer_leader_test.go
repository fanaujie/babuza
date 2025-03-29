package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type BasicTransferLeader struct {
	t *testing.T
}

func (c *BasicTransferLeader) Log(s string) {
	c.t.Log(s)
}

func (c *BasicTransferLeader) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicTransferLeader) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	for i := 0; i < 64; i++ {
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.Set(ctx, s, s)
			return err
		}))
	}

	// Choose a different node to transfer leadership to
	transferLeaderId := (leaderId % 3) + 1

	// Transfer leadership
	assert.Nil(c.t, runWithCtxTimeout(wait*2, func(ctx context.Context) error {
		return kvClient.TransferLeader(ctx, transferLeaderId)
	}))

	// Verify the new leader is the one we transferred to
	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, transferLeaderId, leaderId2)

	// Join a learner node
	learner := makeSingleStandardPeer(4, true)
	connectGroup.Add(learner.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, learner, connectGroup.GetIds()))

	// Verify the learner joined successfully
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId2, learner)
	}))

	// Try to transfer leadership to the learner (should fail)
	assert.Equal(c.t, babuza.ErrLearnerCanNotSwitchLeadership, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.TransferLeader(ctx, 4)
	}))

	// Create some data to replicate
	for i := 65; i < 128; i++ {
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.Set(ctx, s, s)
			return err
		}))
	}

	// Join a new voting member
	follower := makeSingleStandardPeer(5, false)
	connectGroup.Add(follower.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, follower, connectGroup.GetIds()))

	// Verify the new voting member joined successfully
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId2, follower)
	}))

	// Transfer leadership to the new voting member
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.TransferLeader(ctx, 5)
	}))

	// Verify the new voting member is now the leader
	leaderId3, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, uint64(5), leaderId3)

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

}
