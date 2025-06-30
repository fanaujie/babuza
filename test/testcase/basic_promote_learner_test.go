package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/pkg/cluster"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

type BasicPromoteLearner struct {
	t *testing.T
}

func (c *BasicPromoteLearner) Log(s string) {
	c.t.Log(s)
}

func (c *BasicPromoteLearner) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicPromoteLearner) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
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

	learner := makeSingleStandardPeer(4, true)
	connectGroup.Add(learner.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, learner, connectGroup.GetIDs()))

	// Verify the learner exists and is recognized in the cluster
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, learner)
	}))

	// Wait for replication to complete
	time.Sleep(time.Second)
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))

	// Promote the learner to a voting member
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.PromoteLearner(ctx, learner.ID())
	}))

	// Verify the peer is now a voting member (not a learner anymore)
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, makeSingleStandardPeer(4, false))
	}))

	// Test failure cases

	// Try to promote a non-existent learner
	assert.Equal(c.t, cluster.ErrPeerIDNotFound, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.PromoteLearner(ctx, 100)
	}))

	// Try to promote a peer that's already a voting member
	assert.Equal(c.t, babuza.ErrVotingMemberCanNotPromote, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.PromoteLearner(ctx, leaderID)
	}))
}

func TestPromoteLearner(t *testing.T) {
	RunTests(&BasicPromoteLearner{t: t})
}
