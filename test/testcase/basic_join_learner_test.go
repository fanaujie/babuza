package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

type BasicJoinLearner struct {
	t *testing.T
}

func (c *BasicJoinLearner) Log(s string) {
	c.t.Log(s)
}

func (c *BasicJoinLearner) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicJoinLearner) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	learner := makeSingleStandardPeer(4, true)
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
	connectGroup.Add(learner.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, learner, connectGroup.GetIDs()))

	assert.Nil(c.t, runWithCtxTimeout(time.Second*3, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, learner)
	}))

	leaderID2, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderID, leaderID2)
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestJoinLearner(t *testing.T) {
	RunTests(&BasicJoinLearner{t: t})
}
