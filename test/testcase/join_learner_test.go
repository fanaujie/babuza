package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type JoinLearner struct {
	t *testing.T
}

func (c *JoinLearner) Log(s string) {
	c.t.Log(s)
}

func (c *JoinLearner) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *JoinLearner) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	learner := makeSinglePeer(4, true)
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	connectGroup.Add(learner.Id)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, learner, connectGroup.GetIds()))

	assert.Nil(c.t, runWithCtxTimeout(time.Second*3, func(ctx context.Context) error {
		return peerConfigExists(ctx, tc.UseProxyNetwork(), kvClient, learner)
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderId, leaderId2)
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}
