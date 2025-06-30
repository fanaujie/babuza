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

type BasicRemoveLeader struct {
	t *testing.T
}

func (c *BasicRemoveLeader) Log(s string) {
	c.t.Log(s)
}

func (c *BasicRemoveLeader) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicRemoveLeader) Run(tc *testcluster.BabuzaCluster, a any) {
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

	assert.Nil(c.t, tc.RemovePeerFromCluster(wait, kvClient, leaderID))
	connectGroup.Remove(leaderID)

	assert.Error(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, makeSingleStandardPeer(leaderID, false))
	}))

	time.Sleep(tc.RaftElectionTimeout())

	leaderID2, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leaderID, leaderID2)
}

func TestRemoveLeader(t *testing.T) {
	RunTests(&BasicRemoveLeader{t: t})
}
