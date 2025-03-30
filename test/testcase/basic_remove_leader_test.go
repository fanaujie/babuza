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

func (c *BasicRemoveLeader) Run(tc *testcluster.BabuzaCluster) {
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

	assert.Nil(c.t, tc.RemovePeerFromCluster(wait, kvClient, leaderId))
	connectGroup.Remove(leaderId)

	assert.Error(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId, makeSingleStandardPeer(leaderId, false))
	}))

	time.Sleep(tc.RaftElectionTimeout())

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leaderId, leaderId2)
}

func TestRemoveLeader(t *testing.T) {
	RunTests(&BasicRemoveLeader{t: t})
}
