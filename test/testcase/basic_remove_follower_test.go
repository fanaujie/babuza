package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

type BasicRemoveFollower struct {
	t *testing.T
}

func (c *BasicRemoveFollower) Log(s string) {
	c.t.Log(s)
}

func (c *BasicRemoveFollower) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicRemoveFollower) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	followerId := (leaderId % 3) + 1
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	assert.Nil(c.t, tc.RemovePeerFromCluster(wait, kvClient, followerId))
	connectGroup.Remove(followerId)

	assert.Error(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId, makeSingleStandardPeer(followerId, false))
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderId, leaderId2)

	// Test failure cases

	// Try to join a removed peer
	connectGroup.Add(followerId)
	assert.Equal(c.t, cluster.ErrPeerIDRemoved, tc.JoinPeerToCluster(wait, kvClient, makeSingleStandardPeer(followerId, false), connectGroup.GetIds()))

	// Try to remove a non-existent peer
	assert.Equal(c.t, cluster.ErrPeerIDNotFound, tc.RemovePeerFromCluster(wait, kvClient, 100))
}

func TestRemoveFollower(t *testing.T) {
	RunTests(&BasicRemoveFollower{t: t})
}
