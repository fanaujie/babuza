package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

type BasicJoinPeer struct {
	t *testing.T
}

func (c *BasicJoinPeer) Log(s string) {
	c.t.Log(s)
}

func (c *BasicJoinPeer) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicJoinPeer) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	joinPeer := makeSingleStandardPeer(4, false)
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
	connectGroup.Add(joinPeer.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))

	assert.Nil(c.t, runWithCtxTimeout(time.Second*3, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId, joinPeer)
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderId, leaderId2)
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

	// failure
	joinPeer.SetRaftListenAddress("127.0.0.1:34200")
	assert.Equal(c.t, cluster.ErrPeerIDExists, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))
	leader := makeSingleStandardPeer(leaderId, false)
	joinPeer = makeSingleStandardPeer(5, false)
	joinPeer.SetRaftListenAddress(leader.RaftListenAddress(false))
	assert.Equal(c.t, cluster.ErrPeerRaftListenAddrExists, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))
}
