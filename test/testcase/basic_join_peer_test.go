package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type JoinPeer struct {
	t *testing.T
}

func (c *JoinPeer) Log(s string) {
	c.t.Log(s)
}

func (c *JoinPeer) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *JoinPeer) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	joinPeer := makeSinglePeer(4, false)
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()
	connectGroup.Add(joinPeer.Id)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))

	assert.Nil(c.t, runWithCtxTimeout(time.Second*3, func(ctx context.Context) error {
		return peerConfigExists(ctx, tc, kvClient, joinPeer)
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderId, leaderId2)
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

	// failure
	joinPeer.ProxyListenAddr = "127.0.0.1:100"
	assert.Equal(c.t, cluster.ErrPeerIDExists, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))
	leader := makeSinglePeer(leaderId, false)
	joinPeer = makeSinglePeer(5, false)

	joinPeer.ProxyListenAddr = leader.ProxyListenAddr
	joinPeer.RaftListenAddr = leader.RaftListenAddr
	
	assert.Equal(c.t, cluster.ErrPeerRaftListenAddrExists, tc.JoinPeerToCluster(wait, kvClient, joinPeer, connectGroup.GetIds()))
}
