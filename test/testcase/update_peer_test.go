package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

type UpdatePeer struct {
	t *testing.T
}

func (c *UpdatePeer) Log(s string) {
	c.t.Log(s)
}

func (c *UpdatePeer) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *UpdatePeer) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	updatePeerId := (leaderId % 3) + 1
	updateRaftPeer := makeSinglePeer(updatePeerId, false)

	updateRaftPeer.RaftListenAddr = fmt.Sprintf("127.0.0.1:%d", 10000+updateRaftPeer.Id)
	updateRaftPeer.ProxyListenAddr = fmt.Sprintf("127.0.0.1:%d", 14200+updateRaftPeer.Id)
	updateRaftPeer.AppServiceAddresses = []string{fmt.Sprintf("127.0.0.1:%d", 24200+updateRaftPeer.Id)}

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		peerAddr := tc.GetPeerListenAddress(updateRaftPeer)
		return kvClient.Update(ctx, updateRaftPeer.Id, peerAddr)
	}))

	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, tc, kvClient, updateRaftPeer)
	}))

	assert.Nil(c.t, tc.ShutdownPeer(updateRaftPeer.Id))

	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "foo", "bar")
		return err
	}))

	assert.Nil(c.t, tc.RestartPeer(wait, updateRaftPeer, connectGroup.GetIds()))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderId, leaderId2)

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}
