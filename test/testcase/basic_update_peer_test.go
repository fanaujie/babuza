package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type BasicUpdatePeer struct {
	t *testing.T
}

func (c *BasicUpdatePeer) Log(s string) {
	c.t.Log(s)
}

func (c *BasicUpdatePeer) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicUpdatePeer) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	updatePeerId := (leaderId % 3) + 1
	updateRaftPeer := makeSingleStandardPeer(updatePeerId, false)

	updateRaftPeer.SetRaftListenAddress(fmt.Sprintf("127.0.0.1:%d", 10000+updateRaftPeer.ID()))
	updateRaftPeer.SetAppServiceAddresses([]string{fmt.Sprintf("127.0.0.1:%d", 24200+updateRaftPeer.ID())})

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
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient.Update(ctx, updateRaftPeer.ID(), updateRaftPeer.RaftListenAddress(false))
	}))

	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderId, updateRaftPeer)
	}))
	assert.Nil(c.t, tc.ShutdownPeer(updateRaftPeer.ID()))
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

func TestUpdatePeer(t *testing.T) {
	RunTests(&BasicUpdatePeer{t: t})
}
