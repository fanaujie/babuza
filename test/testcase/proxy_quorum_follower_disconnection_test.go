package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

type QuorumFollowerDisconnectionCluster struct {
	t *testing.T
}

func (c *QuorumFollowerDisconnectionCluster) Log(s string) {
	c.t.Log(s)
}

func (c *QuorumFollowerDisconnectionCluster) CreateTestComponents() []BabuzaComponent {
	var r []BabuzaComponent
	r = append(r, proxyClusterComponents(true, true)...)
	r = append(r, proxyClusterComponents(false, false)...)
	return r
}

func (c *QuorumFollowerDisconnectionCluster) Run(tc *testcluster.BabuzaCluster, a any) {

	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3

	// Create 5 voting proxy peers
	peers, connectGroup := makeVotingProxyPeers(5)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Check initial leader election
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Disconnect 3 followers to break quorum (more than half of the cluster)
	follower1Id := (leaderID % 5) + 1
	follower2Id := (follower1Id % 5) + 1
	follower3Id := (follower2Id % 5) + 1

	assert.Nil(c.t, tc.DisconnectPeer(follower1Id))
	assert.Nil(c.t, tc.DisconnectPeer(follower2Id))
	assert.Nil(c.t, tc.DisconnectPeer(follower3Id))

	connectGroup.Remove(follower1Id)
	connectGroup.Remove(follower2Id)
	connectGroup.Remove(follower3Id)

	// Create a client to the cluster
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Attempt to execute a command when quorum is lost - should fail
	assert.Error(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "foo", "foo")
		return err
	}))

	// Reconnect the followers to restore quorum
	assert.Nil(c.t, tc.ConnectPeer(follower1Id))
	assert.Nil(c.t, tc.ConnectPeer(follower2Id))
	assert.Nil(c.t, tc.ConnectPeer(follower3Id))

	connectGroup.Add(follower1Id)
	connectGroup.Add(follower2Id)
	connectGroup.Add(follower3Id)

	// Check leader election after reconnection
	_, err = tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Execute commands after restoring quorum
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "bar", "bar")
		return err
	}))

	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "foobar", "bar")
		return err
	}))

	// Verify data consistency across all peers
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestQuorumFollowerDisconnectionCluster(t *testing.T) {
	RunTests(&QuorumFollowerDisconnectionCluster{
		t: t,
	})
}
