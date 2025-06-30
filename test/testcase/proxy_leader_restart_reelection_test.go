package testcase

import (
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type LeaderRestartReElectionCluster struct {
	t *testing.T
}

func (c *LeaderRestartReElectionCluster) Log(s string) {
	c.t.Log(s)
}

func (c *LeaderRestartReElectionCluster) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents(true, true)
}

func (c *LeaderRestartReElectionCluster) Run(tc *testcluster.BabuzaCluster, a any) {
	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3
	// Create 3 voting proxy peers
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Check initial leader election
	leader1, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Shutdown leader 1
	assert.Nil(c.t, tc.ShutdownPeer(leader1))
	time.Sleep(wait) // Wait for election

	// Restart leader 1
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleProxyPeer(leader1, false), connectGroup.GetIDs()))
	leader2, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leader2, leader1)

	// Shutdown leader 2 and disconnect leader 1
	assert.Nil(c.t, tc.ShutdownPeer(leader2))
	assert.Nil(c.t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader2)
	connectGroup.Remove(leader1)

	// Verify no leader exists in the cluster (not enough nodes for quorum)
	assert.Nil(c.t, tc.CheckNoLeader(wait, connectGroup.GetIDs()))

	// Reconnect leader 1 and restart leader 2
	connectGroup.Add(leader2)
	connectGroup.Add(leader1)
	assert.Nil(c.t, tc.ConnectPeer(leader1))
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleProxyPeer(leader2, false), connectGroup.GetIDs()))

	// Verify a new leader has been elected
	_, err = tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
}

func TestLeaderRestartReElectionCluster(t *testing.T) {
	RunTests(&LeaderRestartReElectionCluster{t: t})
}
