package testcase

import (
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type ReElectionCluster struct {
	t *testing.T
}

func (c *ReElectionCluster) Log(s string) {
	c.t.Log(s)
}

func (c *ReElectionCluster) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents(true, true)
}

func (c *ReElectionCluster) Run(tc *testcluster.BabuzaCluster, a any) {
	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3
	// Create 3 voting proxy peers
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Check for initial leader election
	leader1, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Disconnect the first leader to trigger re-election
	assert.Nil(c.t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader1)
	time.Sleep(wait)

	// Verify a new leader is elected among remaining peers
	leader2, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Reconnect the first leader and verify it doesn't become leader immediately
	assert.Nil(c.t, tc.ConnectPeer(leader1))
	connectGroup.Add(leader1)
	assert.NotEqual(c.t, leader2, leader1)

	// Disconnect both previous leaders to force another election
	assert.Nil(c.t, tc.DisconnectPeer(leader2))
	assert.Nil(c.t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader1)
	connectGroup.Remove(leader2)

	// Verify no leader exists in the remaining single node (can't reach quorum)
	assert.Nil(c.t, tc.CheckNoLeader(wait, connectGroup.GetIDs()))

	// Reconnect leader2, allowing a new election to occur
	assert.Nil(c.t, tc.ConnectPeer(leader2))
	connectGroup.Add(leader2)
	time.Sleep(wait)

	// Verify a third leader is elected
	leader3, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Reconnect all nodes and verify leader stability
	assert.Nil(c.t, tc.ConnectPeer(leader1))
	connectGroup.Add(leader1)

	// Verify the leader doesn't change when all nodes are reconnected
	lastLeader, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leader3, lastLeader)
}

func TestReElectionCluster(t *testing.T) {
	RunTests(&ReElectionCluster{t: t})
}
