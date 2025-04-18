package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type BasicFollowerForwardProposal struct {
	t *testing.T
}

func (c *BasicFollowerForwardProposal) Log(s string) {
	c.t.Log(s)
}

func (c *BasicFollowerForwardProposal) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(false)
}

func (c *BasicFollowerForwardProposal) Run(tc *testcluster.BabuzaCluster, a any) {
	totalPeers := 3
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(totalPeers)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the current leader
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Choose follower nodes to send proposals to
	peerID := leaderID
	for i := 0; i < 64; i++ {
		// Cycle through follower nodes (ensuring we're not sending to the leader)
		if peerID == leaderID {
			peerID = (leaderID % uint64(totalPeers)) + 1
		}
		assert.NotEqual(c.t, leaderID, peerID)

		// Send the proposal to a follower (which should forward it to the leader)
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.DirectKvStore(ctx, peerID, client.Set, s, s)
			return err
		}))

		// Cycle to the next node for the next operation
		if peerID == uint64(totalPeers) {
			peerID = 1
		} else {
			peerID++
		}
	}

	// Verify that all nodes have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestFollowerForwardProposal(t *testing.T) {
	RunTests(&BasicFollowerForwardProposal{t: t})
}
