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

func (c *BasicFollowerForwardProposal) Run(tc *testcluster.BabuzaCluster) {
	totalPeers := 3
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(totalPeers)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the current leader
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Choose follower nodes to send proposals to
	peerId := leaderId
	for i := 0; i < 64; i++ {
		// Cycle through follower nodes (ensuring we're not sending to the leader)
		if peerId == leaderId {
			peerId = (leaderId % uint64(totalPeers)) + 1
		}
		assert.NotEqual(c.t, leaderId, peerId)

		// Send the proposal to a follower (which should forward it to the leader)
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.DirectKvStore(ctx, peerId, client.Set, s, s)
			return err
		}))

		// Cycle to the next node for the next operation
		if peerId == uint64(totalPeers) {
			peerId = 1
		} else {
			peerId++
		}
	}

	// Verify that all nodes have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestFollowerForwardProposal(t *testing.T) {
	RunTests(&BasicFollowerForwardProposal{t: t})
}
