package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type BasicMultiClientFollowerForwardProposal struct {
	t *testing.T
}

func (c *BasicMultiClientFollowerForwardProposal) Log(s string) {
	c.t.Log(s)
}

func (c *BasicMultiClientFollowerForwardProposal) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(false)
}

func (c *BasicMultiClientFollowerForwardProposal) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	totalPeers := 3
	peers, connectGroup := makeVotingStandardPeers(totalPeers)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the current leader
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Launch multiple clients concurrently
	clients := 16
	wg := sync.WaitGroup{}

	// Start the clients
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()

			// Create a client
			kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
			assert.Nil(c.t, err)
			defer func() {
				_ = kvClient.Close()
			}()

			// Cycle through followers for sending proposals
			peerID := leaderID
			for index := 0; index < 256; index++ {
				// Make sure we're sending to follower nodes, not leader
				if peerID == leaderID {
					peerID = (leaderID % uint64(totalPeers)) + 1
				}
				assert.NotEqual(c.t, leaderID, peerID)

				// Send a proposal to a follower node
				assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
					_, err = kvClient.DirectKvStore(ctx, peerID, client.Set,
						fmt.Sprintf("%d-%d", clientId, index), fmt.Sprintf("%d", index))
					return err
				}))

				// Move to next node for the next operation
				if peerID == uint64(totalPeers) {
					peerID = 1
				} else {
					peerID++
				}
			}
		}(i)
	}

	// Wait for all clients to finish
	wg.Wait()

	// Verify that all nodes have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestMultiClientFollowerForwardProposal(t *testing.T) {
	RunTests(&BasicMultiClientFollowerForwardProposal{t: t})
}
