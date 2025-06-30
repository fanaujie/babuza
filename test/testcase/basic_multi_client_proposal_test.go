package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

type BasicMultiClientProposal struct {
	t *testing.T
}

func (c *BasicMultiClientProposal) Log(s string) {
	c.t.Log(s)
}

func (c *BasicMultiClientProposal) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicMultiClientProposal) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the current leader
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Create multiple clients and have them concurrently send proposals
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

			// Each client performs 256 Set operations
			for j := 0; j < 256; j++ {
				key := strconv.Itoa(clientId)
				value := strconv.Itoa(j)

				assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
					_, err := kvClient.Set(ctx, key, value)
					return err
				}))
			}
		}(i)
	}

	// Wait for all clients to finish
	wg.Wait()

	// Verify that all nodes have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestMultiClientProposal(t *testing.T) {
	RunTests(&BasicMultiClientProposal{t: t})
}
