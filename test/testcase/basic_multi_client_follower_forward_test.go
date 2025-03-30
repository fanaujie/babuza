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

func (c *BasicMultiClientFollowerForwardProposal) Run(tc *testcluster.BabuzaCluster) {
    wait := tc.RaftElectionTimeout() * 3
    totalPeers := 3
    peers, connectGroup := makeVotingStandardPeers(totalPeers)
    assert.Nil(c.t, tc.MakeCluster(wait, peers))

    // Identify the current leader
    leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
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
            peerId := leaderId
            for index := 0; index < 256; index++ {
                // Make sure we're sending to follower nodes, not leader
                if peerId == leaderId {
                    peerId = (leaderId % uint64(totalPeers)) + 1
                }
                assert.NotEqual(c.t, leaderId, peerId)

                // Send a proposal to a follower node
                assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
                    _, err = kvClient.DirectKvStore(ctx, peerId, client.Set,
                        fmt.Sprintf("%d-%d", clientId, index), fmt.Sprintf("%d", index))
                    return err
                }))

                // Move to next node for the next operation
                if peerId == uint64(totalPeers) {
                    peerId = 1
                } else {
                    peerId++
                }
            }
        }(i)
    }

    // Wait for all clients to finish
    wg.Wait()

    // Verify that all nodes have consistent state
    assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestMultiClientFollowerForwardProposal(t *testing.T) {
    RunTests(&BasicMultiClientFollowerForwardProposal{t: t})
}
