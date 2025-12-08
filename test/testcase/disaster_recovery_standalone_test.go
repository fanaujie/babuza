// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcase

import (
	"context"
	"strconv"
	"testing"

	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
)

type DisasterRecoveryStandaloneCase struct {
	t *testing.T
}

func (c *DisasterRecoveryStandaloneCase) Log(s string) {
	c.t.Log(s)
}

func (c *DisasterRecoveryStandaloneCase) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *DisasterRecoveryStandaloneCase) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 10

	// Step 1: Create initial 3-node cluster
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	c.t.Logf("Initial leader: %d", leaderID)

	// Step 2: Propose some data to ensure state machine has data
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	for i := 0; i < 10; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err := kvClient.Set(ctx, key, value)
			return err
		}))
	}

	// Wait for consistency
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))

	// Step 3: Simulate disaster - shutdown all nodes (cluster loses quorum)
	c.t.Logf("Simulating disaster: shutting down all nodes")
	for _, peerID := range []uint64{1, 2, 3} {
		assert.Nil(c.t, tc.ShutdownPeer(peerID))
	}

	// Step 4: Recover node 1 as standalone
	c.t.Logf("Recovering node 1 as standalone")
	peer1 := makeSingleStandardPeer(1, false)
	assert.Nil(c.t, tc.RecoverPeerAsStandalone(wait, peer1))

	// Step 5: Verify recovered node becomes leader of single-node cluster
	newLeaderID, err := tc.CheckOneLeader(wait, []uint64{1})
	assert.Nil(c.t, err)
	assert.Equal(c.t, uint64(1), newLeaderID)

	// Step 6: Verify data integrity
	// Create new client connected to recovered standalone node
	standaloneClient, err := embedapp.NewKvStoreClient(tc.GetAppServiceAddresses([]uint64{1}), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = standaloneClient.Close()
	}()

	// Verify all data is still accessible
	for i := 0; i < 10; i++ {
		key := "key" + strconv.Itoa(i)
		expectedValue := "value" + strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			resp, getErr := standaloneClient.Get(ctx, key)
			if getErr == nil {
				assert.Equal(c.t, expectedValue, resp.KvResult.Value)
			}
			return getErr
		}))
	}

	c.t.Logf("Data integrity verified: all 10 key-value pairs recovered correctly")

	// Step 7: Test cluster expansion - add new peers to recovered standalone node
	c.t.Logf("Testing cluster expansion: joining node 4 and 5")

	// Join node 4
	peer4 := makeSingleStandardPeer(4, false)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, standaloneClient, peer4, []uint64{1, 4}))

	// Join node 5
	peer5 := makeSingleStandardPeer(5, false)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, standaloneClient, peer5, []uint64{1, 4, 5}))

	// Verify 3-node cluster is healthy
	expandedGroup := []uint64{1, 4, 5}
	_, err = tc.CheckOneLeader(wait, expandedGroup)
	assert.Nil(c.t, err)

	// Verify consistency after expansion
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, expandedGroup))

	// Verify all data is still accessible in expanded cluster
	expandedClient, err := embedapp.NewKvStoreClient(tc.GetAppServiceAddresses(expandedGroup), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = expandedClient.Close()
	}()

	for i := 0; i < 10; i++ {
		key := "key" + strconv.Itoa(i)
		expectedValue := "value" + strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			resp, getErr := expandedClient.Get(ctx, key)
			if getErr == nil {
				assert.Equal(c.t, expectedValue, resp.KvResult.Value)
			}
			return getErr
		}))
	}

	c.t.Logf("Disaster recovery test completed successfully")
}

func TestDisasterRecoveryStandalone(t *testing.T) {
	RunTests(&DisasterRecoveryStandaloneCase{t: t})
}
