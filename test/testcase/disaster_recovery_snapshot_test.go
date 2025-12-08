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
	"fmt"
	"testing"

	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
)

type DisasterRecoverySnapshotCase struct {
	t             *testing.T
	snapshotCount uint64
}

func (c *DisasterRecoverySnapshotCase) Log(s string) {
	c.t.Log(s)
}

func (c *DisasterRecoverySnapshotCase) CreateTestComponents() []BabuzaComponent {
	return basicSnapshotTestComponents(c.snapshotCount)
}

func (c *DisasterRecoverySnapshotCase) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 10
	clusterNodeSize := 3

	// Step 1: Create initial 3-node cluster with snapshot configuration
	peers, connectGroup := makeVotingStandardPeers(clusterNodeSize)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the initial leader
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	c.t.Logf("Initial leader: %d", leaderID)

	// Step 2: Create a client and write data to trigger snapshot creation
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Write enough data to trigger snapshot (snapshotCount + 10 entries)
	c.t.Logf("Writing %d entries to trigger snapshot creation", c.snapshotCount+10)
	for i := uint64(0); i < c.snapshotCount+10; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			if cErr == nil {
				assert.Equal(c.t, v, res.Key)
				assert.Equal(c.t, v, res.Value)
			}
			return cErr
		}))
	}

	// Verify all peers have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))

	// Step 3: Verify snapshot metadata from leader
	lastSnapshotIndex := uint64(0)
	lastSnapshotTerm := uint64(0)
	assert.Nil(c.t, tc.CheckStatus(wait, leaderID, func(s babuza.Status) bool {
		lastSnapshotIndex = s.LastSnapshotIndex
		lastSnapshotTerm = s.LastSnapshotTerm
		return s.LastSnapshotIndex >= c.snapshotCount &&
			s.LastSnapshotTerm > 0
	}))
	c.t.Logf("Snapshot created - Index: %d, Term: %d", lastSnapshotIndex, lastSnapshotTerm)

	// Step 4: Simulate disaster - shutdown all nodes (cluster loses quorum)
	c.t.Logf("Simulating disaster: shutting down all nodes")
	for _, peerID := range []uint64{1, 2, 3} {
		assert.Nil(c.t, tc.ShutdownPeer(peerID))
	}

	// Step 5: Recover node 1 as standalone
	c.t.Logf("Recovering node 1 as standalone from snapshot")
	peer1 := makeSingleStandardPeer(1, false)
	assert.Nil(c.t, tc.RecoverPeerAsStandalone(wait, peer1))

	// Step 6: Verify recovered node becomes leader of single-node cluster
	newLeaderID, err := tc.CheckOneLeader(wait, []uint64{1})
	assert.Nil(c.t, err)
	assert.Equal(c.t, uint64(1), newLeaderID)

	// Step 7: Verify snapshot metadata is preserved after recovery
	assert.Nil(c.t, tc.CheckStatus(wait, uint64(1), func(s babuza.Status) bool {
		if s.LastSnapshotIndex != lastSnapshotIndex {
			c.t.Logf("Snapshot index mismatch - Expected: %d, Got: %d", lastSnapshotIndex, s.LastSnapshotIndex)
			return false
		}
		if s.LastSnapshotTerm != lastSnapshotTerm {
			c.t.Logf("Snapshot term mismatch - Expected: %d, Got: %d", lastSnapshotTerm, s.LastSnapshotTerm)
			return false
		}
		return true
	}))
	c.t.Logf("Snapshot metadata preserved after recovery - Index: %d, Term: %d", lastSnapshotIndex, lastSnapshotTerm)

	// Step 8: Verify data integrity on recovered standalone node
	standaloneClient, err := embedapp.NewKvStoreClient(tc.GetAppServiceAddresses([]uint64{1}), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = standaloneClient.Close()
	}()

	// Verify all data is still accessible
	c.t.Logf("Verifying data integrity on recovered standalone node")
	for i := uint64(0); i < c.snapshotCount+10; i++ {
		key := fmt.Sprintf("%d", i)
		expectedValue := fmt.Sprintf("%d", i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			resp, getErr := standaloneClient.Get(ctx, key)
			if getErr == nil {
				assert.Equal(c.t, expectedValue, resp.KvResult.Value)
			}
			return getErr
		}))
	}
	c.t.Logf("Data integrity verified: all %d key-value pairs recovered correctly", c.snapshotCount+10)

	// Step 9: Test cluster expansion - add new peers to recovered standalone node
	c.t.Logf("Testing cluster expansion: joining node 4 and 5")

	// Join node 4
	peer4 := makeSingleStandardPeer(4, false)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, standaloneClient, peer4, []uint64{1, 4}))

	// Join node 5
	peer5 := makeSingleStandardPeer(5, false)
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, standaloneClient, peer5, []uint64{1, 4, 5}))

	// Verify 3-node expanded cluster is healthy
	expandedGroup := []uint64{1, 4, 5}
	_, err = tc.CheckOneLeader(wait, expandedGroup)
	assert.Nil(c.t, err)

	// Step 10: Verify final cluster consistency after expansion
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, expandedGroup))

	// Verify all data is still accessible in expanded cluster
	expandedClient, err := embedapp.NewKvStoreClient(tc.GetAppServiceAddresses(expandedGroup), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = expandedClient.Close()
	}()

	for i := uint64(0); i < c.snapshotCount+10; i++ {
		key := fmt.Sprintf("%d", i)
		expectedValue := fmt.Sprintf("%d", i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			resp, getErr := expandedClient.Get(ctx, key)
			if getErr == nil {
				assert.Equal(c.t, expectedValue, resp.KvResult.Value)
			}
			return getErr
		}))
	}

	c.t.Logf("Snapshot-based disaster recovery test completed successfully")
}

func TestDisasterRecoverySnapshot(t *testing.T) {
	RunTests(&DisasterRecoverySnapshotCase{
		t:             t,
		snapshotCount: 50,
	})
}
