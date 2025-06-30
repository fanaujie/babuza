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
	"crypto/rand"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type BasicSendSnapshotToFollower struct {
	t             *testing.T
	snapshotCount uint64
}

func (c *BasicSendSnapshotToFollower) Log(s string) {
	c.t.Log(s)
}

func (c *BasicSendSnapshotToFollower) CreateTestComponents() []BabuzaComponent {
	return basicSnapshotTestComponents(c.snapshotCount)
}

func (c *BasicSendSnapshotToFollower) Run(tc *testcluster.BabuzaCluster, testParams any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Create a client with automatic incrementing session
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Write enough data to trigger snapshot creation
	// We write (snapshotCount + 10) entries to ensure snapshot is created
	data := make([]byte, 10*1024) // 10KB
	_, err = rand.Read(data)
	assert.Nil(c.t, err)
	sData := string(data)
	//test exceeding snapshot size of 5MB
	// 10KB * (500 + 10) > 5MB
	for i := uint64(0); i < c.snapshotCount+10; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)

			res, cErr := kvClient.Set(ctx, v, sData)
			assert.Equal(c.t, v, res.Key)
			//assert.Equal(c.t, sData, res.Value)
			return cErr
		}))
	}

	// Verify all peers have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))

	// Record snapshot metadata from leader for later verification
	lastSnapshotIndex := uint64(0)
	lastSnapshotTerm := uint64(0)
	assert.Nil(c.t, tc.CheckStatus(wait, leaderID, func(s babuza.Status) bool {
		lastSnapshotIndex = s.LastSnapshotIndex
		lastSnapshotTerm = s.LastSnapshotTerm
		return s.LastSnapshotIndex >= c.snapshotCount &&
			s.LastSnapshotTerm > 0
	}))

	newFollowers := make([]testcluster.Peer, 0)
	for i := 0; i < 3; i++ {
		// Add a new follower to the cluster that will receive a snapshot
		newFollowerId := uint64(4 + i)
		newFollower := makeSingleStandardPeer(newFollowerId, false)
		newFollowers = append(newFollowers, newFollower)
		connectGroup.Add(newFollowerId)
		// Join the new node to the cluster - it should receive a snapshot
		assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, newFollower, connectGroup.GetIDs()))
	}

	for _, newFollower := range newFollowers {
		// Check if the new follower properly joined
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			return tc.CheckPeerExists(ctx, leaderID, newFollower)
		}))
	}

	// Wait a bit for snapshot transfer to complete
	time.Sleep(time.Second)

	for _, newFollower := range newFollowers {
		// Verify the snapshot was transferred properly to the new follower
		assert.Nil(c.t, tc.CheckStatus(wait, newFollower.ID(), func(s babuza.Status) bool {
			return s.LastSnapshotIndex == lastSnapshotIndex &&
				s.LastSnapshotTerm == lastSnapshotTerm
		}))
	}
	// Write additional data with the new follower in the cluster
	for i := uint64(100); i < 108; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return cErr
		}))
	}

	// Final verification that all nodes (including the new follower) have identical state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestSendSnapshotToFollower(t *testing.T) {
	RunTests(&BasicSendSnapshotToFollower{
		t:             t,
		snapshotCount: 500,
	})
}
