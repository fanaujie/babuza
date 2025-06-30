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
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

type OneFollowerDisconnectionCluster struct {
	t *testing.T
}

func (c *OneFollowerDisconnectionCluster) Log(s string) {
	c.t.Log(s)
}

func (c *OneFollowerDisconnectionCluster) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents(true, true)
}

func (c *OneFollowerDisconnectionCluster) Run(tc *testcluster.BabuzaCluster, a any) {
	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3

	// Create 3 voting proxy peers
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Check initial leader election
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Disconnect a follower
	followerId := (leaderID % 3) + 1
	assert.Nil(c.t, tc.DisconnectPeer(followerId))
	connectGroup.Remove(followerId)

	// Create a client to the cluster
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Execute commands while follower is disconnected
	for i := 0; i < 8; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			key := fmt.Sprintf("foo-%d", i)
			_, err := kvClient.Set(ctx, key, "foo")
			return err
		}))
	}

	// Reconnect the follower
	assert.Nil(c.t, tc.ConnectPeer(followerId))
	connectGroup.Add(followerId)

	// Verify leader is still the same
	lastLeaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaderID, lastLeaderId)

	// Execute more commands after follower reconnection
	for i := 8; i < 16; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			key := fmt.Sprintf("foo-%d", i)
			_, err = kvClient.Set(ctx, key, "foo")
			return err
		}))
	}

	// Verify data consistency across all peers
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestOneFollowerDisconnectionCluster(t *testing.T) {
	RunTests(&OneFollowerDisconnectionCluster{t: t})
}
