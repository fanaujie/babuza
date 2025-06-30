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
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type BasicRemoveLeader struct {
	t *testing.T
}

func (c *BasicRemoveLeader) Log(s string) {
	c.t.Log(s)
}

func (c *BasicRemoveLeader) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicRemoveLeader) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	assert.Nil(c.t, tc.RemovePeerFromCluster(wait, kvClient, leaderID))
	connectGroup.Remove(leaderID)

	assert.Error(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, makeSingleStandardPeer(leaderID, false))
	}))

	time.Sleep(tc.RaftElectionTimeout())

	leaderID2, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leaderID, leaderID2)
}

func TestRemoveLeader(t *testing.T) {
	RunTests(&BasicRemoveLeader{t: t})
}
