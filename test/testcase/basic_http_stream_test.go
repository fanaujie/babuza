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
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
)

const basicHTTPStreamSnapshotCount = 8

type BasicHTTPStreamSnapshot struct {
	t *testing.T
}

func (c *BasicHTTPStreamSnapshot) Log(s string) {
	c.t.Log(s)
}

func (c *BasicHTTPStreamSnapshot) CreateTestComponents() []BabuzaComponent {
	return []BabuzaComponent{
		{
			CaseName:  "BasicHTTPStreamSnapshot: 3nodes-http-memory-BabuzaWal-DurableSnapshot-NoOpSession",
			ClusterId: 1,
			CreateStateMachine: func(string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStore()
			},
			CreateCustomComponent: func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
				config.BubuzaConfig.RaftConfig.DisableProposalForwarding = true
				config.BubuzaConfig.SnapshotCount = basicHTTPStreamSnapshotCount
				b := customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
					builder.HttpTransport, proxyNet).
					SetClusterId(config.BubuzaConfig.ClusterID).
					SetStorageRootDir(storageDir).
					AddHttpOptions(protocol.SetHttpOptsWithMessageStreamEnabled(true))
				return *config, *b.Build()
			},
			ProxyNetwork: nil,
		},
	}
}

func (c *BasicHTTPStreamSnapshot) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	for i := uint64(0); i < basicHTTPStreamSnapshotCount+10; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return cErr
		}))
	}

	lastSnapshotIndex := uint64(0)
	lastSnapshotTerm := uint64(0)
	assert.Nil(c.t, tc.CheckStatus(wait, leaderID, func(s babuza.Status) bool {
		lastSnapshotIndex = s.LastSnapshotIndex
		lastSnapshotTerm = s.LastSnapshotTerm
		return s.LastSnapshotIndex >= basicHTTPStreamSnapshotCount && s.LastSnapshotTerm > 0
	}))

	newFollower := makeSingleStandardPeer(4, false)
	connectGroup.Add(newFollower.ID())
	assert.Nil(c.t, tc.JoinPeerToCluster(wait, kvClient, newFollower, connectGroup.GetIDs()))
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tc.CheckPeerExists(ctx, leaderID, newFollower)
	}))
	assert.Nil(c.t, tc.CheckStatus(wait, newFollower.ID(), func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex && s.LastSnapshotTerm == lastSnapshotTerm
	}))
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestBasicHTTPStreamSnapshot(t *testing.T) {
	RunTests(&BasicHTTPStreamSnapshot{t: t})
}
