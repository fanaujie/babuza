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
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Component factory for session expiration testing
func sessionLruExpirationTestComponents(maxSession int64) []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to test session expiration responses
	testCases := []struct {
		caseName            string
		sessionType         string
		stateMachineCreator func(string) ibabuza.BaseStateMachine
	}{
		{
			caseName:    "memory store with lru session",
			sessionType: builder.LRUSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
		},
		{
			caseName:    "memory store with session and concurrent snapshot",
			sessionType: builder.LRUSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
		},
		{
			caseName:    "disk store with lru session",
			sessionType: builder.LRUSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
		},
	}

	// Create a BabuzaComponent for each test case
	for _, tc := range testCases {
		components = append(components, BabuzaComponent{
			CaseName:           "SessionExpirationTest: " + tc.caseName,
			ClusterId:          1,
			CreateStateMachine: tc.stateMachineCreator,
			CreateCustomComponent: func(sessionType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
				return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					b := customBabuzaComponent(sessionType, builder.BabuzaWal, builder.DurableSnapshot,
						builder.TcpTransport, proxyNet).
						SetClusterId(config.BubuzaConfig.ClusterID).
						SetStorageRootDir(storageDir).
						AddLruSessionOptions(session.SetLruMgrOptionsWithMaxSessions(maxSession))

					return *config, *b.Build()
				}
			}(tc.sessionType),
			ProxyNetwork: nil,
		})
	}

	return components
}

type BasicClientSessionLruExpiredResponse struct {
	t          *testing.T
	maxSession int64
}

func (c *BasicClientSessionLruExpiredResponse) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientSessionLruExpiredResponse) CreateTestComponents() []BabuzaComponent {
	return sessionLruExpirationTestComponents(c.maxSession)
}

func (c *BasicClientSessionLruExpiredResponse) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	var tempClients []*client.KvStoreClient
	for i := int64(0); i < c.maxSession+1; i++ {
		tempClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
		assert.Nil(c.t, err)

		err = runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err := tempClient.Set(ctx, "temp-key", "temp-value")
			return err
		})
		assert.Nil(c.t, err)
		tempClients = append(tempClients, tempClient)
	}
	defer func() {
		for _, cl := range tempClients {
			_ = cl.Close()
		}
	}()
	// first client should be expired
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = tempClients[0].Set(ctx, "key-after-expiration", "value")
		return err
	})
	assert.Error(c.t, err)

	// unregister the first client session,
	// it should be expired
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tempClients[0].UnregisterSession(ctx)
	})
	assert.Error(c.t, err)

	// the second client unregister session should be ok
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		return tempClients[1].UnregisterSession(ctx)
	})
	assert.Nil(c.t, err)
}

func TestClientSessionLruExpiredResponse(t *testing.T) {
	RunTests(&BasicClientSessionLruExpiredResponse{
		t:          t,
		maxSession: 2})
}
