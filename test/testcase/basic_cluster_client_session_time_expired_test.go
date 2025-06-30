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
	"time"
)

// Component factory for session expiration testing
func sessionTimeExpirationTestComponents() []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to test session expiration responses
	testCases := []struct {
		caseName            string
		sessionType         string
		stateMachineCreator func(string) ibabuza.BaseStateMachine
	}{
		{
			caseName:    "memory store with time expired session",
			sessionType: builder.ExpireSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
		},
		{
			caseName:    "memory store with time expired session and concurrent snapshot",
			sessionType: builder.ExpireSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
		},
		{
			caseName:    "disk store with time expired session",
			sessionType: builder.ExpireSession,
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
						AddExpireSessionOptions(session.SetExpiredMgrOptionsWithExpiredTime(time.Second * 2))
					return *config, *b.Build()
				}
			}(tc.sessionType),
			ProxyNetwork: nil,
		})
	}

	return components
}

type BasicClientSessionTimeExpiredResponse struct {
	t *testing.T
}

func (c *BasicClientSessionTimeExpiredResponse) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientSessionTimeExpiredResponse) CreateTestComponents() []BabuzaComponent {
	return sessionTimeExpirationTestComponents()
}

func (c *BasicClientSessionTimeExpiredResponse) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	mSession := client.NewManualIncrementSession()
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), mSession)
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()
	mSession.SetSequenceNumber(1)
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "test-key1", "test-value1")
		return err
	})
	assert.Nil(c.t, err)
	mSession.SetSequenceNumber(2)
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "test-key2", "test-value2")
		return err
	})
	assert.Nil(c.t, err)
	_ = runWithCtxTimeout(wait, func(ctx context.Context) error {
		v1, err := kvClient.Get(ctx, "test-key1")
		v2, err := kvClient.Get(ctx, "test-key2")
		assert.Nil(c.t, err)
		assert.Equal(c.t, "test-value1", v1.KvResult.Value)
		assert.Equal(c.t, "test-value2", v2.KvResult.Value)
		return nil
	})

	// Wait for the session to expire
	time.Sleep(2 * time.Second)

	mSession.SetSequenceNumber(3)
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "key-after-expiration", "value")
		return err
	})
	// Expecting an error due to session expiration
	assert.Error(c.t, err)

	kvClient2, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient2.Close()
	}()
	// unregister the kvClient
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		return kvClient2.UnregisterSession(ctx)
	})
	assert.Nil(c.t, err)
}

func TestClientSessionExpiredResponse(t *testing.T) {
	RunTests(&BasicClientSessionTimeExpiredResponse{t: t})
}
