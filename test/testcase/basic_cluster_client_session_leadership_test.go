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
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

// Component factory for client session leadership continuity testing
func leadershipSessionTestComponents() []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to test session validity across leadership changes
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
			caseName:    "memory store with expired session",
			sessionType: builder.ExpireSession,
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
			caseName:    "memory store with expired session and concurrent snapshot",
			sessionType: builder.ExpireSession,
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
		{
			caseName:    "disk store with expired session",
			sessionType: builder.ExpireSession,
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
		},
	}

	// Create a BabuzaComponent for each test case
	for _, tc := range testCases {
		for _, transport := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
			components = append(components, BabuzaComponent{
				CaseName:           "BasicTest: 3nodes-" + transport + "-BabuzaWal-DurableSnapshot-" + tc.caseName,
				ClusterId:          1,
				CreateStateMachine: tc.stateMachineCreator,
				CreateCustomComponent: func(sessionType, transport string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						b := customBabuzaComponent(sessionType, builder.BabuzaWal, builder.DurableSnapshot,
							transport, proxyNet).
							SetClusterId(config.BubuzaConfig.ClusterID).
							SetStorageRootDir(storageDir)
						return *config, *b.Build()
					}
				}(tc.sessionType, transport),
				ProxyNetwork: nil,
			})
		}
	}

	return components
}

type BasicValidClientSessionLeaderShutdown struct {
	t *testing.T
}

func (c *BasicValidClientSessionLeaderShutdown) Log(s string) {
	c.t.Log(s)
}

func (c *BasicValidClientSessionLeaderShutdown) CreateTestComponents() []BabuzaComponent {
	return leadershipSessionTestComponents()
}

func (c *BasicValidClientSessionLeaderShutdown) Run(tc *testcluster.BabuzaCluster, a any) {
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

	// Create channels for coordination
	stopLeaderCh := make(chan struct{})
	doneCh := make(chan struct{})
	// Total operations to perform
	count := uint64(512)

	// Start a goroutine to execute operations while leader changes
	go func(kvCounts uint64) {
		for i := uint64(0); i < kvCounts; i++ {
			err = runWithCtxTimeout(wait, func(ctx context.Context) error {
				v := fmt.Sprintf("%d", i)
				_, cErr := kvClient.Append(ctx, v, v)
				return cErr
			})
			if err != nil {
				// do not care about the error
			} else {
				if i == kvCounts/2 {
					// Signal to shut down the leader halfway through
					stopLeaderCh <- struct{}{}
				}
			}
		}
		doneCh <- struct{}{}
	}(count)

	// Wait for the signal to stop the leader
	<-stopLeaderCh
	// Shutdown the current leader
	assert.Nil(c.t, tc.ShutdownPeer(leaderID))
	// Wait for election to complete
	time.Sleep(wait)
	// Remove the old leader from the connection group
	connectGroup.Remove(leaderID)

	// Check that a new leader is elected
	leaderID1, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leaderID, leaderID1)
	// Wait for all operations to complete
	<-doneCh
	// Verify all values were correctly stored
	for i := 0; i < int(count); i++ {
		v := strconv.Itoa(i)
		err = runWithCtxTimeout(wait, func(ctx context.Context) error {
			res, err := kvClient.Get(ctx, v)
			if err != nil {
				return err
			}
			assert.Equal(c.t, res.Key, v)
			assert.Equal(c.t, res.Value, v)
			return nil
		})
		assert.Nil(c.t, err)
	}
}

func TestValidClientSessionLeaderShutdown(t *testing.T) {
	RunTests(&BasicValidClientSessionLeaderShutdown{t: t})
}
