package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Component factory for idempotency testing with different state machines and session managers
func idempotencyTestComponents() []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to match the original test
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
				CreateCustomComponent: func(sessionType, transport string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
						b := customBabuzaComponent(sessionType, builder.BabuzaWal, builder.DurableSnapshot,
							transport, proxyNet).
							SetClusterId(config.ClusterId).
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

type BasicClientRequestIdempotency struct {
	t *testing.T
}

func (c *BasicClientRequestIdempotency) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientRequestIdempotency) CreateTestComponents() []BabuzaComponent {
	return idempotencyTestComponents()
}

func (c *BasicClientRequestIdempotency) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	// Create a client with manual session control so we can test idempotency
	ms := client.NewManualIncrementSession()
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), ms)
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Set sequence number for first operation
	ms.SetSequenceNumber(1)

	// Perform first operation (Set)
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		res, err := kvClient.Set(ctx, "foo", "bar")
		if err == nil {
			assert.Equal(c.t, "foo", res.Key)
			assert.Equal(c.t, "bar", res.Value)
		}
		return err
	})
	assert.Nil(c.t, err)

	// Set sequence number for second operation
	ms.SetSequenceNumber(2)

	// Perform second operation (Append)
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		res, err := kvClient.Append(ctx, "foo", "bar")
		if err == nil {
			assert.Equal(c.t, "foo", res.Key)
			assert.Equal(c.t, "barbar", res.Value)
		}
		return err
	})
	assert.Nil(c.t, err)

	// Repeat the second operation multiple times with the same sequence number
	// This tests idempotency - the operation should only be applied once
	for i := 0; i < 5; i++ {
		err = runWithCtxTimeout(wait, func(ctx context.Context) error {
			res, err := kvClient.Append(ctx, "foo", "bar")
			if err == nil {
				assert.Equal(c.t, "foo", res.Key)
				// Should still be "barbar", not "barbarbar" or more
				assert.Equal(c.t, "barbar", res.Value)
			}
			return err
		})
		assert.Nil(c.t, err)
	}

	// Verify final value
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		res, err := kvClient.Get(ctx, "foo")
		if err == nil {
			assert.Equal(c.t, "foo", res.Key)
			assert.Equal(c.t, "barbar", res.Value)
		}
		return err
	})
	assert.Nil(c.t, err)
}

func TestClientRequestIdempotency(t *testing.T) {
	RunTests(&BasicClientRequestIdempotency{t: t})
}
