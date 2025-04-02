package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/session"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Component factory for session expiration testing
func sessionLruExpirationTestComponents() []BabuzaComponent {
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
			CreateCustomComponent: func(sessionType string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					b := customBabuzaComponent(sessionType, builder.BabuzaWal, builder.DurableSnapshot,
						builder.TcpTransport, proxyNet).
						SetClusterId(config.ClusterId).
						SetStorageRootDir(storageDir).
						SetCustomLogger(&logger.Mock{}).
						AddLruSessionOptions(session.SetLruMgrOptionsWithMaxSessions(2))

					return *config, *b.Build()
				}
			}(tc.sessionType),
			ProxyNetwork: nil,
		})
	}

	return components
}

type BasicClientSessionLruExpiredResponse struct {
	t *testing.T
}

func (c *BasicClientSessionLruExpiredResponse) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientSessionLruExpiredResponse) CreateTestComponents() []BabuzaComponent {
	return sessionLruExpirationTestComponents()
}

func (c *BasicClientSessionLruExpiredResponse) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "test-key", "test-value")
		return err
	})
	assert.Nil(c.t, err)

	var tempClients []*client.KvStoreClient
	for i := 0; i < 3; i++ {
		tempClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
		assert.Nil(c.t, err)

		err = runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err := tempClient.Set(ctx, "temp-key", "temp-value")
			return err
		})
		assert.Nil(c.t, err)
		if i == 2 {
			_ = tempClient.Close()
		} else {
			tempClients = append(tempClients, tempClient)
		}
	}
	defer func() {
		for _, cl := range tempClients {
			_ = cl.Close()
		}
	}()

	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = kvClient.Set(ctx, "key-after-expiration", "value")
		return err
	})

	assert.Error(c.t, err)
	assert.ErrorIs(c.t, err, session.ErrSessionExpired)

}

func TestClientSessionLruExpiredResponse(t *testing.T) {
	RunTests(&BasicClientSessionLruExpiredResponse{t: t})
}
