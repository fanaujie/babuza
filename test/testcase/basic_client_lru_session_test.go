package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

// LRU session component with limit on maximum sessions
func lruSessionComponents(maxSessions int64) []BabuzaComponent {
	var components []BabuzaComponent
	for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
		components = append(components, BabuzaComponent{
			CaseName:  "BasicTest: 3nodes-" + transportType + "DiskStateMachine-BabuzaWal-DurableSnapshot-LruOpSession",
			ClusterId: 1,
			CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(storeDir)
			},
			CreateCustomComponent: func(transport string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
				return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					b := customBabuzaComponent(builder.ExpireSession, builder.BabuzaWal, builder.DurableSnapshot,
						transport, proxyNet).
						SetClusterId(config.BubuzaConfig.ClusterId).
						SetStorageRootDir(storageDir).AddLruSessionOptions(
						session.SetLruMgrOptionsWithMaxSessions(maxSessions),
						session.SetLruMgrOptionsWithSnapshotCompressionType(babuzapb.SnapshotFileCompression_Snappy))
					return *config, *b.Build()
				}
			}(transportType),
			ProxyNetwork: nil,
		})
	}
	return components
}

type BasicClientLRUSession struct {
	t *testing.T
}

func (c *BasicClientLRUSession) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientLRUSession) CreateTestComponents() []BabuzaComponent {
	return lruSessionComponents(5) // Max 5 sessions
}

func (c *BasicClientLRUSession) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Identify the current leader
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	// Create a client with auto increment session
	expiredClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = expiredClient.Close()
	}()

	// Create 5 more clients with auto increment sessions to reach our limit
	for i := int64(0); i < 5; i++ {
		tempClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
		assert.Nil(c.t, err)
		assert.Nil(c.t, tempClient.Close())
	}

	// This attempt should fail with a session expired error since we've exceeded the LRU limit
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = expiredClient.Set(ctx, "foo", "bar")
		return err
	})

	// Verify we get session expired error
	assert.ErrorIs(c.t, err, session.ErrSessionExpired)
}

func TestClientRegisterSession_LRU(t *testing.T) {
	RunTests(&BasicClientLRUSession{t: t})
}
