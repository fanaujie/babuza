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
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Expired session component with specified duration
func expiredSessionComponents(sessionExpiredNaoSec int64) []BabuzaComponent {
	var components []BabuzaComponent
	for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
		components = append(components, BabuzaComponent{
			CaseName:  "BasicTest: 3nodes-" + transportType + "DiskStateMachine-BabuzaWal-DurableSnapshot-ExpireOpSession",
			ClusterId: 1,
			CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(storeDir)
			},
			CreateCustomComponent: func(transport string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					b := customBabuzaComponent(builder.ExpireSession, builder.BabuzaWal, builder.DurableSnapshot,
						transport, proxyNet).
						SetClusterId(config.ClusterId).
						SetStorageRootDir(storageDir).AddExpireSessionOptions(
						session.SetExpiredMgrOptionsWithExpiredNanoseconds(sessionExpiredNaoSec),
						session.SetExpiredMgrOptionsWithSnapshotCompressionType(babuzapb.SnapshotFileCompression_Snappy))
					return *config, *b.Build()
				}
			}(transportType),
			ProxyNetwork: nil,
		})
	}
	return components
}

type BasicClientExpiredSession struct {
	t *testing.T
}

func (c *BasicClientExpiredSession) Log(s string) {
	c.t.Log(s)
}

func (c *BasicClientExpiredSession) CreateTestComponents() []BabuzaComponent {
	return expiredSessionComponents(int64(time.Second)) // 1 second expiry
}

func (c *BasicClientExpiredSession) Run(tc *testcluster.BabuzaCluster) {
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
	// Wait for the session to expire
	time.Sleep(time.Second)

	// This attempt should fail with a session expired error
	err = runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err = expiredClient.Set(ctx, "foo", "bar")
		return err
	})

	// Verify we get session expired error
	assert.ErrorIs(c.t, err, session.ErrSessionExpired)

}

func TestClientRegisterSessionExpired(t *testing.T) {
	RunTests(&BasicClientExpiredSession{t: t})
}
