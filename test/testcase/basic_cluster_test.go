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
	"os"
	"strconv"
	"testing"
)

type BasicCluster struct {
	t *testing.T
}

func (c *BasicCluster) Log(s string) {
	c.t.Log(s)
}

func (c *BasicCluster) CreateTestComponents() []BabuzaComponent {
	return []BabuzaComponent{
		{
			CaseName:  "BasicTest: 3nodes-Tcp-DiskStateMachine-BabuzaWal-DurableSnapshot-NoOpSession",
			ClusterId: 1,
			CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(storeDir)
			},
			CreateCustomComponent: func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return *config, *customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
					builder.TcpTransport, proxyNet).SetClusterId(config.ClusterId).SetStorageRootDir(storageDir).Build()
			},
			StorageRootDir: os.TempDir(),
			ProxyNetwork:   nil,
		},
		{
			CaseName:  "BasicTest: 3nodes-Http-DiskStateMachine-BabuzaWal-DurableSnapshot-NoOpSession",
			ClusterId: 1,
			CreateStateMachine: func(stateMachineDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(stateMachineDir)
			},
			CreateCustomComponent: func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return *config, *customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
					builder.HttpTransport, proxyNet).SetClusterId(config.ClusterId).SetStorageRootDir(storageDir).Build()
			},
			StorageRootDir: os.TempDir(),
			ProxyNetwork:   nil,
		},
		{
			CaseName:  "BasicTest: 3nodes-GRPC-DiskStateMachine-BabuzaWal-DurableSnapshot-NoOpSession",
			ClusterId: 1,
			CreateStateMachine: func(stateMachineDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(stateMachineDir)
			},
			CreateCustomComponent: func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return *config, *customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
					builder.GRPCTranspost, proxyNet).SetClusterId(config.ClusterId).SetStorageRootDir(storageDir).Build()
			},
			StorageRootDir: os.TempDir(),
			ProxyNetwork:   nil,
		},
	}
}

func (c *BasicCluster) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()
	for i := 0; i < 64; i++ {
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.Set(ctx, s, s)
			return err
		}))
	}
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}
