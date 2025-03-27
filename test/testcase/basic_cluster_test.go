package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
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
			CreateStateMachine: func(stateMachineDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(stateMachineDir)
			},
			CreateBuilder: func(babuzaConfig *babuza.BabuzaConfig, walDir, snapshotDir string, votingPeersCfg *babuza.VotingPeersConfiguration,
				pn ibabuza.ProxyNetwork) *babuza.BootstrapBuilder {
				logger := createDefaultLogger()
				return babuza.NewBootstrapBuilder().
					SetConfig(babuzaConfig).
					SetPeersConfig(votingPeersCfg).
					SetCluster(cluster.NewCluster(logger)).
					SetRaftNode(&raftnode.EtcdRaftNode{}).
					SetSessionManager(session.NewNoOpManager(logger)).
					SetSnapshotManager(snapshot.NewDurableSnapshotManager(snapshotDir, logger)).
					SetWalManager(babuzawal.NewWalManager(walDir, logger)).
					SetTransport(transport.New(
						babuzaConfig.ClusterId,
						transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
						limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(),
						protocol.NewTcp(networkio.NewTcpPhysicalIO(), logger), logger)).
					SetLogger(logger)
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
			CreateBuilder: func(babuzaConfig *babuza.BabuzaConfig, walDir, snapshotDir string, votingPeersCfg *babuza.VotingPeersConfiguration,
				pn ibabuza.ProxyNetwork) *babuza.BootstrapBuilder {
				logger := createDefaultLogger()
				return babuza.NewBootstrapBuilder().
					SetConfig(babuzaConfig).
					SetPeersConfig(votingPeersCfg).
					SetCluster(cluster.NewCluster(logger)).
					SetRaftNode(&raftnode.EtcdRaftNode{}).
					SetSessionManager(session.NewNoOpManager(logger)).
					SetSnapshotManager(snapshot.NewDurableSnapshotManager(snapshotDir, logger)).
					SetWalManager(babuzawal.NewWalManager(walDir, logger)).
					SetTransport(transport.New(
						babuzaConfig.ClusterId,
						transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
						limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(),
						protocol.NewHttp(logger), logger)).
					SetLogger(logger)
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
			CreateBuilder: func(babuzaConfig *babuza.BabuzaConfig, walDir, snapshotDir string, votingPeersCfg *babuza.VotingPeersConfiguration,
				pn ibabuza.ProxyNetwork) *babuza.BootstrapBuilder {
				logger := createDefaultLogger()
				return babuza.NewBootstrapBuilder().
					SetConfig(babuzaConfig).
					SetPeersConfig(votingPeersCfg).
					SetCluster(cluster.NewCluster(logger)).
					SetRaftNode(&raftnode.EtcdRaftNode{}).
					SetSessionManager(session.NewNoOpManager(logger)).
					SetSnapshotManager(snapshot.NewDurableSnapshotManager(snapshotDir, logger)).
					SetWalManager(babuzawal.NewWalManager(walDir, logger)).
					SetTransport(transport.New(
						babuzaConfig.ClusterId,
						transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
						limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(),
						protocol.NewGrpc(logger), logger)).
					SetLogger(logger)
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
