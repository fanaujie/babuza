package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"time"
)

type babuzaDirectory struct {
	stateMachineDir string
	walDir          string
	snapshotDir     string
}

func makeVotingPeers(totalPeers int) ([]testcluster.BabuzaPeer, *testcluster.ConnectedGroup) {
	var peers []testcluster.BabuzaPeer
	for i := 0; i < totalPeers; i++ {
		peerId := uint64(i + 1)
		peers = append(peers, testcluster.BabuzaPeer{
			Id:                  peerId,
			RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
			ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
			AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		})
	}
	return peers, testcluster.NewConnectedGroup(peers)
}
func makeSinglePeer(peerId uint64, isLearner bool) testcluster.BabuzaPeer {
	return testcluster.BabuzaPeer{
		Id:                  peerId,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
		ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerId),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		IsLearner:           isLearner,
	}
}

func customBabuzaComponent(sessionType, walType, snapshotType, transport string,
	proxyNet ibabuza.ProxyNetwork) *builder.BabuzaComponentBuilder {
	b := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		SessionType:   sessionType,
		TransportType: transport,
		WalType:       walType,
		SnapshotType:  snapshotType,
	})
	if proxyNet != nil {
		b.SetTransportTcpNetwork(proxyNet.(tcp.NetworkIO))
	}
	return b
}

func runWithCtxTimeout(timeout time.Duration, run func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return run(ctx)
}

func peerConfigExists(ctx context.Context, userProxyNetwork bool, c *client.KvStoreClient, peer testcluster.BabuzaPeer) error {
	targetAddr := peer.RaftListenAddr
	if userProxyNetwork {
		targetAddr = peer.ProxyListenAddr
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			ctx2, cancel := context.WithTimeout(context.Background(), time.Second)
			clusterCfg, err := c.GetClusterConfiguration(ctx2)
			cancel()
			if err != nil {
				return err
			}
			for _, p := range clusterCfg.Peers {
				if p.Id == peer.Id && p.IsLearner == p.IsLearner && p.RaftListenAddr == targetAddr {
					return nil
				}
			}
		}
	}
}

func basicClusterComponents() []BabuzaComponent {
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
			ProxyNetwork: nil,
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
			ProxyNetwork: nil,
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
			ProxyNetwork: nil,
		},
	}
}
