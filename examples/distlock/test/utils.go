package test

import (
	"github.com/fanaujie/babuza/examples/distlock/embedapp"
	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	"github.com/fanaujie/babuza/test/testcluster"
)

var peerFactory = testcluster.NewPeerFactory(15200, 11000, 25200)

func makeVotingStandardPeers(count int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	return peerFactory.MakeVotingStandardPeers(count)
}

func makeVotingProxyPeers(count int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	return peerFactory.MakeVotingProxyPeers(count)
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

func basicClusterComponents() []BabuzaComponent {
	var components []BabuzaComponent
	components = append(components, BabuzaComponent{
		CaseName:  "BasicTest: 3nodes-TcpTransport-MemoryStateMachine-BadgerWal-DurableSnapshot-NoOpSession",
		ClusterId: 1,
		CreateStateMachine: func(storeDir string) *lockstore.LockStore {
			return lockstore.NewLockStore()
		},
		CreateCustomComponent: func(config *embedapp.DistLockAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.DistLockAppConfig, builder.BabuzaComponent) {
			b := customBabuzaComponent(builder.NoOpSession, builder.BadgerWalMemory, builder.DurableSnapshot,
				builder.TcpTransport, proxyNet).
				SetClusterId(config.BabuzaConfig.ClusterID).
				SetStorageRootDir(storageDir)
			return *config, *b.Build()
		},
		ProxyNetwork: nil,
	})
	return components
}

func proxyClusterComponents() []BabuzaComponent {
	var components []BabuzaComponent
	pn := proxynetwork.New()
	components = append(components, BabuzaComponent{
		CaseName:  "ProxyTest: 3nodes-TcpTransport-MemoryStateMachine-BadgerWal-DurableSnapshot-NoOpSession",
		ClusterId: 1,
		CreateStateMachine: func(storeDir string) *lockstore.LockStore {
			return lockstore.NewLockStore()
		},
		CreateCustomComponent: func(config *embedapp.DistLockAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.DistLockAppConfig, builder.BabuzaComponent) {
			config.BabuzaConfig.RaftConfig.CheckQuorum = true
			config.BabuzaConfig.RaftConfig.PreVote = true
			b := customBabuzaComponent(builder.NoOpSession, builder.BadgerWalMemory, builder.DurableSnapshot,
				builder.TcpTransport, proxyNet).
				SetClusterId(config.BabuzaConfig.ClusterID).
				SetStorageRootDir(storageDir)
			return *config, *b.Build()
		},
		ProxyNetwork: pn,
	})
	return components
}
