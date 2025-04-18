package testcase

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	"github.com/fanaujie/babuza/test/testcluster"
	"io"
	"time"
)

func makeVotingStandardPeers(totalPeers int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	var peers []testcluster.Peer
	var peerIDs []uint64
	for i := 0; i < totalPeers; i++ {
		peerID := uint64(i + 1)
		peerIDs = append(peerIDs, peerID)
		peers = append(peers, makeSingleStandardPeer(peerID, false))
	}
	return peers, testcluster.NewConnectedGroup(peerIDs)
}
func makeSingleStandardPeer(peerID uint64, isLearner bool) testcluster.Peer {
	return &testcluster.StandardPeer{
		Id:                  peerID,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerID),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerID)},
		IsLearner:           isLearner,
	}
}

func makeVotingProxyPeers(count int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	var peers []testcluster.Peer
	var peerIDs []uint64
	for i := 0; i < count; i++ {
		peerID := uint64(i + 1)
		peerIDs = append(peerIDs, peerID)
		peers = append(peers, makeSingleProxyPeer(peerID, false))
	}
	return peers, testcluster.NewConnectedGroup(peerIDs)
}

func makeSingleProxyPeer(peerID uint64, isLearner bool) testcluster.Peer {
	return &testcluster.BabuzaPeer{
		Id:                  peerID,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerID),
		ProxyListenAddr:     fmt.Sprintf("127.0.0.1:%d", 24200+peerID),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerID)},
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

func basicClusterComponents(disableProposalForwarding bool) []BabuzaComponent {
	var components []BabuzaComponent
	for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.LsmtWalDisk} {
		for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
			components = append(components, BabuzaComponent{
				CaseName:  fmt.Sprintf("BasicTest: 3nodes-%s-DiskStateMachine-(%s)-DurableSnapshot-NoOpSession", transportType, walType),
				ClusterId: 1,
				CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
					return kvstore.NewDisk(storeDir)
				},
				CreateCustomComponent: func(walType, transportType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						config.BubuzaConfig.RaftConfig.DisableProposalForwarding = disableProposalForwarding
						b := customBabuzaComponent(builder.NoOpSession, walType, builder.DurableSnapshot,
							transportType, proxyNet).
							SetClusterId(config.BubuzaConfig.ClusterID).
							SetStorageRootDir(storageDir)
						return *config, *b.Build()
					}
				}(walType, transportType),
				ProxyNetwork: nil,
			})
		}
	}
	return components
}

func proxyClusterComponents(checkQuorum, preVote bool) []BabuzaComponent {
	var components []BabuzaComponent
	for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.LsmtWalDisk} {
		for _, transportType := range []string{builder.TcpTransport} {
			pn := proxynetwork.New()
			components = append(components, BabuzaComponent{
				CaseName: fmt.Sprintf("ProxyTest(checkQuorum=%t,preVote=%t): 3nodes-%s-MemoryStateMachine-%s-DurableSnapshot-NoOpSession",
					checkQuorum, preVote, transportType, walType),
				ClusterId: 1,
				CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
					return kvstore.NewMemoryStore()
				},
				CreateCustomComponent: func(walType, transportType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
					return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						config.BubuzaConfig.RaftConfig.CheckQuorum = checkQuorum
						config.BubuzaConfig.RaftConfig.PreVote = preVote
						b := customBabuzaComponent(builder.NoOpSession, walType, builder.DurableSnapshot,
							transportType, proxyNet).
							SetClusterId(config.BubuzaConfig.ClusterID).
							SetStorageRootDir(storageDir)
						return *config, *b.Build()
					}
				}(walType, transportType),
				ProxyNetwork: pn,
			})
		}
	}
	return components
}

func newKvOperationOrderMap(reader io.Reader) (*kvstore.KvOperationOrderMap, error) {
	buf := make([]byte, 8)
	store := kvstore.NewKvOperationOrderMap()

	var batchKv kvstore.BatchKVPair
	for {
		if _, err := io.ReadFull(reader, buf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		batchKvSize := binary.LittleEndian.Uint64(buf)
		data := make([]byte, batchKvSize)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &batchKv); err != nil {
			return nil, err
		}
		for _, pair := range batchKv {
			store.Set(string(pair.Key), string(pair.Value))
		}
		batchKv = batchKv[:0]
	}
	return store, nil
}
