package testcase

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"io"
	"time"
)

func makeVotingStandardPeers(totalPeers int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	var peers []testcluster.Peer
	var peerIDs []uint64
	for i := 0; i < totalPeers; i++ {
		peerId := uint64(i + 1)
		peerIDs = append(peerIDs, peerId)
		peers = append(peers, makeSingleStandardPeer(peerId, false))
	}
	return peers, testcluster.NewConnectedGroup(peerIDs)
}
func makeSingleStandardPeer(peerId uint64, isLearner bool) testcluster.Peer {
	return &testcluster.StandardPeer{
		Id:                  peerId,
		RaftListenAddr:      fmt.Sprintf("127.0.0.1:%d", 14200+peerId),
		AppServiceAddresses: []string{fmt.Sprintf("127.0.0.1:%d", 10000+peerId)},
		IsLearner:           isLearner,
	}
}

func makeVotingProxyPeers(count int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	var peers []testcluster.Peer
	var peerIDs []uint64
	for i := 0; i < count; i++ {
		peerId := uint64(i + 1)
		peerIDs = append(peerIDs, peerId)
		peers = append(peers, makeSingleProxyPeer(peerId, false))
	}
	return peers, testcluster.NewConnectedGroup(peerIDs)
}

func makeSingleProxyPeer(peerId uint64, isLearner bool) testcluster.Peer {
	return &testcluster.BabuzaPeer{
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

func basicClusterComponents(disableProposalForwarding bool) []BabuzaComponent {
	var components []BabuzaComponent
	for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
		components = append(components, BabuzaComponent{
			CaseName:  "BasicTest: 3nodes-" + transportType + "DiskStateMachine-BabuzaWal-DurableSnapshot-NoOpSession",
			ClusterId: 1,
			CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(storeDir)
			},
			CreateCustomComponent: func(transport string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
				return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					config.DisableProposalForwarding = disableProposalForwarding
					b := customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
						transport, proxyNet).
						SetClusterId(config.ClusterId).
						SetStorageRootDir(storageDir)
					return *config, *b.Build()
				}
			}(transportType),
			ProxyNetwork: nil,
		})
	}
	return components
}

func proxyClusterComponents() []BabuzaComponent {
	var components []BabuzaComponent
	for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.LsmtWalDisk} {
		for _, transportType := range []string{builder.TcpTransport} {
			pn := proxynetwork.New()
			components = append(components, BabuzaComponent{
				CaseName:  "ProxyTest: 3nodes-" + transportType + "-MemoryStateMachine-" + walType + "-DurableSnapshot-NoOpSession",
				ClusterId: 1,
				CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
					return kvstore.NewMemoryStore()
				},
				CreateCustomComponent: func(walType, transportType string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
						b := customBabuzaComponent(builder.NoOpSession, walType, builder.DurableSnapshot,
							transportType, proxyNet).
							SetClusterId(config.ClusterId).
							SetStorageRootDir(storageDir).
							SetTransportTcpNetwork(pn)
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
