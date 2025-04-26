package testcase

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/testcontainers/testcontainers-go/modules/minio"
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

type minioContainer struct {
	minioContainer *minio.MinioContainer
	setupFunc      func() (*minio.MinioContainer, error)
	deferFunc      func()
}

func (m *minioContainer) Setup() error {
	c, err := minio.Run(context.Background(), "minio/minio:latest",
		minio.WithUsername("minioroot"), minio.WithPassword("miniopassword"))
	if err != nil {
		return err
	}
	m.minioContainer = c
	return nil
}

func (m *minioContainer) Defer() error {
	if m.minioContainer != nil {
		return m.minioContainer.Terminate(context.Background())
	}
	return errors.New("minio container is nil")
}

func basicSnapshotTestComponents(snapshotCount uint64) []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to test session validity across leadership changes
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
		for _, snapshotType := range []string{builder.DurableSnapshot, builder.MinIOSnapshot} {
			for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
				var mc *minioContainer
				if snapshotType == builder.MinIOSnapshot {
					mc = &minioContainer{}
				}
				components = append(components, BabuzaComponent{
					InitFunc: func() error {
						if mc == nil {
							return nil
						}
						return mc.Setup()
					},
					DeferFunc: func() error {
						if mc == nil {
							return nil
						}
						return mc.Defer()
					},
					CaseName:           "BasicTest: 3nodes-" + transportType + "-BabuzaWal-" + snapshotType + "-" + tc.caseName,
					ClusterId:          1,
					CreateStateMachine: tc.stateMachineCreator,
					CreateCustomComponent: func(snapshotType, sessionType, transportType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
							config.BubuzaConfig.SnapshotCount = snapshotCount
							chunkSize := 5 * 1024 * 1024
							b := customBabuzaComponent(sessionType, builder.BabuzaWal, snapshotType,
								transportType, proxyNet).
								SetClusterId(config.BubuzaConfig.ClusterID).
								SetStorageRootDir(storageDir).
								AddTransportOptions(transport.SetTransportOptionsWithPeerSnapshotChunkSize(
									int64(chunkSize)))
							if transportType == builder.GRPCTransport {
								b.AddGrpcOptions(protocol.SetGrpcOptsWithRecvMsgMaxSize(
									int(float32(chunkSize) * 1.2)))
							}
							if snapshotType == builder.MinIOSnapshot {
								// If using MinIO and gRPC, the chunk size must be greater than 5MB.
								// If the number of chunks is greater than 1, the chunk size must be greater than 5MB.
								// This is a limitation of MinIO's compose functionality.
								endpoint, err := mc.minioContainer.ConnectionString(context.Background())
								if err != nil {
									panic(err)
								}
								b.SetMinIOConfig(&cloudstorage.Config{
									Endpoint:        endpoint,
									AccessKeyID:     mc.minioContainer.Username,
									SecretAccessKey: mc.minioContainer.Password,
									UseSSL:          false,
									Bucket:          "test-bucket",
									Prefix:          "snapshot",
								})
							}
							return *config, *b.Build()
						}
					}(snapshotType, tc.sessionType, transportType),
					ProxyNetwork: nil,
				})
			}
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
