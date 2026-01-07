// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package testcase

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var peerFactory = testcluster.NewPeerFactory(14200, 10000, 24200)

func makeVotingStandardPeers(totalPeers int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	return peerFactory.MakeVotingStandardPeers(totalPeers)
}

func makeSingleStandardPeer(peerID uint64, isLearner bool) testcluster.Peer {
	return peerFactory.MakeSingleStandardPeer(peerID, isLearner)
}

func makeVotingProxyPeers(count int) ([]testcluster.Peer, *testcluster.ConnectedGroup) {
	return peerFactory.MakeVotingProxyPeers(count)
}

func makeSingleProxyPeer(peerID uint64, isLearner bool) testcluster.Peer {
	return peerFactory.MakeSingleProxyPeer(peerID, isLearner)
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
	testCases := []struct {
		caseName            string
		stateMachineCreator func(string) ibabuza.BaseStateMachine
	}{
		{
			caseName: "memory state machine",
			stateMachineCreator: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStore()
			},
		},
		{
			caseName: "disk state machine",
			stateMachineCreator: func(storeDir string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(storeDir)
			},
		},
	}
	var components []BabuzaComponent
	//// Create a BabuzaComponent for each test case
	for _, tc := range testCases {
		for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.BadgerWalDisk, builder.PebbleWalDisk} {
			for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
				components = append(components, BabuzaComponent{
					CaseName:  fmt.Sprintf("BasicTest: 3nodes-(%s)-(%s)-(%s)-DurableSnapshot-NoOpSession", transportType, tc.caseName, walType),
					ClusterId: 1,
					CreateStateMachine: func(storeDir string) ibabuza.BaseStateMachine {
						return tc.stateMachineCreator(storeDir)
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
	}
	return components
}

const (
	rustfsImage    = "rustfs/rustfs:latest"
	rustfsUsername = "rustfsadmin"
	rustfsPassword = "rustfsadmin"
)

type s3Container struct {
	container testcontainers.Container
	endpoint  string
}

func (s *s3Container) Setup() error {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        rustfsImage,
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": rustfsUsername,
			"RUSTFS_SECRET_KEY": rustfsPassword,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}
	s.container = container

	host, err := container.Host(ctx)
	if err != nil {
		return err
	}
	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return err
	}
	s.endpoint = "http://" + host + ":" + mappedPort.Port()
	return nil
}

func (s *s3Container) Defer() error {
	if s.container != nil {
		return s.container.Terminate(context.Background())
	}
	return errors.New("s3 container is nil")
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
		for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.BadgerWalDisk, builder.PebbleWalDisk} {
			for _, snapshotType := range []string{builder.DurableSnapshot, builder.S3Snapshot} {
				for _, transportType := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
					var sc *s3Container
					if snapshotType == builder.S3Snapshot {
						sc = &s3Container{}
					}
					components = append(components, BabuzaComponent{
						InitFunc: func() error {
							if sc == nil {
								return nil
							}
							return sc.Setup()
						},
						DeferFunc: func() error {
							if sc == nil {
								return nil
							}
							return sc.Defer()
						},
						CaseName:           "BasicTest: 3nodes-" + transportType + walType + snapshotType + "-" + tc.caseName,
						ClusterId:          1,
						CreateStateMachine: tc.stateMachineCreator,
						CreateCustomComponent: func(snapshotType, sessionType, transportType string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
							return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
								config.BubuzaConfig.SnapshotCount = snapshotCount
								chunkSize := 5 * 1024 * 1024
								b := customBabuzaComponent(sessionType, walType, snapshotType,
									transportType, proxyNet).
									SetClusterId(config.BubuzaConfig.ClusterID).
									SetStorageRootDir(storageDir).
									AddTransportOptions(transport.SetTransportOptionsWithPeerSnapshotChunkSize(
										int64(chunkSize)))
								if transportType == builder.GRPCTransport {
									b.AddGrpcOptions(protocol.SetGrpcOptsWithRecvMsgMaxSize(
										int(float32(chunkSize) * 1.2)))
								}
								if snapshotType == builder.S3Snapshot {
									// If using S3 and gRPC, the chunk size must be greater than 5MB.
									// If the number of chunks is greater than 1, the chunk size must be greater than 5MB.
									// This is a limitation of S3's multipart upload functionality.
									b.SetS3Config(&cloudstorage.S3Config{
										Endpoint:        sc.endpoint,
										Region:          "us-east-1",
										AccessKeyID:     rustfsUsername,
										SecretAccessKey: rustfsPassword,
										UsePathStyle:    true,
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

	}

	return components
}

func proxyClusterComponents(checkQuorum, preVote bool) []BabuzaComponent {
	var components []BabuzaComponent
	for _, walType := range []string{builder.BabuzaWal, builder.ETCDWal, builder.BadgerWalDisk, builder.PebbleWalDisk} {
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

type MyClient struct {
	cs babuza.ClientSession
}

func NewMyClient(sessionId uint64) *MyClient {
	return &MyClient{
		cs: babuza.ClientSession{
			SessionID:      sessionId,
			SequenceNumber: 0,
		},
	}
}

func (c *MyClient) ClientSession() babuza.ClientSession {
	c.cs.SequenceNumber++
	return c.cs
}

func (c *MyClient) Response(sequenceNumber uint64) {

}

type raftKVDirectClient struct {
	kvStores []*babuza.Raft
	client   *MyClient
	leader   int
}

func newDirectRaftKvClient(kvStoreCluster []*babuza.Raft) *raftKVDirectClient {
	c := &raftKVDirectClient{
		kvStores: kvStoreCluster,
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		res := c.kvStores[c.leader].RegisterSession(ctx)
		result := res.WaitForApplyResult()
		if result.Error == nil {
			c.client = NewMyClient(result.LogIndex)
			res.Release()
			cancel()
			break
		}
		res.Release()
		cancel()
		c.nextTryLeader()
		time.Sleep(time.Millisecond * 300)
	}
	return c
}

func (c *raftKVDirectClient) Get(ctx context.Context, key string) (string, error) {
	r := c.kvStores[c.leader]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err := r.LinearizableRead(ctx)
	defer cancel()
	if err == nil {
		v, err := r.GetStateMachine().(*kvstore.MemoryStoreWithSession).Query(key)
		if err != nil {
			if errors.Is(err, kverror.ErrKeyNotFound) {
				return "", nil
			}
			fmt.Println("raftKVDirectClient: failed to get key", err.Error())
			return "", err
		}
		sv := v.(string)
		return sv, nil
	}
	fmt.Println("raftKVDirectClient: failed to get key", err.Error())
	return "", err
}

func (c *raftKVDirectClient) Set(ctx context.Context, key string, value string) error {
	var req kvstore.KvCommand
	command, err := req.Set(key, value)
	if err != nil {
		return err
	}
	c.command(command)
	return nil
}

func (c *raftKVDirectClient) Append(ctx context.Context, key string, value string) error {
	var req kvstore.KvCommand
	command, err := req.Append(key, value)
	if err != nil {
		return err
	}
	c.command(command)
	return nil
}

func (c *raftKVDirectClient) nextTryLeader() {
	c.leader++
	if c.leader == len(c.kvStores) {
		c.leader = 0
	}
}

func (c *raftKVDirectClient) command(command []byte) {
	cs := c.client.ClientSession()
	for {
		r := c.kvStores[c.leader]
		if r.Status().IsLeader() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			result := r.ProposeThenWaitResponse(ctx, cs, command)
			if result.Error == nil {
				cancel()
				c.client.Response(0)
				return
			}
			cancel()
			fmt.Println("raftKVDirectClient: failed to propose command, retrying...", result.Error)
		}
		c.nextTryLeader()
		time.Sleep(time.Millisecond * 100)
	}
}

type kvClientWrapper struct {
	cli           *client.KvStoreClient
	maxRetries    int
	manualSession *client.ManualIncrementSession
}

func newKvClientWrapper(maxRetries int, cli *client.KvStoreClient, manualSession *client.ManualIncrementSession) *kvClientWrapper {
	return &kvClientWrapper{
		maxRetries:    maxRetries,
		cli:           cli,
		manualSession: manualSession,
	}
}

func (k *kvClientWrapper) Get(ctx context.Context, key string) (string, error) {
	v, err := k.cli.LinearizableGet(ctx, key)
	if err != nil {
		return "", err
	}
	return v.Value, nil
}

func (k *kvClientWrapper) Set(ctx context.Context, key string, value string) error {
	s := k.manualSession.ClientSession()
	for i := 0; i < k.maxRetries; i++ {
		ctx2, cancel := context.WithTimeout(ctx, time.Second)
		k.manualSession.SetSequenceNumber(s.SequenceNumber + 1)
		_, err := k.cli.Set(ctx2, key, value)
		cancel()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("kvClientWrapper failed to set value for key %s after %d retries", key, k.maxRetries)
}

func (k *kvClientWrapper) Append(ctx context.Context, key string, value string) error {
	s := k.manualSession.ClientSession()
	for i := 0; i < k.maxRetries; i++ {
		ctx2, cancel := context.WithTimeout(ctx, time.Second)
		k.manualSession.SetSequenceNumber(s.SequenceNumber + 1)
		_, err := k.cli.Append(ctx2, key, value)
		cancel()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("kvClientWrapper failed to append value for key %s after %d retries", key, k.maxRetries)
}

//
//type linearizabilityKvClient interface {
//	Get(ctx context.Context, key string) (string, error)
//	Set(ctx context.Context, key string, value string)
//	Append(ctx context.Context, key string, value string)
//}
