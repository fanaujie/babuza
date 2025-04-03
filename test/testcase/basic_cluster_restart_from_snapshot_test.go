package testcase

import (
	"context"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"testing"
	"time"
)

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
					CreateCustomComponent: func(snapshotType, sessionType, transportType string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
						return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
							config.SnapshotCount = snapshotCount
							chunkSize := 5 * 1024 * 1024
							b := customBabuzaComponent(sessionType, builder.BabuzaWal, snapshotType,
								transportType, proxyNet).
								SetClusterId(config.ClusterId).
								SetStorageRootDir(storageDir).
								AddTransportOptions(transport.SetTransportOptionsWithPeerSnapshotChunkSize(
									int64(chunkSize))).SetCustomLogger(&logger.Mock{})
							if snapshotType == builder.MinIOSnapshot {
								// If using MinIO and gRPC, the chunk size must be greater than 5MB.
								// If the number of chunks is greater than 1, the chunk size must be greater than 5MB.
								// This is a limitation of MinIO's compose functionality.
								if transportType == builder.GRPCTransport {
									b.AddGrpcOptions(protocol.SetGrpcOptsWithRecvMsgMaxSize(
										int(float32(chunkSize) * 1.2)))
								}
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

type BasicRestartFromSnapshot struct {
	t             *testing.T
	snapshotCount uint64
}

func (c *BasicRestartFromSnapshot) Log(s string) {
	c.t.Log(s)
}

func (c *BasicRestartFromSnapshot) CreateTestComponents() []BabuzaComponent {
	return basicSnapshotTestComponents(c.snapshotCount)
}

func (c *BasicRestartFromSnapshot) Run(tc *testcluster.BabuzaCluster, testParams any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	oldLeaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	// Create a client with automatic incrementing session
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Write enough data to trigger snapshot creation
	// We write (snapshotCount + 10) entries to ensure snapshot is created
	for i := uint64(0); i < c.snapshotCount+10; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return cErr
		}))
	}

	// Verify all peers have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

	// Record snapshot metadata from leader for later verification
	lastSnapshotIndex := uint64(0)
	lastSnapshotTerm := uint64(0)
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderId, func(s babuza.Status) bool {
		lastSnapshotIndex = s.LastSnapshotIndex
		lastSnapshotTerm = s.LastSnapshotTerm
		return s.LastSnapshotIndex >= c.snapshotCount &&
			s.LastSnapshotTerm > 0
	}))

	// Part 1: Test restart after snapshot creation
	// Shutdown leader to trigger leadership change
	assert.Nil(c.t, tc.ShutdownPeer(oldLeaderId))

	time.Sleep(time.Second * 5) // Wait for election to complete
	connectGroup.Remove(oldLeaderId)
	// Ensure new leader is elected
	newLeaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	// Write more data with the new leader
	for i := uint64(100); i < 108; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, err := kvClient.Set(ctx, v, v)
			if err != nil {
				return err
			}
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return nil
		}))
	}

	// Restart the old leader and verify it:
	// 1. Rejoins the cluster as a follower
	// 2. Has the correct snapshot information
	connectGroup.Add(oldLeaderId)
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(oldLeaderId, false), connectGroup.GetIds()))
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderId, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))

	// Verify all peers (including restarted old leader) have consistent state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
	assert.NotEqual(c.t, oldLeaderId, newLeaderId)

	// Part 2: Test restart again to verify consistency is maintained
	// Shutdown old leader again
	assert.Nil(c.t, tc.ShutdownPeer(oldLeaderId))

	// Write different data with entirely new keys
	for i := uint64(1000); i < 1008; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, err := kvClient.Set(ctx, v, v)
			if err != nil {
				return err
			}
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return nil
		}))
	}

	// Restart old leader again and verify it still:
	// 1. Rejoins as a follower
	// 2. Has the same snapshot information as before
	// 3. Successfully catches up with new data
	assert.Nil(c.t, tc.RestartPeer(wait, makeSingleStandardPeer(oldLeaderId, false), connectGroup.GetIds()))
	assert.Nil(c.t, tc.CheckStatus(wait, oldLeaderId, func(s babuza.Status) bool {
		return s.LastSnapshotIndex == lastSnapshotIndex &&
			s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
	}))

	// Final verification that all nodes have identical state
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestRestartFromSnapshot(t *testing.T) {
	RunTests(&BasicRestartFromSnapshot{
		t:             t,
		snapshotCount: 50})
}
