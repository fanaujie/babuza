package testcase

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/builder"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"testing"
)

func snapshotManualTriggerTestComponents() []BabuzaComponent {
	// Create components for all combinations we want to test
	var components []BabuzaComponent

	// Test cases to test session validity across leadership changes
	testCases := []struct {
		caseName            string
		stateMachineCreator func(string) ibabuza.BaseStateMachine
		fileTag             string
	}{
		{
			caseName:            "memory store",
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine { return kvstore.NewMemoryStore() },
			fileTag:             kvstore.MemorySnapshotTag,
		},
		{
			caseName:            "memory store with concurrent snapshot",
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine { return kvstore.NewMemoryStoreWithConcurrentSnapshot() },
			fileTag:             kvstore.BadgerDBSnapshotTag,
		},
		{
			caseName:            "disk store",
			stateMachineCreator: func(s string) ibabuza.BaseStateMachine { return kvstore.NewDisk(s) },
			fileTag:             kvstore.BadgerDBSnapshotTag,
		},
	}

	// Create a BabuzaComponent for each test case
	for _, tc := range testCases {
		for _, transport := range []string{builder.TcpTransport} {
			components = append(components, BabuzaComponent{
				CaseName:           "SnapshotManualTrigger: 3nodes-" + transport + "-BabuzaWal-DurableSnapshot-" + tc.caseName,
				ClusterId:          1,
				CreateStateMachine: tc.stateMachineCreator,
				CreateCustomComponent: func(transport string) func(*babuza.BabuzaConfig, string, ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
					return func(config *babuza.BabuzaConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (babuza.BabuzaConfig, builder.BabuzaComponent) {
						b := customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, builder.DurableSnapshot,
							transport, proxyNet).
							SetClusterId(config.ClusterId).
							SetStorageRootDir(storageDir)
						return *config, *b.Build()
					}
				}(transport),
				ProxyNetwork: nil,
				TestParams:   tc.fileTag,
			})
		}
	}

	return components
}

type SnapshotManualTrigger struct {
	t *testing.T
}

func (c *SnapshotManualTrigger) Log(s string) {
	c.t.Log(s)
}

func (c *SnapshotManualTrigger) CreateTestComponents() []BabuzaComponent {
	return snapshotManualTriggerTestComponents()
}

func (c *SnapshotManualTrigger) Run(tc *testcluster.BabuzaCluster, testParams any) {
	wait := tc.RaftElectionTimeout() * 4
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)

	// Create a client with automatic incrementing session
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Write some data
	for i := 0; i < 8; i++ {
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			v := fmt.Sprintf("%d", i)
			res, cErr := kvClient.Set(ctx, v, v)
			assert.Equal(c.t, v, res.Key)
			assert.Equal(c.t, v, res.Value)
			return cErr
		}))
	}

	// Trigger manual snapshot
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	assert.Nil(c.t, tc.ExecutePeerRaftOperation(leaderId, func(r *babuza.Raft) error {
		result := r.ManualSnapshot(ctx)
		err = result.Wait()
		if err != nil {
			return err
		}
		// Verify snapshot
		assert.Equal(c.t, result.SnapshotMetadata().Snapshot.Metadata.Index, r.Status().LastSnapshotIndex)
		assert.Equal(c.t, result.SnapshotMetadata().Snapshot.Metadata.Term, r.Status().LastSnapshotTerm)
		tagMap := map[babuzapb.SnapshotFileType]struct{}{
			babuzapb.SnapshotFileType_Cluster:      {},
			babuzapb.SnapshotFileType_StateMachine: {},
			babuzapb.SnapshotFileType_Session:      {},
		}
		for _, tag := range result.SnapshotMetadata().Files {
			delete(tagMap, tag.FileType)
		}
		assert.Equal(c.t, 0, len(tagMap))
		// Check snapshot file content
		snapReader, err := result.SnapshotFileReader()
		assert.Nil(c.t, err)
		fs, _, err := snapReader.Open(testParams.(string))
		assert.Nil(c.t, err)
		om, err := newKvOperationOrderMap(fs)
		assert.Nil(c.t, err)
		for i := 0; i < 8; i++ {
			k := fmt.Sprintf("%d", i)
			v, ok := om.Get(k)
			assert.True(c.t, ok)
			assert.Equal(c.t, k, v)
		}
		return nil
	}))
}

func TestSnapshotManualTrigger(t *testing.T) {
	RunTests(&SnapshotManualTrigger{t: t})
}
