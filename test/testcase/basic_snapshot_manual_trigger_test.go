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
	"fmt"
	"testing"

	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
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
		for _, snapshotType := range []string{builder.DurableSnapshot, builder.S3Snapshot} {
			for _, transport := range []string{builder.TcpTransport, builder.HttpTransport, builder.GRPCTransport} {
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
					CaseName:           "SnapshotManualTrigger: 3nodes-" + transport + "-BabuzaWal-" + snapshotType + "-" + tc.caseName,
					ClusterId:          1,
					CreateStateMachine: tc.stateMachineCreator,
					CreateCustomComponent: func(snapshotType, transport string) func(*embedapp.KvStoreAppConfig, string, ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
						return func(config *embedapp.KvStoreAppConfig, storageDir string, proxyNet ibabuza.ProxyNetwork) (embedapp.KvStoreAppConfig, builder.BabuzaComponent) {
							b := customBabuzaComponent(builder.NoOpSession, builder.BabuzaWal, snapshotType,
								transport, proxyNet).
								SetClusterId(config.BubuzaConfig.ClusterID).
								SetStorageRootDir(storageDir)
							if snapshotType == builder.S3Snapshot {
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
					}(snapshotType, transport),
					ProxyNetwork: nil,
					TestParams:   tc.fileTag,
				})
			}
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
	fileFlag, ok := testParams.(string)
	if !ok {
		c.t.Fatalf("Invalid test parameter: %v", testParams)
	}
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	// Identify the current leader
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
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

	assert.Nil(c.t, tc.ExecutePeerRaftOperation(leaderID, func(r *babuza.Raft) error {
		result := r.ManualSnapshot(ctx)
		err = result.Wait()
		if err != nil {
			return err
		}
		// Verify snapshot
		m, _ := result.SnapshotMetadata()
		assert.Equal(c.t, m.Snapshot.Metadata.Index, r.Status().LastSnapshotIndex)
		assert.Equal(c.t, m.Snapshot.Metadata.Term, r.Status().LastSnapshotTerm)
		tagMap := map[babuzapb.SnapshotFileType]struct{}{
			babuzapb.SnapshotFileType_Cluster:      {},
			babuzapb.SnapshotFileType_StateMachine: {},
			babuzapb.SnapshotFileType_Session:      {},
		}
		for _, tag := range m.Files {
			delete(tagMap, tag.FileType)
		}
		assert.Equal(c.t, 0, len(tagMap))
		// Check snapshot file content
		snapReader, err := result.SnapshotFileReader()
		assert.Nil(c.t, err)
		fs, _, err := snapReader.Open(fileFlag)
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
