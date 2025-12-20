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

package raft

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func genConfChangeEntry(index, nodeId uint64, confChangeType raftpb.ConfChangeType, result []raftpb.Entry) []raftpb.Entry {
	cc := raftpb.ConfChange{
		Type:    confChangeType,
		NodeID:  nodeId,
		Context: nil,
	}
	data, _ := cc.Marshal()
	result = append(result, raftpb.Entry{
		Term:  1,
		Index: index,
		Type:  raftpb.EntryConfChange,
		Data:  data,
	})
	return result
}

func TestBootstrap_ListRaftConfChangeAddNodeIds(t *testing.T) {

	walDir := os.TempDir()
	defer os.RemoveAll(walDir)

	ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
	expectIds := func() UInt64Slice {
		var result []raftpb.Entry
		result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
		result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
		result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
		result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
		result = genConfChangeEntry(5, 50, raftpb.ConfChangeUpdateNode, result)
		result = genConfChangeEntry(6, 51, raftpb.ConfChangeUpdateNode, result)
		_, w, err := ws.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 2,
		})
		assert.Nil(t, err)
		assert.Nil(t, w.Save(raftpb.HardState{
			Term:   1,
			Commit: 6,
		}, result))
		assert.Nil(t, w.Close())
		return UInt64Slice{2, 3}
	}()

	result, _, w, err := ws.ReplayWal(nil, false)
	assert.Nil(t, err)
	assert.Nil(t, w.Close())

	t.Run("snapshot is nil", func(t *testing.T) {
		idSlice, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		assert.Equal(t, expectIds, idSlice)
	})

	t.Run("snapshot is not nil", func(t *testing.T) {
		voters := []uint64{51, 52}
		idSlice, err := listRaftConfChangeAddNodeIds(&raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{
					Voters: voters,
				},
			},
		}, result)
		assert.Nil(t, err)
		expectIds = append(expectIds, voters...)
		assert.Equal(t, expectIds, idSlice)
	})
}

func TestBootstrap_CreateRaftConfigChangeEntries(t *testing.T) {

	//TODO: live node is LearnerNode. learner node to voting node

	t.Run("live node in configuration", func(t *testing.T) {
		walDir := os.TempDir()
		defer os.RemoveAll(walDir)

		ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
		newLocalId := uint64(2)
		func() {
			var result []raftpb.Entry
			result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
			result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(5, 4, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(6, 5, raftpb.ConfChangeAddNode, result)
			_, w, err := ws.CreateWal(babuzapb.WalMetadata{
				ClusterID:   100,
				LocalPeerID: newLocalId,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.Save(raftpb.HardState{
				Term:   1,
				Commit: 5,
			}, result))
			assert.Nil(t, w.Close())
		}()

		result, _, w, err := ws.ReplayWal(nil, true)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		st := result.HardState()

		confChangeIds, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		ents, err := createRaftConfigChangeEntries(newLocalId, "localhost:14200", confChangeIds, &st)
		assert.Nil(t, err)
		assert.Equal(t, ents[len(ents)-1].Index, st.Commit)
		assert.Equal(t, len(confChangeIds)-1, len(ents))
	})
	t.Run("removed node", func(t *testing.T) {
		walDir, err := ioutil.TempDir("", "bootstrap-wal")
		assert.Nil(t, err)
		defer os.RemoveAll(walDir)

		ws := babuzawal.NewWalManager(walDir, &logger.Mock{})
		removePeerId := uint64(1)
		func() {
			var result []raftpb.Entry
			result = genConfChangeEntry(1, 1, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(2, 1, raftpb.ConfChangeRemoveNode, result)
			result = genConfChangeEntry(3, 2, raftpb.ConfChangeAddNode, result)
			result = genConfChangeEntry(4, 3, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(5, 4, raftpb.ConfChangeAddLearnerNode, result)
			result = genConfChangeEntry(6, 5, raftpb.ConfChangeAddNode, result)
			_, w, err := ws.CreateWal(babuzapb.WalMetadata{
				ClusterID:   100,
				LocalPeerID: removePeerId,
			})
			assert.Nil(t, err)
			assert.Nil(t, w.Save(raftpb.HardState{
				Term:   1,
				Commit: 3,
			}, result))
			assert.Nil(t, w.Close())
		}()

		result, _, w, err := ws.ReplayWal(nil, true)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		st := result.HardState()

		confChangeIds, err := listRaftConfChangeAddNodeIds(nil, result)
		assert.Nil(t, err)
		ents, err := createRaftConfigChangeEntries(removePeerId, "localhost:14200", confChangeIds, &st)
		assert.Nil(t, err)
		assert.Equal(t, ents[len(ents)-1].Index, st.Commit)
		assert.Equal(t, 2, len(ents))
	})
}
