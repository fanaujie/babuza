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


package experimental

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type Scheduler interface {
	EnqueueBatchState(state int, groupIDs []ibabuza.RaftGroupID)
	EnqueueState(state int, groupID ibabuza.RaftGroupID)
	Start() error
	Stop()
}

type JobFunc func()

type JobQueue interface {
	Put(groupID ibabuza.RaftGroupID, job JobFunc) error
	Start() error
	Stop()
}

type StateProcessor interface {
	ProcessTick(groupID ibabuza.RaftGroupID)
	ProcessReady(groupID ibabuza.RaftGroupID)
	ProcessStep(groupID ibabuza.RaftGroupID)
	ProcessProposal(groupID ibabuza.RaftGroupID)
	ProcessConfigChange(groupID ibabuza.RaftGroupID)
}

type BootstrapStorage interface {
	HasExistingWalFiles() ([]ibabuza.RaftGroupID, error)
	ScanInstalledSnapshot([]ibabuza.RaftGroupID) error
	FindSnapshotFromWal(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error)
	LoadLastValidFromSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	OpenWalAndReplay(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error)
	CreateWal(groupID ibabuza.RaftGroupID, metadata babuzapb.WalMetadata) error
	GetEntryStorage(groupID ibabuza.RaftGroupID) (ibabuza.EntryStorage, error)
	CreateStateMachine(groupID ibabuza.RaftGroupID) (ibabuza.ResponseSerializer, error)
	OpenStateMachine(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot) (ibabuza.ResponseSerializer, error)
	SetWalNoFSync(groupID ibabuza.RaftGroupID) error
	CreateReplicaStorage(groupID ibabuza.RaftGroupID) (babuza.RaftStorage, error)
	StartPurgingProcess()
	RemoveData(groupID ibabuza.RaftGroupID) error
	Close() error
}
