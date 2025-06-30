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
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type StorageSnapshotContext interface {
	Term() uint64
	Index() uint64
	StateMachineSnapshotContext() ibabuza.StateMachineSnapshotContext
	AtomicSnapshotWriter() ibabuza.AtomicSnapshotWriter
	ConfState() *raftpb.ConfState
}

type BootstrapStorage interface {
	ScanInstalledSnapshot() error
	FindSnapshotFromWal() ([]walpb.Snapshot, error)
	LoadLastValidFromSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	HasExistingWalFiles() (bool, error)
	CreateWal(metadata babuzapb.WalMetadata) error
	OpenWalAndReplay(snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error)
	SetWalNoFSync() error
	OpenStateMachine(snapshot *raftpb.Snapshot) error
	GetEntryStorage() (ibabuza.EntryStorage, error)
	GetApplyResultSerializer() ibabuza.ResponseSerializer
	GetRaftStorage() (RaftStorage, error)
}

type RaftStorage interface {
	Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error
	CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error
	ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error
	EntryStorageAppend(entries []raftpb.Entry) error
	EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error)
	CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
		cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (StorageSnapshotContext, error)
	SaveStateMachineSnapshot(ctx StorageSnapshotContext) (babuzapb.SnapshotMetadata, error)
	RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error
	ProcessMetadataSnapshotMessage(msg babuzapb.SnapshotMessage) error
	ProcessFinishSnapshotMessage(msg babuzapb.SnapshotMessage) error
	ProcessChunkSnapshotMessage(msg babuzapb.SnapshotMessage) error
	Apply(e ibabuza.Entry) ibabuza.ApplyResult
	CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error)
	GetStateMachine() ibabuza.BaseStateMachine
	GetBasedStateMachineInfo() *BasedStateMachineInfo
}

type InternalIdGenerator interface {
	Next() uint64
}

type InternalResultReplier interface {
	SendResult(uint64, ibabuza.ApplyResult)
	AcquireResultChan(uint64) (chan ibabuza.ApplyResult, error)
	CancelResult(uint64)
}

type InternalCompletionReplier interface {
	AcquireCompletionChan(id uint64) chan struct{}
	MarkCompleted(id uint64)
}

type InternalAppliedFacade interface {
	ApplyNilEntryInNewTerm(index, term uint64)
	ApplyNormalEntry(entry raftpb.Entry) (babuzapb.NormalRequest, ibabuza.ApplyResult, ibabuza.Session)
	ApplyConfChangeEntry(entry raftpb.Entry) (babuzapb.RequestContext, ibabuza.ApplyResult, bool)
	SendAppliedResult(replyID uint64, ar ibabuza.ApplyResult)
}
