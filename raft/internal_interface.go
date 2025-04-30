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
	GetRaftStorage() (Storage, error)
}

type Storage interface {
	Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error
	CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error
	ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error
	EntryStorageAppend(entries []raftpb.Entry) error
	EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error)
	CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
		cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (StorageSnapshotContext, error)
	SaveStateMachineSnapshot(ctx StorageSnapshotContext) (babuzapb.SnapshotMetadata, error)
	RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error
	ReceiveSnapshotMessage(msg babuzapb.SnapshotMessage) (bool, error)

	GetStateMachineAppliedIndex() uint64
	SetStateMachineAppliedIndex(index uint64)
	Apply(e ibabuza.Entry) ibabuza.ApplyResult
	SupportConcurrentSnapshot() bool
	CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error)
	GetStateMachine() ibabuza.BaseStateMachine
	Close() error
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
	ApplyNormalEntry(entry raftpb.Entry) (babuzapb.NormalRequest, ibabuza.ApplyResult)
	ApplyConfChangeEntry(entry raftpb.Entry) (babuzapb.RequestContext, ibabuza.ApplyResult, bool)
	SendAppliedResult(replyID uint64, ar ibabuza.ApplyResult)
}
