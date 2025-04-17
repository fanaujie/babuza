package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type InternalStorageSnapshotContext interface {
	Term() uint64
	Index() uint64
	StateMachineSnapshotContext() ibabuza.StateMachineSnapshotContext
	AtomicSnapshotWriter() ibabuza.AtomicSnapshotWriter
	ConfState() *raftpb.ConfState
}

type InternalStorage interface {
	ScanInstalledSnapshot() error
	FindSnapshotFromWal() ([]walpb.Snapshot, error)
	LoadLastValidFromSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	HasExistingWalFiles() (bool, error)
	CreateWal(metadata babuzapb.WalMetadata) error
	OpenWalAndReplay(snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error)
	SetWalNoFSync() error
	Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error
	CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error
	ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error
	GetEntryStorage() (ibabuza.EntryStorage, error)
	EntryStorageAppend(entries []raftpb.Entry) error
	EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error)
	OpenStateMachine(snapshot *raftpb.Snapshot) error
	GetApplyResultSerializer() ibabuza.ResponseSerializer
	CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
		cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (InternalStorageSnapshotContext, error)
	SaveStateMachineSnapshot(ctx InternalStorageSnapshotContext) (babuzapb.SnapshotMetadata, error)
	RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error
	ReceiveSnapshotMessage(msg babuzapb.SnapshotMessage) (bool, error)
	Close() error
	GetStateMachineAppliedIndex() uint64
	SetStateMachineAppliedIndex(index uint64)
	Apply(e ibabuza.Entry)
	SupportConcurrentSnapshot() bool
	CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error)
	GetStateMachine() ibabuza.BaseStateMachine
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
	ApplyNormalEntry(e raftpb.Entry) ibabuza.Entry
	ApplyConfChangeEntry(entry raftpb.Entry) bool
	SendStateMachineAppliedResult(e *Entry, ar ibabuza.ApplyResult)
}
