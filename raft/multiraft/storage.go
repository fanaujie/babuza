package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type replicaStorage interface {
	Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error
	ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error
	EntryStorageAppend(entries []raftpb.Entry) error
	CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
		cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (babuza.InternalStorageSnapshotContext, error)
	Apply(e ibabuza.Entry)
	RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error
	SetStateMachineAppliedIndex(index uint64)
	SaveStateMachineSnapshot(ctx babuza.InternalStorageSnapshotContext) (babuzapb.SnapshotMetadata, error)
	CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error
	SupportConcurrentSnapshot() bool
}

type StorageManager interface{}
