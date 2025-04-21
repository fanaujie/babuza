package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type ReplicaStorage interface {
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
	GetStateMachineAppliedIndex() uint64
}

type StorageManager interface {
	HasExistingWalFiles() ([]ibabuza.RaftGroupID, error)
	ScanInstalledSnapshot([]ibabuza.RaftGroupID) error
	FindSnapshotFromWal(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error)
	LoadLastValidFromSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	OpenWalAndReplay(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot,
		deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error)
	GetEntryStorage(groupID ibabuza.RaftGroupID) (ibabuza.EntryStorage, error)
	OpenStateMachine(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot) error
	RestoreFromSnapshot(groupID ibabuza.RaftGroupID, snapShotIndex uint64, restoreStateMachine bool,
		cluster ibabuza.Cluster, session ibabuza.SessionManager) error
	SetWalNoFSync(groupID ibabuza.RaftGroupID) error
	GetReplicaStorage(groupID ibabuza.RaftGroupID) (ReplicaStorage, error)
}
