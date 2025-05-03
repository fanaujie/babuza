package multiraft

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
	Put(job JobFunc) error
	Start() error
	Stop()
}

type StateProcessor interface {
	ProcessTick(groupID ibabuza.RaftGroupID)
	ProcessReady(groupID ibabuza.RaftGroupID)
	ProcessStep(groupID ibabuza.RaftGroupID)
	ProcessProposal(groupID ibabuza.RaftGroupID)
	ProcessConfigChange(groupID ibabuza.RaftGroupID)
	ProcessRaftStatus(groupID ibabuza.RaftGroupID)
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
	GetReplicaStorage(groupID ibabuza.RaftGroupID) (babuza.Storage, error)
}
