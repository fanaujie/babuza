package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type storageManager struct {
}

func (s *storageManager) HasExistingWalFiles() ([]ibabuza.RaftGroupID, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) ScanInstalledSnapshot(ids []ibabuza.RaftGroupID) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) FindSnapshotFromWal(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) LoadLastValidFromSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) OpenWalAndReplay(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) GetEntryStorage(groupID ibabuza.RaftGroupID) (ibabuza.EntryStorage, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) OpenStateMachine(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) RestoreFromSnapshot(groupID ibabuza.RaftGroupID, snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) SetWalNoFSync(groupID ibabuza.RaftGroupID) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) GetReplicaStorage(groupID ibabuza.RaftGroupID) (ReplicaStorage, error) {
	//TODO implement me
	panic("implement me")
}
