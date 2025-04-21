package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"sync"
)

type storageManager struct {
	walManager ibabuza.MultiRaftWalManager

	mu           sync.RWMutex
	entryStorage map[ibabuza.RaftGroupID]ibabuza.EntryStorage
	wal          map[ibabuza.RaftGroupID]ibabuza.Wal
}

func NewStorageManager(walManager ibabuza.MultiRaftWalManager) StorageManager {
	return &storageManager{
		walManager: walManager,
	}
}

func (s *storageManager) HasExistingWalFiles() ([]ibabuza.RaftGroupID, error) {
	return s.walManager.HasExistingWals()
}

func (s *storageManager) ScanInstalledSnapshot(ids []ibabuza.RaftGroupID) error {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) FindSnapshotFromWal(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	return s.walManager.FindSnapshot(groupID)
}

func (s *storageManager) LoadLastValidFromSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (s *storageManager) OpenWalAndReplay(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
	entryStorage, wal, result, err := s.walManager.ReplayWal(groupID, snapshot, deleteUnCommittedEntries)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entryStorage[groupID] = entryStorage
	s.wal[groupID] = wal
	return result, nil
}

func (s *storageManager) GetEntryStorage(groupID ibabuza.RaftGroupID) (ibabuza.EntryStorage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if entryStorage, ok := s.entryStorage[groupID]; ok {
		return entryStorage, nil
	}
	return nil, errors.Errorf("entry storage not found for groupID %v", groupID)
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	if wal, ok := s.wal[groupID]; ok {
		wal.SetUnsafeNoFsync()
		return nil
	}
	return errors.Errorf("wal not found for groupID %v", groupID)
}

func (s *storageManager) GetReplicaStorage(groupID ibabuza.RaftGroupID) (ReplicaStorage, error) {
	//TODO implement me
	panic("implement me")
}
