package raft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type bootstrapStorage struct {
	stateMachine    ibabuza.BaseStateMachine
	walManager      ibabuza.WalManager
	snapshotManager ibabuza.SnapshotManager
	entryStorage    ibabuza.EntryStorage
	wal             ibabuza.Wal
	bsmInfo         *BasedStateMachineInfo
}

func newBootstrapStorage(stateMachine ibabuza.BaseStateMachine, snapshotManager ibabuza.SnapshotManager,
	walManager ibabuza.WalManager) (*bootstrapStorage, error) {

	bsm, err := NewBasedStateMachineInfo(stateMachine)
	if err != nil {
		return nil, err
	}
	return &bootstrapStorage{
		stateMachine:    stateMachine,
		walManager:      walManager,
		snapshotManager: snapshotManager,
		bsmInfo:         bsm,
	}, nil
}

func (s *bootstrapStorage) ScanInstalledSnapshot() error {
	//TODO: verify snapshot files?
	return s.snapshotManager.ScanInstalledSnapshots(true)
}

func (s *bootstrapStorage) FindSnapshotFromWal() ([]walpb.Snapshot, error) {
	return s.walManager.FindSnapshot()
}

func (s *bootstrapStorage) LoadLastValidFromSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.snapshotManager.LoadLastValidSnapshot(walSnaps)
}

func (s *bootstrapStorage) HasExistingWalFiles() (bool, error) {
	exist, err := s.walManager.HasExistingWals()
	if err != nil {
		return false, err
	}
	return exist, nil
}

func (s *bootstrapStorage) CreateWal(metadata babuzapb.WalMetadata) error {
	entryStorage, w, err := s.walManager.CreateWal(metadata)
	if err != nil {
		return err
	}
	s.entryStorage = entryStorage
	s.wal = w
	return nil
}

func (s *bootstrapStorage) OpenWalAndReplay(snapshot *raftpb.Snapshot,
	deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
	entryStorage, wal, result, err := s.walManager.ReplayWal(snapshot, deleteUnCommittedEntries)
	if err != nil {
		return nil, err
	}
	s.entryStorage = entryStorage
	s.wal = wal
	return result, nil
}

func (s *bootstrapStorage) SetWalNoFSync() error {
	s.wal.SetUnsafeNoFsync()
	return nil
}

func (s *bootstrapStorage) OpenStateMachine(snapshot *raftpb.Snapshot) error {

	restoreStateMachine := func() error {
		reader, err := s.snapshotManager.CreateInstalledSnapshotReader(snapshot.Metadata.Index, false)
		if err != nil {
			return err
		}
		if err = s.stateMachine.RestoreFromSnapshot(reader); err != nil {
			return err
		}
		s.bsmInfo.appliedIndex = snapshot.Metadata.Index
		return nil
	}
	if s.bsmInfo.diskType {
		diskApplyIndex, rebuildStateMachine, err := s.stateMachine.(ibabuza.DiskStateMachine).Open()
		if err != nil && rebuildStateMachine == false {
			return err
		}
		if rebuildStateMachine {
			if snapshot != nil {
				if err = restoreStateMachine(); err != nil {
					return err
				}
			}
			return nil
		}
		if snapshot != nil && diskApplyIndex < snapshot.Metadata.Index {
			return fmt.Errorf("storage: on disk applied index (%d) is less than snapshot index (%d)",
				diskApplyIndex, snapshot.Metadata.Index)
		}
		s.bsmInfo.appliedIndex = diskApplyIndex
	} else {
		if snapshot != nil {
			if err := restoreStateMachine(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *bootstrapStorage) GetEntryStorage() (ibabuza.EntryStorage, error) {
	if s.entryStorage == nil {
		return nil, errors.New("storage: entry storage is nil")
	}
	return s.entryStorage, nil
}

func (s *bootstrapStorage) GetApplyResultSerializer() ibabuza.ResponseSerializer {
	if s.bsmInfo.supportSession {
		return s.stateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
	}
	return nil
}

func (s *bootstrapStorage) GetRaftStorage() (RaftStorage, error) {
	if s.wal == nil {
		return nil, errors.New("storage: wal is nil")
	}
	if s.entryStorage == nil {
		return nil, errors.New("storage: entry storage is nil")
	}
	if s.stateMachine == nil {
		return nil, errors.New("storage: state machine is nil")
	}
	return &raftStorage{
		snapshotor:   s.snapshotManager,
		wal:          s.wal,
		entryStorage: s.entryStorage,
		stateMachine: s.stateMachine,
		bsmInfo:      s.bsmInfo,
	}, nil
}
