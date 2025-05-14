package multiraft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"sync"
)

type stateMachineFactory interface {
	CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error)
}

type raftStateMachineWrapper struct {
	stateMachine ibabuza.BaseStateMachine
	bsmInfo      *babuza.BasedStateMachineInfo
}

type bootstrapStorage struct {
	stateMachineRootDir      string
	stateMachineFactory      stateMachineFactory
	walManager               ibabuza.MultiRaftWalManager
	snapshotManager          ibabuza.MultiRaftSnapshotManager
	logger                   ibabuza.Logger
	mu                       sync.RWMutex
	entryStorage             map[ibabuza.RaftGroupID]ibabuza.EntryStorage
	wal                      map[ibabuza.RaftGroupID]ibabuza.Wal
	raftStateMachineWrappers map[ibabuza.RaftGroupID]raftStateMachineWrapper
}

func newBootstrapStorage(stateMachineRootDir string, stateMachineFactory stateMachineFactory,
	walManager ibabuza.MultiRaftWalManager, snapManager ibabuza.MultiRaftSnapshotManager, logger ibabuza.Logger) BootstrapStorage {
	return &bootstrapStorage{
		stateMachineRootDir:      stateMachineRootDir,
		stateMachineFactory:      stateMachineFactory,
		walManager:               walManager,
		snapshotManager:          snapManager,
		logger:                   logger,
		entryStorage:             make(map[ibabuza.RaftGroupID]ibabuza.EntryStorage),
		wal:                      make(map[ibabuza.RaftGroupID]ibabuza.Wal),
		raftStateMachineWrappers: make(map[ibabuza.RaftGroupID]raftStateMachineWrapper),
	}
}

func (s *bootstrapStorage) HasExistingWalFiles() ([]ibabuza.RaftGroupID, error) {
	return s.walManager.HasExistingWals()
}

func (s *bootstrapStorage) ScanInstalledSnapshot(ids []ibabuza.RaftGroupID) error {
	return s.snapshotManager.ScanInstalledSnapshots(ids, true)
}

func (s *bootstrapStorage) FindSnapshotFromWal(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	return s.walManager.FindSnapshot(groupID)
}

func (s *bootstrapStorage) LoadLastValidFromSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.snapshotManager.LoadLastValidSnapshot(groupID, walSnaps)
}

func (s *bootstrapStorage) OpenWalAndReplay(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
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

func (s *bootstrapStorage) CreateWal(groupID ibabuza.RaftGroupID, metadata babuzapb.WalMetadata) error {
	entryStorage, w, err := s.walManager.CreateWal(groupID, metadata)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entryStorage[groupID] = entryStorage
	s.wal[groupID] = w
	return nil
}

func (s *bootstrapStorage) GetEntryStorage(groupID ibabuza.RaftGroupID) (ibabuza.EntryStorage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if entryStorage, ok := s.entryStorage[groupID]; ok {
		return entryStorage, nil
	}
	return nil, errors.Errorf("entry storage not found for groupID %v", groupID)
}

func (s *bootstrapStorage) CreateStateMachine(groupID ibabuza.RaftGroupID) (ibabuza.ResponseSerializer, error) {
	stateMachine, bsmInfo, responseSerializer, err := s.createStateMachineInternal(groupID)
	if err != nil {
		return nil, err
	}

	s.raftStateMachineWrappers[groupID] = raftStateMachineWrapper{
		stateMachine: stateMachine,
		bsmInfo:      bsmInfo,
	}
	return responseSerializer, nil
}

func (s *bootstrapStorage) OpenStateMachine(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot) (ibabuza.ResponseSerializer, error) {
	stateMachine, bsmInfo, responseSerializer, err := s.createStateMachineInternal(groupID)
	if err != nil {
		return nil, err
	}

	restoreStateMachine := func() error {
		reader, err := s.snapshotManager.CreateInstalledSnapshotReader(groupID, snapshot.Metadata.Index, false)
		if err != nil {
			return err
		}
		if err = stateMachine.RestoreFromSnapshot(reader); err != nil {
			return err
		}
		bsmInfo.SetOpenAppliedIndex(snapshot.Metadata.Index)
		return nil
	}

	if bsmInfo.IsDiskType() {
		diskAppliedIndex, rebuildStateMachine := stateMachine.(ibabuza.DiskStateMachine).Open()
		if rebuildStateMachine {
			if snapshot != nil {
				if err = restoreStateMachine(); err != nil {
					return nil, err
				}
			}
			s.raftStateMachineWrappers[groupID] = raftStateMachineWrapper{
				stateMachine: stateMachine,
				bsmInfo:      bsmInfo,
			}
			bsmInfo.SetOpenAppliedIndex(diskAppliedIndex)
			return responseSerializer, nil
		}
		if snapshot != nil && diskAppliedIndex < snapshot.Metadata.Index {
			return nil, fmt.Errorf("storage: on disk applied index (%d) is less than snapshot index (%d)",
				diskAppliedIndex, snapshot.Metadata.Index)
		}
		// open state machine is successful
		bsmInfo.SetOpenAppliedIndex(diskAppliedIndex)
	} else {
		if snapshot != nil {
			if err = restoreStateMachine(); err != nil {
				return nil, err
			}
		}
	}
	s.raftStateMachineWrappers[groupID] = raftStateMachineWrapper{
		stateMachine: stateMachine,
		bsmInfo:      bsmInfo,
	}
	return responseSerializer, nil
}

func (s *bootstrapStorage) SetWalNoFSync(groupID ibabuza.RaftGroupID) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if wal, ok := s.wal[groupID]; ok {
		wal.SetUnsafeNoFsync()
		return nil
	}
	return errors.Errorf("wal not found for groupID %v", groupID)
}

func (s *bootstrapStorage) GetReplicaStorage(groupID ibabuza.RaftGroupID) (babuza.RaftStorage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshotManager := s.snapshotManager.GetGroupSnapshot(groupID)

	entryStorage, ok := s.entryStorage[groupID]
	if !ok {
		return nil, errors.Errorf("entry storage not found for groupID %v", groupID)
	}

	wal, ok := s.wal[groupID]
	if !ok {
		return nil, errors.Errorf("wal not found for groupID %v", groupID)
	}

	wrapper, ok := s.raftStateMachineWrappers[groupID]
	if !ok {
		return nil, errors.Errorf("state machine not found for groupID %v", groupID)
	}

	return babuza.NewRaftStorage(snapshotManager, wal, entryStorage, wrapper.stateMachine, wrapper.bsmInfo), nil
}

func (s *bootstrapStorage) Close() error {
	m := multierror.New()
	m.Append(s.walManager.Close())
	m.Append(s.snapshotManager.Close())
	return m.Get()
}

func (s *bootstrapStorage) createStateMachineInternal(groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, *babuza.BasedStateMachineInfo, ibabuza.ResponseSerializer, error) {
	stateMachine, err := s.stateMachineFactory.CreateStateMachine(s.stateMachineRootDir, groupID)
	if err != nil {
		return nil, nil, nil, err
	}

	bsmInfo, err := babuza.NewBasedStateMachineInfo(stateMachine)
	if err != nil {
		return nil, nil, nil, err
	}

	var responseSerializer ibabuza.ResponseSerializer
	if bsmInfo.SupportSession() {
		responseSerializer = stateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.raftStateMachineWrappers[groupID]
	if ok {
		return nil, nil, nil, errors.Errorf("groupID %v statemachine already exists", groupID)
	}

	return stateMachine, bsmInfo, responseSerializer, nil
}
