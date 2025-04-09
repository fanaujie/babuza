package raft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type storageManager struct {
	stateMachine     ibabuza.BaseStateMachine
	walManager       ibabuza.WalManager
	snapshotManager  ibabuza.SnapshotManager
	entryStorage     ibabuza.EntryStorage
	wal              ibabuza.Wal
	snapshotReceiver ibabuza.AtomicSnapshotReceiver
	bsmInfo          basedStateMachineInfo
}

func newStorageManager(stateMachine ibabuza.BaseStateMachine, snapshotManager ibabuza.SnapshotManager, walManager ibabuza.WalManager) (*storageManager, error) {
	bsm, err := newBasedStateMachineInfo(stateMachine)
	if err != nil {
		return nil, err
	}
	return &storageManager{
		stateMachine:    stateMachine,
		walManager:      walManager,
		snapshotManager: snapshotManager,
		bsmInfo:         bsm,
	}, nil
}

func (s *storageManager) ScanInstalledSnapshot() error {
	//TODO: verify snapshot files?
	return s.snapshotManager.ScanInstalledSnapshots(true)
}

func (s *storageManager) FindSnapshotFromWal() ([]walpb.Snapshot, error) {
	return s.walManager.FindSnapshot()
}

func (s *storageManager) LoadLastValidFromSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.snapshotManager.LoadLastValidSnapshot(walSnaps)
}

func (s *storageManager) HasExistingWalFiles() (bool, error) {
	exist, err := s.walManager.HasExistingWals()
	if err != nil {
		return false, err
	}
	return exist, nil
}

func (s *storageManager) CreateWal(metadata babuzapb.WalMetadata) error {
	entryStorage, w, err := s.walManager.CreateWal(metadata)
	if err != nil {
		return err
	}
	s.entryStorage = entryStorage
	s.wal = w
	return nil
}

func (s *storageManager) OpenWalAndReplay(snapshot *raftpb.Snapshot,
	deleteUnCommittedEntries bool) (ibabuza.ReplayWalResult, error) {
	entryStorage, wal, result, err := s.walManager.ReplayWal(snapshot, deleteUnCommittedEntries)
	if err != nil {
		return nil, err
	}
	s.entryStorage = entryStorage
	s.wal = wal
	return result, nil
}

func (s *storageManager) SetWalNoFSync() error {
	s.wal.SetUnsafeNoFsync()
	return nil
}

func (s *storageManager) Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error {
	//TODO: test. See https://github.com/etcd-io/etcd/issues/10219 for more details.
	if err := s.wal.Save(hs, entries); err != nil {
		return err
	}
	if !raft.IsEmptySnap(snapshot) {
		if err := s.wal.SaveSnapshot(snapshot); err != nil {
			return err
		}
		if err := s.wal.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageManager) GetEntryStorage() (ibabuza.EntryStorage, error) {
	if s.entryStorage == nil {
		return nil, errors.New("storage: entry storage is nil")
	}
	return s.entryStorage, nil
}

func (s *storageManager) GetApplyResultSerializer() ibabuza.ResponseSerializer {
	if s.bsmInfo.supportSession {
		return s.stateMachine.(ibabuza.SessionEnabledStateMachine).GetResponseSerializer()
	}
	return nil
}

func (s *storageManager) OpenStateMachine(snapshot *raftpb.Snapshot) error {

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

func (s *storageManager) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	return s.snapshotManager.CreateInstalledSnapshotReader(snapshotIndex, false)
}

func (s *storageManager) SaveStateMachineSnapshot(ctx InternalStorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {
	me := multierror.New()
	var err error
	sw := ctx.AtomicSnapshotWriter()
	defer func() {
		me.Append(s.releaseSnapshotContext(ctx.StateMachineSnapshotContext()))
		err = me.Get()
	}()
	if err = s.stateMachine.SaveSnapshot(ctx.StateMachineSnapshotContext(), ctx.AtomicSnapshotWriter()); err != nil {
		me.Append(err)
		return babuzapb.SnapshotMetadata{}, err
	}
	snap, err := s.entryStorage.CreateSnapshot(ctx.Index(), ctx.ConfState(), nil)
	if err != nil {
		me.Append(err)
		return babuzapb.SnapshotMetadata{}, err
	}
	metadata, err := sw.Commit(snap)
	if err != nil {
		me.Append(err)
		return babuzapb.SnapshotMetadata{}, err
	}
	return metadata, err
}

func (s *storageManager) CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
	cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (InternalStorageSnapshotContext, error) {

	smSnapCtx, err := s.prepareSnapshotContext()
	if err != nil {
		return nil, err
	}
	sw, err := s.snapshotManager.CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex)
	if err != nil {
		return nil, err
	}
	clusterWriter, err := sw.CreateClusterFile(babuzapb.SnapshotFileCompression_None)
	if err != nil {
		return nil, err
	}
	if err = cluster.Snapshot(clusterWriter); err != nil {
		return nil, err
	}
	if err = clusterWriter.Close(); err != nil {
		return nil, err
	}
	sessionWriter, err := sw.CreateSessionFile(babuzapb.SnapshotFileCompression_None)
	if err != nil {
		return nil, err
	}
	if err = sessionMgr.Snapshot(sessionWriter); err != nil {
		return nil, err
	}
	if err = sessionWriter.Close(); err != nil {
		return nil, err
	}
	return &snapshotContext{
		term:                snapshotTerm,
		index:               snapshotIndex,
		snapWr:              sw,
		confState:           confState,
		stateMachineContext: smSnapCtx,
	}, nil

}

func (s *storageManager) RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error {
	reader, err := s.snapshotManager.CreateInstalledSnapshotReader(snapShotIndex, false)
	if err != nil {
		return err
	}
	defer reader.Close()
	if restoreStateMachine {
		if err = s.stateMachine.RestoreFromSnapshot(reader); err != nil {
			return err
		}
	}
	clusterReader, err := reader.Cluster()
	if err != nil {
		return err
	}
	if err = cluster.Restore(clusterReader); err != nil {
		return err
	}
	sessionReader, err := reader.Session()
	if err != nil {
		return err
	}
	return session.Restore(sessionReader)
}

func (s *storageManager) ReceiveSnapshotMessage(msg babuzapb.SnapshotMessage) (bool, error) {
	if msg.Metadata != nil {
		if s.snapshotReceiver != nil {
			if err := s.snapshotReceiver.DeleteDir(); err != nil {
				return false, err
			}
		}
		snapshotReceiver, err := s.snapshotManager.CreateAtomicSnapshotReceiver(*msg.Metadata)
		if err != nil {
			return false, err
		}
		s.snapshotReceiver = snapshotReceiver
		return false, nil
	}
	if s.snapshotReceiver == nil {
		return false, fmt.Errorf("storage: received chunk message but snapshot receiver is nil (index=%d)", msg.Index)
	}
	if msg.ChunkMessage != nil {
		if err := s.snapshotReceiver.SaveChunk(msg.Index, *msg.ChunkMessage); err != nil {
			return false, err
		}
		return false, nil
	} else if msg.FinishMessage != nil {
		if err := s.snapshotReceiver.Commit(msg.Index); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("storage: unexpected snapshot message")
}

func (s *storageManager) CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error {
	if err := s.entryStorage.Compact(index); err != nil {
		return err
	}
	return s.release(snapshot)
}

func (s *storageManager) ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error {
	if err := s.entryStorage.ApplySnapshot(snapshot); err != nil {
		return err
	}
	return s.release(snapshot)
}

func (s *storageManager) EntryStorageApplySnapshot(snapshot raftpb.Snapshot) error {
	return s.entryStorage.ApplySnapshot(snapshot)
}

func (s *storageManager) EntryStorageAppend(entries []raftpb.Entry) error {
	return s.entryStorage.Append(entries)
}
func (s *storageManager) EntryStorageCompact(compactIndex uint64) error {
	return s.entryStorage.Compact(compactIndex)
}
func (s *storageManager) EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error) {
	lastIndex, err = s.entryStorage.LastIndex()
	if err != nil {
		return 0, 0, raftpb.Snapshot{}, err
	}
	lastTerm, err = s.entryStorage.Term(lastIndex)
	if err != nil {
		return 0, 0, raftpb.Snapshot{}, err
	}
	snapshot, err = s.entryStorage.Snapshot()
	if err != nil {
		return 0, 0, raftpb.Snapshot{}, err
	}
	return
}

func (s *storageManager) Close() error {
	me := multierror.New()
	me.Append(s.stateMachine.Close())
	me.Append(s.wal.Close())
	me.Append(s.snapshotManager.Close())
	return me.Get()
}

func (s *storageManager) GetStateMachineAppliedIndex() uint64 {
	return s.bsmInfo.appliedIndex
}

func (s *storageManager) SetStateMachineAppliedIndex(index uint64) {
	s.bsmInfo.appliedIndex = index
}

func (s *storageManager) Apply(e ibabuza.Entry) {
	s.stateMachine.Apply(e)
}

func (s *storageManager) SupportConcurrentSnapshot() bool {
	return s.bsmInfo.supportConcurrentSnapshot
}

func (s *storageManager) GetStateMachine() ibabuza.BaseStateMachine {
	return s.stateMachine
}

func (s *storageManager) releaseSnapshotContext(ctx ibabuza.StateMachineSnapshotContext) error {
	if s.bsmInfo.supportConcurrentSnapshot {
		return s.stateMachine.(ibabuza.ConcurrentSnapshotStateMachine).ReleaseSnapshotContext(ctx)
	}
	return nil
}

func (s *storageManager) prepareSnapshotContext() (ibabuza.StateMachineSnapshotContext, error) {
	if s.bsmInfo.supportConcurrentSnapshot {
		return s.stateMachine.(ibabuza.ConcurrentSnapshotStateMachine).PrepareSnapshotContext()
	}
	return nil, nil
}

func (s *storageManager) release(snapshot raftpb.Snapshot) error {
	if err := s.wal.Purge(snapshot); err != nil {
		return err
	}
	if err := s.snapshotManager.Purge(snapshot); err != nil {
		return err
	}
	return nil
}
