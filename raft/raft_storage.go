package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type raftStorage struct {
	snapshotor       ibabuza.SnapshotManager
	wal              ibabuza.Wal
	entryStorage     ibabuza.EntryStorage
	stateMachine     ibabuza.BaseStateMachine
	bsmInfo          *BasedStateMachineInfo
	snapshotReceiver ibabuza.AtomicSnapshotReceiver
}

func NewRaftStorage(snapshotor ibabuza.SnapshotManager, wal ibabuza.Wal,
	entryStorage ibabuza.EntryStorage, stateMachine ibabuza.BaseStateMachine,
	bsmInfo *BasedStateMachineInfo) Storage {
	return &raftStorage{
		snapshotor:   snapshotor,
		wal:          wal,
		entryStorage: entryStorage,
		stateMachine: stateMachine,
		bsmInfo:      bsmInfo,
	}
}

func (s *raftStorage) Save(hs raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) error {
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

func (s *raftStorage) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	return s.snapshotor.CreateInstalledSnapshotReader(snapshotIndex, false)
}

func (s *raftStorage) SaveStateMachineSnapshot(ctx StorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {
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

func (s *raftStorage) CreateSnapshotContext(snapshotTerm, snapshotIndex uint64, confState raftpb.ConfState,
	cluster ibabuza.Cluster, sessionMgr ibabuza.SessionManager) (StorageSnapshotContext, error) {

	smSnapCtx, err := s.prepareSnapshotContext()
	if err != nil {
		return nil, err
	}
	sw, err := s.snapshotor.CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex)
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

func (s *raftStorage) RestoreFromSnapshot(snapShotIndex uint64, restoreStateMachine bool, cluster ibabuza.Cluster, session ibabuza.SessionManager) error {
	reader, err := s.snapshotor.CreateInstalledSnapshotReader(snapShotIndex, false)
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

func (s *raftStorage) ReceiveSnapshotMessage(msg babuzapb.SnapshotMessage) (bool, error) {
	if msg.Metadata.Files != nil {
		if s.snapshotReceiver != nil {
			if err := s.snapshotReceiver.DeleteDir(); err != nil {
				return false, err
			}
		}
		snapshotReceiver, err := s.snapshotor.CreateAtomicSnapshotReceiver(msg.Metadata)
		if err != nil {
			return false, err
		}
		s.snapshotReceiver = snapshotReceiver
		return false, nil
	}
	if s.snapshotReceiver == nil {
		return false, fmt.Errorf("storage: received chunk message but snapshot receiver is nil (index=%d)", msg.Index)
	}
	if msg.ChunkMessage.ContinueCrc32 != 0 {
		if err := s.snapshotReceiver.SaveChunk(msg.Index, msg.ChunkMessage); err != nil {
			return false, err
		}
		return false, nil
	} else if msg.FinishMessage.To != 0 {
		if err := s.snapshotReceiver.Commit(msg.Index); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("storage: unexpected snapshot message")
}

func (s *raftStorage) CompactAndReleaseSnapshot(index uint64, snapshot raftpb.Snapshot) error {
	if err := s.entryStorage.Compact(index); err != nil {
		return err
	}
	return s.release(snapshot)
}

func (s *raftStorage) ApplyAndReleaseSnapshot(snapshot raftpb.Snapshot) error {
	if err := s.entryStorage.ApplySnapshot(snapshot); err != nil {
		return err
	}
	return s.release(snapshot)
}

func (s *raftStorage) EntryStorageApplySnapshot(snapshot raftpb.Snapshot) error {
	return s.entryStorage.ApplySnapshot(snapshot)
}

func (s *raftStorage) EntryStorageAppend(entries []raftpb.Entry) error {
	return s.entryStorage.Append(entries)
}
func (s *raftStorage) EntryStorageCompact(compactIndex uint64) error {
	return s.entryStorage.Compact(compactIndex)
}
func (s *raftStorage) EntryStorageInfo() (lastIndex uint64, lastTerm uint64, snapshot raftpb.Snapshot, err error) {
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

func (s *raftStorage) Close() error {
	me := multierror.New()
	me.Append(s.stateMachine.Close())
	me.Append(s.wal.Close())
	me.Append(s.snapshotor.Close())
	return me.Get()
}

func (s *raftStorage) GetStateMachineAppliedIndex() uint64 {
	return s.bsmInfo.appliedIndex
}

func (s *raftStorage) SetStateMachineAppliedIndex(index uint64) {
	s.bsmInfo.appliedIndex = index
}

func (s *raftStorage) Apply(e ibabuza.Entry) {
	s.stateMachine.Apply(e)
}

func (s *raftStorage) SupportConcurrentSnapshot() bool {
	return s.bsmInfo.supportConcurrentSnapshot
}

func (s *raftStorage) GetStateMachine() ibabuza.BaseStateMachine {
	return s.stateMachine
}

func (s *raftStorage) releaseSnapshotContext(ctx ibabuza.StateMachineSnapshotContext) error {
	if s.bsmInfo.supportConcurrentSnapshot {
		return s.stateMachine.(ibabuza.ConcurrentSnapshotStateMachine).ReleaseSnapshotContext(ctx)
	}
	return nil
}

func (s *raftStorage) prepareSnapshotContext() (ibabuza.StateMachineSnapshotContext, error) {
	if s.bsmInfo.supportConcurrentSnapshot {
		return s.stateMachine.(ibabuza.ConcurrentSnapshotStateMachine).PrepareSnapshotContext()
	}
	return nil, nil
}

func (s *raftStorage) release(snapshot raftpb.Snapshot) error {
	if err := s.wal.Purge(snapshot); err != nil {
		return err
	}
	if err := s.snapshotor.Purge(snapshot); err != nil {
		return err
	}
	return nil
}
