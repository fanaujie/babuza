package snapshot

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"path/filepath"
	"strconv"
	"sync"
)

type MultiRaftSnapshotManager struct {
	config        Config
	fs            api.SnapshotFileSystem
	logger        ibabuza.Logger
	mu            sync.Mutex
	snapshotorMap map[ibabuza.RaftGroupID]*Snapshotor
}

func NewMultiRaftSnapshotManager(config Config, fs api.SnapshotFileSystem, logger ibabuza.Logger) *MultiRaftSnapshotManager {
	return &MultiRaftSnapshotManager{
		config:        config,
		fs:            fs,
		logger:        logger,
		snapshotorMap: make(map[ibabuza.RaftGroupID]*Snapshotor),
	}
}

func (m *MultiRaftSnapshotManager) getGroupSnapshotDir(groupID ibabuza.RaftGroupID) string {
	return filepath.Join(m.config.SnapshotDir, strconv.FormatUint(uint64(groupID), 10))
}

func (m *MultiRaftSnapshotManager) getOrCreateSnapshotor(groupID ibabuza.RaftGroupID) *Snapshotor {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshotor, ok := m.snapshotorMap[groupID]

	if ok {
		return snapshotor
	}

	groupSnapshotDir := m.getGroupSnapshotDir(groupID)
	groupConfig := Config{
		SnapshotVersion: m.config.SnapshotVersion,
		MaxSnapFiles:    m.config.MaxSnapFiles,
		SnapshotDir:     groupSnapshotDir,
	}

	snapshotor = New(groupConfig, m.fs, m.logger)
	m.snapshotorMap[groupID] = snapshotor
	return snapshotor
}

func (m *MultiRaftSnapshotManager) ScanInstalledSnapshots(groupIDs []ibabuza.RaftGroupID, removeUnfinishedSnapshotDir bool) error {
	for _, id := range groupIDs {
		if err := m.getOrCreateSnapshotor(id).ScanInstalledSnapshots(removeUnfinishedSnapshotDir); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiRaftSnapshotManager) LoadLastValidSnapshot(groupID ibabuza.RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return m.getOrCreateSnapshotor(groupID).LoadLastValidSnapshot(walSnaps)
}

func (m *MultiRaftSnapshotManager) CreateAtomicSnapshotWriter(groupID ibabuza.RaftGroupID, snapshotTerm, snapshotIndex uint64) (ibabuza.AtomicSnapshotWriter, error) {
	return m.getOrCreateSnapshotor(groupID).CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex)
}

func (m *MultiRaftSnapshotManager) CreateInstalledSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64, validateFsmFiles bool) (ibabuza.SnapshotReader, error) {
	return m.getOrCreateSnapshotor(groupID).CreateInstalledSnapshotReader(snapshotIndex, validateFsmFiles)
}

func (m *MultiRaftSnapshotManager) CreateAtomicSnapshotReceiver(groupID ibabuza.RaftGroupID, metadata babuzapb.SnapshotMetadata) (ibabuza.AtomicSnapshotReceiver, error) {
	return m.getOrCreateSnapshotor(groupID).CreateAtomicSnapshotReceiver(metadata)
}

func (m *MultiRaftSnapshotManager) Purge(groupID ibabuza.RaftGroupID, snapshot raftpb.Snapshot) error {
	return m.getOrCreateSnapshotor(groupID).Purge(snapshot)
}

func (m *MultiRaftSnapshotManager) GetGroupSnapshot(groupID ibabuza.RaftGroupID) ibabuza.SnapshotManager {
	return m.getOrCreateSnapshotor(groupID)
}

func (m *MultiRaftSnapshotManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for groupID, snapshotor := range m.snapshotorMap {
		if err := snapshotor.Close(); err != nil {
			m.logger.Errorf("failed to close snapshotor for group %d: %v", groupID, err)
			lastErr = err
		}
	}
	m.snapshotorMap = make(map[ibabuza.RaftGroupID]*Snapshotor)
	return lastErr
}
