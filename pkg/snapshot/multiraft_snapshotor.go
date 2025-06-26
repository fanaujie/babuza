package snapshot

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"path/filepath"
	"strconv"
)

type MultiRaftSnapshotManager struct {
	config         Config
	fs             api.SnapshotFileSystem
	logger         ibabuza.Logger
	purgeRequestCh chan purgeRequest
	stopCh         chan struct{}
}

func NewMultiRaftSnapshotManager(config Config, fs api.SnapshotFileSystem, logger ibabuza.Logger) *MultiRaftSnapshotManager {
	return &MultiRaftSnapshotManager{
		config:         config,
		fs:             fs,
		logger:         logger,
		purgeRequestCh: make(chan purgeRequest, 8),
		stopCh:         make(chan struct{}),
	}
}

func (m *MultiRaftSnapshotManager) getGroupSnapshotDir(groupID ibabuza.RaftGroupID) string {
	return filepath.Join(m.config.SnapshotDir, strconv.FormatUint(uint64(groupID), 10))
}

func (m *MultiRaftSnapshotManager) createSnapshotor(groupID ibabuza.RaftGroupID) *Snapshotor {
	groupSnapshotDir := m.getGroupSnapshotDir(groupID)
	groupConfig := Config{
		SnapshotVersion: m.config.SnapshotVersion,
		MaxSnapFiles:    m.config.MaxSnapFiles,
		SnapshotDir:     groupSnapshotDir,
	}
	return New(groupConfig, m.fs, m.logger, m.purgeRequestCh)
}

func (m *MultiRaftSnapshotManager) ScanInstalledSnapshots(groupIDs []ibabuza.RaftGroupID, removeUnfinishedSnapshotDir bool) (map[ibabuza.RaftGroupID]ibabuza.SnapshotManager, error) {
	if len(groupIDs) == 0 {
		return nil, errors.New("empty groupIDs")
	}
	snapshotManagers := make(map[ibabuza.RaftGroupID]ibabuza.SnapshotManager)
	for _, id := range groupIDs {
		s := m.createSnapshotor(id)
		if err := s.ScanInstalledSnapshots(removeUnfinishedSnapshotDir); err != nil {
			return nil, err
		}
		snapshotManagers[id] = s
	}
	return snapshotManagers, nil
}

func (m *MultiRaftSnapshotManager) RemoveData(groupID ibabuza.RaftGroupID) error {
	groupSnapshotDir := m.getGroupSnapshotDir(groupID)
	if m.fs.ExistDir(groupSnapshotDir) {
		if err := m.fs.RemoveDir(groupSnapshotDir); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiRaftSnapshotManager) Purger() ibabuza.SnapshotPurger {
	return &multiRaftPurger{
		MultiRaftSnapshotManager: m,
	}
}

type multiRaftPurger struct {
	*MultiRaftSnapshotManager
}

func (m *multiRaftPurger) Start() {
	go func() {
		for {
			select {
			case req := <-m.purgeRequestCh:
				if err := m.createSnapshotor(req.groupID).purgeSnapshot(req.snapshot); err != nil {
					m.logger.Errorf("failed to purge snapshot for group %d: %v", req.groupID, err)
				}
			case <-m.stopCh:
				return
			}
		}
	}()
}

func (m *MultiRaftSnapshotManager) CreateSnapshotManager(groupID ibabuza.RaftGroupID) ibabuza.SnapshotManager {

	snapshotDir := m.getGroupSnapshotDir(groupID)
	_, ok := m.fs.(*durable.SnapshotFS)
	if ok {
		if !fileutil.Exist(snapshotDir) {
			if err := fileutil.CreateDirAndTouch(snapshotDir); err != nil {
				m.logger.Panicf("failed to create snapshot dir %s: %v", snapshotDir, err)
			}
		}

	}
	return m.createSnapshotor(groupID)
}

func (m *MultiRaftSnapshotManager) Close() error {
	select {
	case <-m.stopCh:
		return nil
	default:
		close(m.stopCh)
	}
	return nil
}
