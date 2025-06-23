package babuzawal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type MultiRaftWalManager struct {
	WalRootDir   string
	options      Options
	memPool      *allocator.ByteSlicePool
	logger       ibabuza.Logger
	purgerSnapCh chan purgerSnapshot
	purgerStopCh chan struct{}
	logMgrMu     struct {
		mu  sync.Mutex
		mgr map[ibabuza.RaftGroupID]*logfile.Manager
	}
}

type purgerSnapshot struct {
	groupID  ibabuza.RaftGroupID
	snapshot raftpb.Snapshot
}

var _ ibabuza.MultiRaftWalManager = (*MultiRaftWalManager)(nil)

func NewMultiRaftWalManager(walRootDir string, logger ibabuza.Logger, setOptions ...SetOptions) *MultiRaftWalManager {
	opts := defaultOptions()
	for _, opt := range setOptions {
		opt(&opts)
	}
	memPool := allocator.NewByteSlicePool(opts.WalMinEntryBufferSize, opts.WalMaxEntryBufferSize, 2)
	logger.Infof("MultiRaftWalManager: create multi-raft wal manager with walRootDir=%s", walRootDir)
	return &MultiRaftWalManager{
		WalRootDir:   walRootDir,
		options:      opts,
		memPool:      memPool,
		logger:       logger,
		purgerSnapCh: make(chan purgerSnapshot, 10),
		purgerStopCh: make(chan struct{}),
		logMgrMu: struct {
			mu  sync.Mutex
			mgr map[ibabuza.RaftGroupID]*logfile.Manager
		}{
			mgr: make(map[ibabuza.RaftGroupID]*logfile.Manager),
		},
	}
}

func (m *MultiRaftWalManager) getGroupWalDir(groupID ibabuza.RaftGroupID) string {
	return filepath.Join(m.WalRootDir, strconv.FormatUint(uint64(groupID), 10))
}

func (m *MultiRaftWalManager) FindSnapshot(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	walDir := m.getGroupWalDir(groupID)
	return findSnapshotInternal(walDir, m.memPool)
}

func (m *MultiRaftWalManager) CreateWal(groupID ibabuza.RaftGroupID, metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	walDir := m.getGroupWalDir(groupID)
	es, wal, logMgr, err := createWalInternal(walDir, metadata, m.options, m.memPool, m.createGroupPurgerChannel(groupID))
	if err != nil {
		return nil, nil, err
	}
	m.logMgrMu.mu.Lock()
	defer m.logMgrMu.mu.Unlock()
	if _, exists := m.logMgrMu.mgr[groupID]; exists {
		m.logger.Panicf("WAL for group %d already exists, overwriting", groupID)
	}
	m.logMgrMu.mgr[groupID] = logMgr
	return es, wal, nil
}

func (m *MultiRaftWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	walDir := m.getGroupWalDir(groupID)
	es, wal, logMgr, result, err := replayWalInternal(walDir, snapshot, deleteUncommitted, m.options, m.memPool, m.createGroupPurgerChannel(groupID))
	if err != nil {
		return nil, nil, nil, err
	}
	m.logMgrMu.mu.Lock()
	defer m.logMgrMu.mu.Unlock()
	if _, exists := m.logMgrMu.mgr[groupID]; exists {
		m.logger.Panicf("WAL for group %d already exists, overwriting", groupID)
	}
	m.logMgrMu.mgr[groupID] = logMgr
	return es, wal, result, nil
}

func (m *MultiRaftWalManager) HasExistingWals() ([]ibabuza.RaftGroupID, error) {
	if !fileutil.Exist(m.WalRootDir) {
		return nil, nil
	}

	files, err := os.ReadDir(m.WalRootDir)
	if err != nil {
		return nil, err
	}

	var groupIDs []ibabuza.RaftGroupID
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		groupIDStr := f.Name()
		groupID, err := strconv.ParseUint(groupIDStr, 10, 64)
		if err != nil {
			m.logger.Warningf("Found directory with invalid group ID format: %s", groupIDStr)
			continue
		}

		groupDir := filepath.Join(m.WalRootDir, groupIDStr)
		hasWal, err := hasWalFilesInDir(groupDir)
		if err != nil {
			m.logger.Warningf("Failed to check WAL files for group ID %d: %v", groupID, err)
			continue
		}

		if hasWal {
			groupIDs = append(groupIDs, ibabuza.RaftGroupID(groupID))
		}
	}

	return groupIDs, nil
}

func (m *MultiRaftWalManager) createGroupPurgerChannel(groupID ibabuza.RaftGroupID) chan raftpb.Snapshot {
	ch := make(chan raftpb.Snapshot, 1)
	go func() {
		for snap := range ch {
			select {
			case m.purgerSnapCh <- purgerSnapshot{groupID: groupID, snapshot: snap}:
			case <-m.purgerStopCh:
				return
			}
		}
	}()
	return ch
}

func (m *MultiRaftWalManager) Purger() ibabuza.WalPurger {
	return &multiRaftPurger{
		MultiRaftWalManager: m,
	}
}

func (m *MultiRaftWalManager) RemoveData(groupID ibabuza.RaftGroupID) error {
	m.logMgrMu.mu.Lock()
	defer m.logMgrMu.mu.Unlock()

	// Close the log manager for this group if it exists
	if logMgr, exists := m.logMgrMu.mgr[groupID]; exists {
		if err := logMgr.Close(); err != nil {
			m.logger.Warningf("Failed to close log manager for group %d: %v", groupID, err)
		}
		delete(m.logMgrMu.mgr, groupID)
	}

	// Remove the group's WAL directory
	walDir := m.getGroupWalDir(groupID)
	if err := os.RemoveAll(walDir); err != nil {
		return err
	}

	m.logger.Infof("Successfully removed WAL data for group %d", groupID)
	return nil
}

func (m *MultiRaftWalManager) Close() error {
	select {
	case <-m.purgerStopCh:
	default:
		close(m.purgerStopCh)
	}
	return nil
}

type multiRaftPurger struct {
	*MultiRaftWalManager
}

func (p *multiRaftPurger) Start() {
	go func() {
		for {
			select {
			case purgerSnap := <-p.purgerSnapCh:
				p.logMgrMu.mu.Lock()
				logMgr, exists := p.logMgrMu.mgr[purgerSnap.groupID]
				p.logMgrMu.mu.Unlock()

				if !exists {
					p.logger.Warningf("failed to purge snapshot for group %d: log manager not found", purgerSnap.groupID)
					continue
				}

				if err := logMgr.Purge(purgerSnap.snapshot.Metadata.Index); err != nil {
					p.logger.Errorf("failed to purge snapshot for group %d index=%d: %v", purgerSnap.groupID, purgerSnap.snapshot.Metadata.Index, err)
				}
			case <-p.purgerStopCh:
				return
			}
		}
	}()
}
