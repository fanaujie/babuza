package babuzawal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"os"
	"path/filepath"
	"strconv"
)

type MultiRaftWalManager struct {
	WalRootDir string
	options    Options
	memPool    *allocator.ByteSlicePool
	logger     ibabuza.Logger
}

func NewMultiRaftWalManager(walRootDir string, logger ibabuza.Logger, setOptions ...SetOptions) *MultiRaftWalManager {
	opts := defaultOptions()
	for _, opt := range setOptions {
		opt(&opts)
	}
	memPool := allocator.NewByteSlicePool(opts.WalMinEntryBufferSize, opts.WalMaxEntryBufferSize, 2)
	logger.Infof("MultiRaftWalManager: create multi-raft wal manager with walRootDir=%s", walRootDir)
	return &MultiRaftWalManager{
		WalRootDir: walRootDir,
		options:    opts,
		memPool:    memPool,
		logger:     logger,
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
	return createWalInternal(walDir, metadata, m.options, m.memPool)
}

func (m *MultiRaftWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	walDir := m.getGroupWalDir(groupID)
	return replayWalInternal(walDir, snapshot, deleteUncommitted, m.options, m.memPool)
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

func (m *MultiRaftWalManager) PurgeWals(config ibabuza.WalPurgeConfig) {

}
