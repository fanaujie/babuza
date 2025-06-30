// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
	"sync"
)

type MultiRaftWalManager struct {
	WalRootDir   string
	options      Options
	memPool      *allocator.ByteSlicePool
	purgerSnapCh chan walCleanupContext
	purgerStopCh chan struct{}
	once         sync.Once
	logger       ibabuza.Logger
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
		purgerSnapCh: make(chan walCleanupContext, 10),
		purgerStopCh: make(chan struct{}),
		logger:       logger,
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
	return createWalInternal(walDir, metadata, m.options, m.memPool, m.purgerSnapCh)
}

func (m *MultiRaftWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.ReplayWalResult, ibabuza.EntryStorage, ibabuza.Wal, error) {
	walDir := m.getGroupWalDir(groupID)
	return replayWalInternal(walDir, snapshot, deleteUncommitted, m.options, m.memPool, m.purgerSnapCh)
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

func (m *MultiRaftWalManager) Purger() ibabuza.WalPurger {
	return &multiRaftPurger{
		MultiRaftWalManager: m,
	}
}

func (m *MultiRaftWalManager) RemoveData(groupID ibabuza.RaftGroupID) error {
	// Remove the group's WAL directory
	walDir := m.getGroupWalDir(groupID)
	if !fileutil.Exist(walDir) {
		// If directory doesn't exist, return success (idempotent operation)
		m.logger.Infof("Group %d WAL directory does not exist, RemoveData is a no-op", groupID)
		return nil
	}
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
	p.once.Do(func() {
		go func() {
			for {
				select {
				case ctx := <-p.purgerSnapCh:
					if err := ctx.logMgr.Purge(ctx.snapshot.Metadata.Index); err != nil {
						p.logger.Errorf("wal purger failed to purge snapshot index=%d: %v", ctx.snapshot.Metadata.Index, err)
					}
				case <-p.purgerStopCh:
					return
				}
			}
		}()
	})
}
