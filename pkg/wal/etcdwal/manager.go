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


package etcdwal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	etcdfileutil "go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"time"
)

type Options struct {
	MaxKeepWalFiles   uint
	PurgeFileInterval time.Duration
}

type SetOptions func(*Options)

func SetOptionWithMaxKeepWalFiles(maxKeepWalFiles uint) SetOptions {
	return func(cfg *Options) {
		cfg.MaxKeepWalFiles = maxKeepWalFiles
	}
}

func SetOptionWithPurgeFileInterval(purgeFileInterval time.Duration) SetOptions {
	return func(cfg *Options) {
		cfg.PurgeFileInterval = purgeFileInterval
	}
}

func defaultWalPurgeConfig() *Options {
	return &Options{
		MaxKeepWalFiles:   3,
		PurgeFileInterval: 30 * time.Second,
	}
}

type WalManager struct {
	walDir       string
	config       *Options
	wal          *wal.WAL
	logger       *zap.Logger
	purgerStopCh chan struct{}
}

var _ ibabuza.WalManager = (*WalManager)(nil)

func NewWalManager(walDir string, logger *zap.Logger, opts ...SetOptions) *WalManager {
	config := defaultWalPurgeConfig()
	for _, opt := range opts {
		opt(config)
	}
	return &WalManager{
		walDir:       walDir,
		config:       config,
		logger:       logger,
		purgerStopCh: make(chan struct{}),
	}
}

func (e *WalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	return wal.ValidSnapshotEntries(e.logger, e.walDir)
}

func (e *WalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	mData, err := metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}
	w, err := wal.Create(e.logger, e.walDir, mData)
	if err != nil {
		return nil, nil, err
	}
	wrapper := WalWrapper{WAL: w}
	e.wal = w
	return raft.NewMemoryStorage(), &wrapper, nil
}

func (e *WalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.ReplayWalResult, ibabuza.EntryStorage, ibabuza.Wal, error) {

	repaired := false
	var walSnap walpb.Snapshot
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	var err error
	var result *walbase.ReplayResult
	var w *wal.WAL
	for {
		w, err = wal.Open(e.logger, e.walDir, walSnap)
		if err != nil {
			return nil, nil, nil, err
		}
		metadata, hardState, entries, err := w.ReadAll()
		if err != nil {
			w.Close()
			if repaired || err != io.ErrUnexpectedEOF {
				return nil, nil, nil, err
			}
			if !wal.Repair(e.logger, e.walDir) {
				return nil, nil, nil, err
			} else {
				repaired = true
			}
			continue
		}
		result = walbase.NewReplayResult(metadata, hardState, entries)
		break
	}
	m := raft.NewMemoryStorage()
	if snapshot != nil {
		m.ApplySnapshot(*snapshot)
	}
	m.SetHardState(result.HardState())
	if deleteUncommitted {
		if err = result.DeleteUncommittedEntry(result.HardState().Commit); err != nil {
			return nil, nil, nil, err
		}
	}
	if err = m.Append(result.GetEntries()); err != nil {
		return nil, nil, nil, err
	}
	e.wal = w
	return result, m, NewWalWrapper(w), nil
}

func (e *WalManager) HasExistingWals() (bool, error) {
	if fileutil.Exist(e.walDir) == false {
		return false, nil
	}
	files, err := os.ReadDir(e.walDir)
	if err != nil {
		return false, err
	}
	for _, f := range files {
		name := f.Name()
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(name, ".wal") {
			return true, nil
		}
	}
	return false, nil
}

func (e *WalManager) Purger() ibabuza.WalPurger {
	return &purger{
		walDir: e.walDir,
		config: e.config,
		logger: e.logger,
		stopCh: e.purgerStopCh,
	}
}

func (e *WalManager) Close() error {
	if e.wal != nil {
		return e.wal.Close()
	}
	select {
	case <-e.purgerStopCh:
	default:
		close(e.purgerStopCh)
	}
	return nil
}

type purger struct {
	walDir string
	config *Options
	logger *zap.Logger
	stopCh chan struct{}
}

// Start is to release the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (p *purger) Start() {
	if p.config.MaxKeepWalFiles > 0 {
		go func() {
			var errCh <-chan error
			var doneCh <-chan struct{}
			doneCh, errCh = etcdfileutil.PurgeFileWithDoneNotify(p.logger, p.walDir, "wal", p.config.MaxKeepWalFiles,
				p.config.PurgeFileInterval, p.stopCh)
			select {
			case _ = <-errCh:
				return
			case <-doneCh:
				return
			case <-p.stopCh:
				return
			}
		}()
	}
}

func (p *purger) Stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}
