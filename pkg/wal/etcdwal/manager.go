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
)

type WalManager struct {
	walDir string
	wal    *wal.WAL
	logger *zap.Logger
}

var _ ibabuza.WalManager = (*WalManager)(nil)

func NewWalManager(walDir string, logger *zap.Logger) *WalManager {
	return &WalManager{
		walDir: walDir,
		logger: logger,
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

func (e *WalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {

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
	return m, NewWalWrapper(w), result, nil
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

// StartWalPurgingProcess is to release the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (e *WalManager) PurgeWals(purgeCfg ibabuza.WalPurgeConfig) {
	//TODO: add stop purging process
	if purgeCfg.MaxKeepWalFiles > 0 {
		go func() {
			var errCh <-chan error
			var doneCh <-chan struct{}
			doneCh, errCh = etcdfileutil.PurgeFileWithDoneNotify(e.logger, purgeCfg.WalDir, "wal", purgeCfg.MaxKeepWalFiles,
				purgeCfg.PurgeFileInterval, purgeCfg.StopCh)
			select {
			case _ = <-errCh:
				return
			case <-doneCh:
				return
			}
		}()
	}
}

func (e *WalManager) Close() error {
	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}
