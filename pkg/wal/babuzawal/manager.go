package babuzawal

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/collection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrystore"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/player"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"os"
	"strings"
)

type WalManager struct {
	walDir  string
	options Options
	cascade *allocator.TwoLevelPool
	logger  ibabuza.Logger
}

type Options struct {
	WalLogFileChunkSize          int
	WalAlignmentPageSize         int
	WalPageWriteBufferSize       int
	WalFixedEntryBufferSize      int
	WalMaxDynamicEntryBufferSize int
	WalMaxKeepLogFiles           uint
	DisableEntryIndex            bool
}

func defaultOptions() Options {
	return Options{
		WalLogFileChunkSize:          64 * 1000 * 1000,
		WalAlignmentPageSize:         4096,
		WalPageWriteBufferSize:       4096 * 32,
		WalFixedEntryBufferSize:      1024 * 1024,
		WalMaxDynamicEntryBufferSize: 4 * 1024 * 1024,
		WalMaxKeepLogFiles:           5,
		DisableEntryIndex:            false,
	}
}

type SetOptions func(opt *Options)

func SetOptsWithWalLogFileChunkSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalLogFileChunkSize = d
	}
}

func SetOptsWithWalAlignmentPageSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalAlignmentPageSize = d
	}
}

func SetOptsWithWalPageWriteBufferSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalPageWriteBufferSize = d
	}
}

func SetOptsWithWalFixedEntryBufferSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalFixedEntryBufferSize = d
	}
}

func SetOptsWithWalMaxDynamicEntryBufferSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalMaxDynamicEntryBufferSize = d
	}
}

func SetOptsWithWalMaxKeepLogFiles(d uint) SetOptions {
	return func(opt *Options) {
		opt.WalMaxKeepLogFiles = d
	}
}

func SetOptsWithWalDisableEntryIndex(d bool) SetOptions {
	return func(opt *Options) {
		opt.DisableEntryIndex = d
	}
}

func NewWalManager(walDir string, logger ibabuza.Logger, setOptions ...SetOptions) ibabuza.WalManager {
	opts := defaultOptions()
	for _, s := range setOptions {
		s(&opts)
	}
	logger.Infof("BabuzaWalManager: create wal manager with walDir=%s", walDir)
	return &WalManager{
		walDir:  walDir,
		options: opts,
		cascade: allocator.NewDefaultTwoLevelPool(opts.WalFixedEntryBufferSize, opts.WalMaxDynamicEntryBufferSize),
		logger:  logger,
	}
}

func (w *WalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	result := player.NewReplayResult(collection.NewNopEntry())
	p, err := player.Create(w.walDir, EmptyWalpbSnapshot, w.cascade)
	if err != nil {
		return nil, err
	}
	if err = p.Replay(result, false); err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
	}
	hs := result.HardState()
	var walSnapshots []walpb.Snapshot
	for _, s := range result.WalSnapshots() {
		if s.Index <= hs.Commit {
			walSnapshots = append(walSnapshots, s)
		}
	}
	return walSnapshots, nil
}

func (w *WalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	if fileutil.Exist(w.walDir) {
		if err := os.Remove(w.walDir); err != nil {
			return nil, nil, err
		}
	}
	if err := fileutil.CreateDirAndTouch(w.walDir); err != nil {
		return nil, nil, err
	}
	md, err := metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}
	logMgr, err := logfile.NewManager(logfile.ManagerConfig{
		WalDir:            w.walDir,
		LogFileChunkSize:  w.options.WalLogFileChunkSize,
		AlignmentPageSize: w.options.WalAlignmentPageSize,
		PageWriterBufSize: w.options.WalPageWriteBufferSize,
		MaxKeepLogFiles:   w.options.WalMaxKeepLogFiles,
	}, w.cascade)
	if err != nil {
		return nil, nil, err
	}
	wal, err := CreateWal(md, logMgr)
	if err != nil {
		return nil, nil, err
	}
	var entryStorage ibabuza.EntryStorage
	if !w.options.DisableEntryIndex {
		em := entrystore.NewStorage(logMgr)
		wal.SetEntryIndexStorage(em)
		entryStorage = em
	} else {
		entryStorage = raft.NewMemoryStorage()
	}
	return entryStorage, wal, nil
}

func (w *WalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	walSnap := EmptyWalpbSnapshot
	if snapshot != nil {
		walSnap = walpb.Snapshot{
			Index:     snapshot.Metadata.Index,
			Term:      snapshot.Metadata.Term,
			ConfState: &snapshot.Metadata.ConfState,
		}
	}
	p, err := player.Create(w.walDir, walSnap, w.cascade)
	if err != nil {
		return nil, nil, nil, err
	}
	var result *player.ReplayResult
	if !w.options.DisableEntryIndex {
		result = player.NewReplayResult(collection.NewEntryIndex())
	} else {
		result = player.NewReplayResult(collection.NewEntry())
	}
	if err = p.Replay(result, true); err != nil {
		if err != io.ErrUnexpectedEOF {
			return nil, nil, nil, err
		}
	}
	logMgr, err := logfile.NewManagerWithScan(logfile.ManagerConfig{
		WalDir:            w.walDir,
		LogFileChunkSize:  w.options.WalLogFileChunkSize,
		AlignmentPageSize: w.options.WalAlignmentPageSize,
		PageWriterBufSize: w.options.WalPageWriteBufferSize,
		MaxKeepLogFiles:   w.options.WalMaxKeepLogFiles,
	}, walSnap, w.cascade)
	if err != nil {
		return nil, nil, nil, err
	}

	wal, err := OpenWal(logMgr, result)
	if err != nil {
		return nil, nil, nil, err
	}
	var entryStorage ibabuza.EntryStorage
	if !w.options.DisableEntryIndex {
		em := entrystore.NewStorage(logMgr)
		wal.SetEntryIndexStorage(em)
		entryStorage = em
		result.EntryCollection().(*collection.EntryIndex).SetReader(logMgr)
	} else {
		entryStorage = raft.NewMemoryStorage()
	}
	if snapshot != nil {
		entryStorage.ApplySnapshot(*snapshot)
	}
	entryStorage.SetHardState(result.HardState())
	if deleteUncommitted {
		if err = result.EntryCollection().DeleteUncommittedEntry(result.HardState().Commit); err != nil {
			return nil, nil, nil, err
		}
	}
	ents, err := result.EntryCollection().Entries()
	if err != nil {
		return nil, nil, nil, err
	}
	if !w.options.DisableEntryIndex {
		if err = entryStorage.(*entrystore.Storage).AppendEntryIndex(ents.([]entrystore.EntryIndex)); err != nil {
			return nil, nil, nil, err
		}
	} else {
		if err = entryStorage.Append(ents.([]raftpb.Entry)); err != nil {
			return nil, nil, nil, err
		}
	}

	//TODO: append to cache? if delete uncommitted entry
	return entryStorage, wal, result, nil
}

func (w *WalManager) HasExistingWals() (bool, error) {
	if fileutil.Exist(w.walDir) == false {
		return false, nil
	}
	files, err := os.ReadDir(w.walDir)
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

func (w *WalManager) PurgeWals(purgeCfg ibabuza.WalPurgeConfig) {

}
