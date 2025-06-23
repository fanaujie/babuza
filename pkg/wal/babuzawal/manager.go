package babuzawal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type WalManager struct {
	walDir       string
	options      Options
	memPool      *allocator.ByteSlicePool
	logger       ibabuza.Logger
	logMgr       *logfile.Manager
	purgerSnapCh chan raftpb.Snapshot
	purgerStopCh chan struct{}
}

type Options struct {
	WalLogFileChunkSize    int
	WalAlignmentPageSize   int
	WalPageWriteBufferSize int
	WalMinEntryBufferSize  int
	WalMaxEntryBufferSize  int
	WalMaxKeepLogFiles     uint
	DisableEntryIndex      bool
}

func defaultOptions() Options {
	return Options{
		WalLogFileChunkSize:    64 * 1000 * 1000,
		WalAlignmentPageSize:   4096,
		WalPageWriteBufferSize: 4096 * 32,
		WalMinEntryBufferSize:  1024 * 1024,
		WalMaxEntryBufferSize:  4 * 1024 * 1024,
		WalMaxKeepLogFiles:     5,
		DisableEntryIndex:      false,
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
		opt.WalMinEntryBufferSize = d
	}
}

func SetOptsWithWalMaxDynamicEntryBufferSize(d int) SetOptions {
	return func(opt *Options) {
		opt.WalMaxEntryBufferSize = d
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

var _ ibabuza.WalManager = (*WalManager)(nil)

func NewWalManager(walDir string, logger ibabuza.Logger, setOptions ...SetOptions) *WalManager {
	opts := defaultOptions()
	for _, s := range setOptions {
		s(&opts)
	}
	logger.Infof("BabuzaWalManager: create wal manager with walDir=%s", walDir)
	return &WalManager{
		walDir:       walDir,
		options:      opts,
		memPool:      allocator.NewByteSlicePool(opts.WalMinEntryBufferSize, opts.WalMaxEntryBufferSize, 2),
		logger:       logger,
		purgerSnapCh: make(chan raftpb.Snapshot, 1),
		purgerStopCh: make(chan struct{}),
	}
}

func (w *WalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	return findSnapshotInternal(w.walDir, w.memPool)
}

func (w *WalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	es, wal, logMgr, err := createWalInternal(w.walDir, metadata, w.options, w.memPool, w.purgerSnapCh)
	if err != nil {
		return nil, nil, err
	}
	w.logMgr = logMgr
	return es, wal, nil
}

func (w *WalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	es, wal, logMgr, result, err := replayWalInternal(w.walDir, snapshot, deleteUncommitted, w.options, w.memPool, w.purgerSnapCh)
	if err != nil {
		return nil, nil, nil, err
	}
	w.logMgr = logMgr
	return es, wal, result, nil
}

func (w *WalManager) HasExistingWals() (bool, error) {
	return hasWalFilesInDir(w.walDir)
}

func (w *WalManager) Purger() ibabuza.WalPurger {
	return &purger{
		WalManager: w,
	}
}

func (w *WalManager) Close() error {
	select {
	case <-w.purgerStopCh:
	default:
		close(w.purgerStopCh)
	}
	return nil
}

type purger struct {
	*WalManager
}

func (p *purger) Start() {
	go func() {
		for {
			select {
			case snap := <-p.purgerSnapCh:
				if err := p.logMgr.Purge(snap.Metadata.Index); err != nil {
					p.logger.Errorf("failed to purge snapshot index=%d: %v", snap.Metadata.Index, err)
				}
			case <-p.purgerStopCh:
				return
			}
		}
	}()
}
