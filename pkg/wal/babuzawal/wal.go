package babuzawal

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrystore"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"sync"
)

type EntryIndexStorage interface {
	AppendCache([]raftpb.Entry)
	DeleteCache(uint64)
	AppendEntryIndex([]entrystore.EntryIndex) error
}

var EmptyWalpbSnapshot = walpb.Snapshot{}

type walState struct {
	metadata       []byte
	nextEntry      pb.WalNextEntry
	hardState      raftpb.HardState
	lastEntryIndex uint64
}

func (s walState) EmptyNextEntry() bool {
	return s.nextEntry.NextTerm == 0 && s.nextEntry.NextIndex == 0
}

type Wal struct {
	logMgr            iwal.LogFileManager
	state             walState
	currentLogFile    iwal.LogFile
	entryIndexStorage EntryIndexStorage
	enableNoSync      bool
	mu                sync.Mutex
}

func CreateWal(metadata []byte, logMgr iwal.LogFileManager) (*Wal, error) {
	startLogId := uint64(0)
	startLogIndex := uint64(0)

	tempLogFile, err := logMgr.CreateNextTempLogFile(startLogId, startLogIndex)
	if err != nil {
		return nil, err
	}
	if err = tempLogFile.Crc(0); err != nil {
		return nil, err
	}
	if err = tempLogFile.Metadata(metadata); err != nil {
		return nil, err
	}
	if err = tempLogFile.Snapshot(EmptyWalpbSnapshot); err != nil {
		return nil, err
	}
	if err = tempLogFile.Sync(true); err != nil {
		return nil, err
	}
	if err = tempLogFile.Close(); err != nil {
		return nil, err
	}
	if err = logMgr.FinalizeTempLogFile(startLogId); err != nil {
		return nil, err
	}

	var cErr error
	var openLogFile iwal.LogFile
	defer func() {
		if cErr != nil {
			//TODO: broken wal
			//makeBrokenFile(fm)
		}
	}()
	openLogFile, cErr = logMgr.OpenLogFile(startLogId, tempLogFile.Offset(), tempLogFile.LastCrc())
	if cErr != nil {
		return nil, cErr
	}
	if cErr = logMgr.SyncWalFolder(); err != nil {
		return nil, cErr
	}

	return &Wal{
		logMgr: logMgr,
		state: walState{
			metadata: metadata,
		},
		currentLogFile: openLogFile,
	}, nil
}

func OpenWal(logMgr iwal.LogFileManager, lastLogMeta iwal.ReplayLastLogFileResult) (*Wal, error) {
	logWriter, err := logMgr.OpenLogFile(lastLogMeta.LastLogFileDesc().Id, lastLogMeta.LastValidLogOffset(),
		lastLogMeta.LastValidLogCrc())
	if err != nil {
		return nil, err
	}
	w := &Wal{
		logMgr: logMgr,
		state: walState{
			metadata:  lastLogMeta.Metadata(),
			nextEntry: lastLogMeta.NextEntry(),
			hardState: lastLogMeta.HardState(),
		},
		currentLogFile:    logWriter,
		entryIndexStorage: nil,
		enableNoSync:      false,
	}
	return w, nil
}

func (w *Wal) SetUnsafeNoFsync() {
	w.enableNoSync = true
}

func (w *Wal) Save(state raftpb.HardState, entries []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	entriesLen := len(entries)
	if entriesLen > 0 {
		if err := w.saveEntry(entries); err != nil {
			return err
		}
	}
	if err := w.saveHardState(state); err != nil {
		return err
	}

	if w.currentLogFile.DoCycle() {
		if err := w.cycle(); err != nil {
			return err
		}
	} else {
		if raft.MustSync(state, w.state.hardState, entriesLen) == true {
			if err := w.currentLogFile.Sync(!w.enableNoSync); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Wal) SaveSnapshot(snapshot raftpb.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	walsnap := walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
	if err := w.currentLogFile.Snapshot(walsnap); err != nil {
		return err
	}
	if w.state.lastEntryIndex < walsnap.Index {
		w.state.lastEntryIndex = walsnap.Index
	}
	if err := w.currentLogFile.Sync(!w.enableNoSync); err != nil {
		return err
	}
	return nil
}

func (w *Wal) Purge(snap raftpb.Snapshot) error {
	return w.logMgr.Purge(snap.Metadata.Index)
}

func (w *Wal) Sync() error {
	return w.currentLogFile.Sync(!w.enableNoSync)
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var errs []error
	if err := w.currentLogFile.Sync(!w.enableNoSync); err != nil {
		return err
	}
	if err := w.currentLogFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := w.logMgr.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New("")
}

func (w *Wal) SetEntryIndexStorage(es EntryIndexStorage) {
	w.entryIndexStorage = es
}

func (w *Wal) saveEntry(entries []raftpb.Entry) error {
	var entriesIndex []entrystore.EntryIndex
	if w.entryIndexStorage != nil {
		entriesIndex = make([]entrystore.EntryIndex, len(entries))
	}
	for i := range entries {
		e := &entries[i]
		if w.state.nextEntry.NextTerm != e.Term || w.state.nextEntry.NextIndex != e.Index {
			w.state.nextEntry.NextTerm = e.Term
			w.state.nextEntry.NextIndex = e.Index
			if err := w.currentLogFile.NextEntry(w.state.nextEntry); err != nil {
				return err
			}
		}
		if w.entryIndexStorage != nil {
			entIndex := &entriesIndex[i]
			entIndex.Index = e.Index
			entIndex.Term = e.Term
			entIndex.Type = e.Type
			entIndex.FileId = w.tailLogFileDesc().Id
			entIndex.EntryOffset = w.currentLogFile.Offset() + codec.HeaderSize
			entIndex.EntryDataLen = int64(len(e.Data))
			entIndex.EntryDataCapacity = entIndex.EntryDataLen + ((8 - (entIndex.EntryDataLen % 8)) % 8)
		}
		if err := w.currentLogFile.Entry(pb.LogType(e.Type), e.Data); err != nil {
			return err
		}
		//TODO: check e.index+1 equal Next Index
		w.state.nextEntry.NextIndex = e.Index + 1
		w.state.lastEntryIndex = e.Index
	}
	if w.entryIndexStorage != nil {
		if err := w.entryIndexStorage.AppendEntryIndex(entriesIndex); err != nil {
			return err
		}
		w.entryIndexStorage.AppendCache(entries)
	}
	return nil
}

func (w *Wal) saveHardState(st raftpb.HardState) error {
	if !raft.IsEmptyHardState(st) {
		if err := w.currentLogFile.HardState(st); err != nil {
			return err
		}
		w.state.hardState = st
		if w.entryIndexStorage != nil {
			w.entryIndexStorage.DeleteCache(st.Commit)
		}
	}
	return nil
}

func (w *Wal) cycle() error {
	if err := w.currentLogFile.Sync(!w.enableNoSync); err != nil {
		return err
	}
	if err := w.currentLogFile.Truncate(); err != nil {
		return err
	}
	if err := w.currentLogFile.Close(); err != nil {
		return err
	}
	lastCrc := w.currentLogFile.LastCrc()
	nextId := w.tailLogFileDesc().Id + 1
	nextLogger, err := w.logMgr.CreateNextTempLogFile(nextId, w.state.lastEntryIndex+1)
	if err = nextLogger.Crc(lastCrc); err != nil {
		return err
	}
	if err = nextLogger.Metadata(w.state.metadata); err != nil {
		return err
	}
	if err = nextLogger.HardState(w.state.hardState); err != nil {
		return err
	}
	if !w.state.EmptyNextEntry() {
		if err = nextLogger.NextEntry(w.state.nextEntry); err != nil {
			return err
		}
	}
	if err = nextLogger.Sync(true); err != nil {
		return err
	}
	if err = nextLogger.Close(); err != nil {
		return err
	}
	if err = w.logMgr.FinalizeTempLogFile(nextId); err != nil {
		return err
	}
	if err = w.logMgr.SyncWalFolder(); err != nil {
		return err
	}
	w.currentLogFile, err = w.logMgr.OpenLogFile(nextId, nextLogger.Offset(), nextLogger.LastCrc())
	if err != nil {
		return err
	}
	return nil
}

func (w *Wal) tailLogFileDesc() iwal.LogFileDesc {
	f, err := w.logMgr.LastLogFileDesc()
	if err != nil {
		panic(err)
	}
	return f
}

func makeBrokenFile(fm iwal.LogFileDesc) error {

	return nil
}
