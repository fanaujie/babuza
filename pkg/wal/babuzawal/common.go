package babuzawal

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/collection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/player"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"os"
	"strings"
)

func findSnapshotInternal(walDir string, cascade *allocator.TwoLevelPool) ([]walpb.Snapshot, error) {
	result := player.NewReplayResult(collection.NewNopEntry())
	p, err := player.Create(walDir, EmptyWalpbSnapshot, cascade)
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

// createWalInternal 在指定目录创建WAL
func createWalInternal(walDir string, metadata babuzapb.WalMetadata, options Options, cascade *allocator.TwoLevelPool) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	if fileutil.Exist(walDir) {
		if err := os.RemoveAll(walDir); err != nil {
			return nil, nil, err
		}
	}
	if err := fileutil.CreateDirAndTouch(walDir); err != nil {
		return nil, nil, err
	}

	md, err := metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}

	logMgr, err := logfile.NewManager(logfile.ManagerConfig{
		WalDir:            walDir,
		LogFileChunkSize:  options.WalLogFileChunkSize,
		AlignmentPageSize: options.WalAlignmentPageSize,
		PageWriterBufSize: options.WalPageWriteBufferSize,
		MaxKeepLogFiles:   options.WalMaxKeepLogFiles,
	}, cascade)
	if err != nil {
		return nil, nil, err
	}

	wal, err := CreateWal(md, logMgr)
	if err != nil {
		return nil, nil, err
	}

	var entryStorage ibabuza.EntryStorage
	if !options.DisableEntryIndex {
		es := &storage.EntryStorage{
			EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](logMgr),
		}
		wal.SetEntryIndexStorage(es)
		entryStorage = es
	} else {
		entryStorage = raft.NewMemoryStorage()
	}

	return entryStorage, wal, nil
}

func replayWalInternal(walDir string, snapshot *raftpb.Snapshot, deleteUncommitted bool, options Options, cascade *allocator.TwoLevelPool) (ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {
	walSnap := EmptyWalpbSnapshot
	if snapshot != nil {
		walSnap = walpb.Snapshot{
			Index:     snapshot.Metadata.Index,
			Term:      snapshot.Metadata.Term,
			ConfState: &snapshot.Metadata.ConfState,
		}
	}

	p, err := player.Create(walDir, walSnap, cascade)
	if err != nil {
		return nil, nil, nil, err
	}

	var result *player.ReplayResult
	if !options.DisableEntryIndex {
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
		WalDir:            walDir,
		LogFileChunkSize:  options.WalLogFileChunkSize,
		AlignmentPageSize: options.WalAlignmentPageSize,
		PageWriterBufSize: options.WalPageWriteBufferSize,
		MaxKeepLogFiles:   options.WalMaxKeepLogFiles,
	}, walSnap, cascade)
	if err != nil {
		return nil, nil, nil, err
	}

	wal, err := OpenWal(logMgr, result)
	if err != nil {
		return nil, nil, nil, err
	}

	var entryStorage ibabuza.EntryStorage
	if !options.DisableEntryIndex {
		es := &storage.EntryStorage{
			EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](logMgr),
		}
		wal.SetEntryIndexStorage(es)
		entryStorage = es
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

	if !options.DisableEntryIndex {
		if err = entryStorage.(*storage.EntryStorage).AppendEntryIndex(ents.([]walbase.EntryIndex[storage.EntryMetadata])); err != nil {
			return nil, nil, nil, err
		}
	} else {
		if err = entryStorage.Append(ents.([]raftpb.Entry)); err != nil {
			return nil, nil, nil, err
		}
	}

	return entryStorage, wal, result, nil
}

// hasWalFilesInDir 检查目录中是否有WAL文件
func hasWalFilesInDir(dir string) (bool, error) {
	if !fileutil.Exist(dir) {
		return false, nil
	}

	files, err := os.ReadDir(dir)
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
