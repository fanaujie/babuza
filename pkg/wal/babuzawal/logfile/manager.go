package logfile

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrystore"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile/page"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrNotFoundValidLogFiles = errors.New("not found valid log files")
)

type ManagerConfig struct {
	WalDir            string
	LogFileChunkSize  int
	AlignmentPageSize int
	PageWriterBufSize int
	MaxKeepLogFiles   uint
}

type Manager struct {
	cfg        ManagerConfig
	filesDesc  map[uint64]iwal.LogFileDesc
	nextLogId  uint64
	cascade    *allocator.TwoLevelPool
	walDirFile *os.File
	mu         sync.Mutex
}

func NewManager(cfg ManagerConfig, cascade *allocator.TwoLevelPool) (*Manager, error) {
	var dirFile *os.File
	dirFile, err := os.Open(cfg.WalDir)
	if err != nil {
		return nil, err
	}
	return &Manager{
		cfg:        cfg,
		filesDesc:  make(map[uint64]iwal.LogFileDesc),
		cascade:    cascade,
		walDirFile: dirFile,
	}, nil
}

func NewManagerWithScan(cfg ManagerConfig, startSnapshot walpb.Snapshot, cascade *allocator.TwoLevelPool) (*Manager, error) {
	var dirFile *os.File
	dirFile, err := os.Open(cfg.WalDir)
	if err != nil {
		return nil, err
	}
	logFiles, err := utility.ReadValidLogFiles(cfg.WalDir, startSnapshot)
	if err != nil {
		return nil, err
	}
	if len(logFiles) == 0 {
		return nil, ErrNotFoundValidLogFiles
	}
	filesDesc := make(map[uint64]iwal.LogFileDesc)
	for _, s := range logFiles {
		filesDesc[s.Id] = s
	}
	return &Manager{
		cfg:        cfg,
		filesDesc:  filesDesc,
		nextLogId:  logFiles[len(logFiles)-1].Id + 1,
		cascade:    cascade,
		walDirFile: dirFile,
	}, nil
}

func (m *Manager) OpenLogFile(id uint64, seekTo int64, initCrc uint32) (iwal.LogFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logDesc, ok := m.filesDesc[id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("logfile manager: not found log file(Id=%d).", id))
	}
	handle, err := utility.OpenLogFileHandle(filepath.Join(m.cfg.WalDir, logDesc.GetLogFileName()))
	if err != nil {
		return nil, err
	}
	if _, err = handle.Seek(seekTo, io.SeekStart); err != nil {
		return nil, err
	}
	pw, err := page.CreateWriter(m.cfg.LogFileChunkSize, m.cfg.AlignmentPageSize, m.cfg.PageWriterBufSize, handle)
	if err != nil {
		return nil, err
	}
	return New(pw, codec.NewEncoder(pw, m.cascade, initCrc)), nil
}

func (m *Manager) CreateNextTempLogFile(id uint64, startLogIndex uint64) (iwal.LogFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nextLogId != id {
		return nil, errors.New(fmt.Sprintf("logfile manager: expected next log file (Id=%d). but log file (Id=%d).", m.nextLogId, id))
	}
	desc := iwal.LogFileDesc{
		Id:            id,
		StartLogIndex: startLogIndex,
		IsTempFile:    true,
	}
	handle, err := utility.CreateLogFileHandle(filepath.Join(m.cfg.WalDir, desc.GetTempLogFileName()), m.cfg.LogFileChunkSize)
	if err != nil {
		return nil, err
	}
	m.filesDesc[id] = desc
	m.nextLogId++
	pw, err := page.CreateWriter(m.cfg.LogFileChunkSize, m.cfg.AlignmentPageSize, m.cfg.PageWriterBufSize, handle)
	if err != nil {
		return nil, err
	}
	return New(pw, codec.NewEncoder(pw, m.cascade, 0)), nil
}

func (m *Manager) FinalizeTempLogFile(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logDesc, ok := m.filesDesc[id]
	if !ok {
		return errors.New(fmt.Sprintf("logfile manager: not found log file(Id=%d).", id))
	}
	if err := os.Rename(filepath.Join(m.cfg.WalDir, logDesc.GetTempLogFileName()),
		filepath.Join(m.cfg.WalDir, logDesc.GetLogFileName())); err != nil {
		return err
	}
	logDesc.IsTempFile = false
	m.filesDesc[id] = logDesc
	return nil
}

func (m *Manager) SyncWalFolder() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := fileutil.Sync(m.walDirFile); err != nil {
		return err
	}
	return nil
}

func (m *Manager) Purge(snapshotIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var r utility.LogFileDescSlice
	for _, seg := range m.filesDesc {
		r = append(r, seg)
	}
	sort.Sort(r)
	smaller := 0
	for i := range r {
		if r[i].StartLogIndex >= snapshotIndex {
			smaller = i - 1
			break
		}
	}
	if smaller >= 0 {
		for i := 0; i < smaller; i++ {
			for len(r) > int(m.cfg.MaxKeepLogFiles) {
				if r[0].StartLogIndex < snapshotIndex {
					if err := os.RemoveAll(filepath.Join(m.cfg.WalDir, r[0].GetLogFileName())); err != nil {
						return err
					}
					delete(m.filesDesc, r[0].Id)
					break
				}
				r = r[1:]
			}
		}
	}
	return nil
}

func (m *Manager) ReadEntriesData(readEntryIndex []entrystore.EntryIndex, destEnts []raftpb.Entry) error {
	if len(readEntryIndex) != len(destEnts) || len(readEntryIndex) == 0 {
		return errors.New("logfile manager: invalid the size of entryIndex and raftpb.Entry")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	currFileId := readEntryIndex[0].FileId
	startIndex := 0
	for i := range readEntryIndex {
		ei := &readEntryIndex[i]
		if currFileId != ei.FileId {
			desc, ok := m.filesDesc[currFileId]
			if !ok {
				return errors.New(fmt.Sprintf("logfile manager: not found log file(Id=%d).", currFileId))
			}
			if err := utility.ReadEntriesData(filepath.Join(m.cfg.WalDir, desc.GetLogFileName()),
				readEntryIndex[startIndex:i], destEnts[startIndex:i]); err != nil {
				return err
			}
			currFileId = ei.FileId
			startIndex = i
		}
	}
	// last file
	desc, ok := m.filesDesc[currFileId]
	if !ok {
		return errors.New(fmt.Sprintf("logfile manager: not found log file(Id=%d).", currFileId))
	}
	if err := utility.ReadEntriesData(filepath.Join(m.cfg.WalDir, desc.GetLogFileName()),
		readEntryIndex[startIndex:], destEnts[startIndex:]); err != nil {
		return err
	}

	return nil
}

func (m *Manager) LastLogFileDesc() (iwal.LogFileDesc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.filesDesc) == 0 {
		return iwal.LogFileDesc{}, errors.New("logfile manager: not found any log files")
	}
	return m.filesDesc[m.nextLogId-1], nil
}

func (m *Manager) Close() error {
	return m.walDirFile.Close()
}
