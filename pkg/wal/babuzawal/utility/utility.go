package utility

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"golang.org/x/exp/mmap"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	logFileMagicHeader         = "babuzawal-raft-wal-D37314C80A57AA1ABB325BF1A2750F53FA822E64"
	logFileVersion      uint64 = 1
	LogFileHeaderLength        = len(logFileMagicHeader) + 8
)

type LogFileDescSlice []iwal.LogFileDesc

func (p LogFileDescSlice) Len() int           { return len(p) }
func (p LogFileDescSlice) Less(i, j int) bool { return p[i].StartLogIndex < p[j].StartLogIndex }
func (p LogFileDescSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func ParseLogFileName(name string) (iwal.LogFileDesc, error) {
	var sequence, entryIndex uint64
	_, err := fmt.Sscanf(name, "%016x-%016x.wal", &sequence, &entryIndex)
	return iwal.LogFileDesc{Id: sequence, StartLogIndex: entryIndex}, err
}

func ReadValidLogFiles(walDir string, snap walpb.Snapshot) (LogFileDescSlice, error) {
	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	var walFiles LogFileDescSlice
	for _, f := range files {
		name := f.Name()
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(name, ".wal") {
			desc, err := ParseLogFileName(name)
			if err != nil {
				return nil, err
			}
			walFiles = append(walFiles, desc)
		}
	}
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].Id < walFiles[j].Id
	})
	var nextSeq uint64
	for i := len(walFiles) - 1; i >= 0; i-- {
		f := walFiles[i]
		if nextSeq != 0 {
			if f.Id != nextSeq-1 {
				return nil, errors.New(fmt.Sprintf("wal: file id is not continuous. expect(%d) real(%d)", nextSeq-1, f.Id))
			}
		}
		nextSeq = f.Id
		if snap.Index >= f.StartLogIndex {
			return walFiles[i:], nil
		}
	}
	return walFiles, nil
}

type LogFileIoWrapper struct {
	*os.File
	desc iwal.LogFileDesc
}

func (w *LogFileIoWrapper) CurrentLogFileDesc() iwal.LogFileDesc {
	return w.desc
}

func OpenLogFileHandle(filePath string) (*os.File, error) {
	fd, err := os.OpenFile(filePath, os.O_RDWR, fileutil.FileMode)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, LogFileHeaderLength)
	if _, err = fd.Read(buf); err != nil {
		return nil, err
	}
	if bytes.Compare(buf[:len(logFileMagicHeader)], []byte(logFileMagicHeader)) != 0 {
		return nil, errors.New("wal: corrupted file.(magic header mismatch)")
	}
	v := binary.LittleEndian.Uint64(buf[len(logFileMagicHeader):])
	if v != logFileVersion {
		return nil, errors.New(fmt.Sprintf("wal: corrupted file.(unrecognized file version is %d)", v))
	}
	return fd, nil
}

func CreateLogFileHandle(filePath string, preAllocSize int) (*os.File, error) {
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, fileutil.FileMode)
	if err != nil {
		return nil, err
	}
	if err = fileutil.AllocateFileSpace(fd, 0, int64(preAllocSize)); err != nil {
		return nil, err
	}
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	if _, err = fd.Write([]byte(logFileMagicHeader)); err != nil {
		return nil, err
	}
	ver := make([]byte, 8)
	binary.LittleEndian.PutUint64(ver, logFileVersion)
	if _, err = fd.Write(ver); err != nil {
		return nil, err
	}
	return fd, nil
}

func GetLogFileReader(dir string, desc iwal.LogFileDesc) (*LogFileIoWrapper, error) {
	f, err := OpenLogFileHandle(filepath.Join(dir, desc.GetLogFileName()))
	if err != nil {
		return nil, err
	}
	return &LogFileIoWrapper{
		File: f,
		desc: desc,
	}, nil
}

func ReadEntriesData(filePath string, readMetadata []walbase.EntryIndex[storage.EntryMetadata], destEnts []raftpb.Entry) error {
	at, err := mmap.Open(filePath)
	if err != nil {
		return err
	}
	defer at.Close()
	for i := range readMetadata {
		m := &readMetadata[i]
		d := &destEnts[i]
		if m.Metadata.DataLen > 0 {
			d.Data = make([]byte, m.Metadata.DataLen, m.Metadata.DataCapacity)
			if _, err = at.ReadAt(d.Data, m.Metadata.Offset); err != nil {
				return err
			}
		}
	}
	return nil
}

func RepairLogFile(repairFilePath string, lastValidOffset int64) error {
	handle, err := OpenLogFileHandle(repairFilePath)
	if err != nil {
		return err
	}
	_, err = handle.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	defer handle.Close()
	bf, err := os.Create(repairFilePath + ".broken")
	if err != nil {
		return err
	}
	defer bf.Close()

	if _, err = io.Copy(bf, handle); err != nil {
		return err
	}
	if err = handle.Truncate(lastValidOffset); err != nil {
		return err
	}
	return fileutil.Sync(handle)
}
