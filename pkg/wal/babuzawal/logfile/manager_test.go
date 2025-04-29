package logfile

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var (
	defaultManagerConfig = ManagerConfig{
		LogFileChunkSize:  64 * 1000 * 1000,
		AlignmentPageSize: 4096,
		PageWriterBufSize: 1024 * 1024,
		MaxKeepLogFiles:   5,
	}
)

func TestManager_ScanDir(t *testing.T) {
	t.Run("continue", func(t *testing.T) {
		p := t.TempDir()
		for i := uint64(0); i < 8; i++ {
			desc := iwal.LogFileDesc{
				Id:            i,
				StartLogIndex: i,
			}
			f := filepath.Join(p, desc.GetLogFileName())
			assert.Nil(t, os.WriteFile(f, []byte{1}, 0600))
		}
		mgr, err := NewManagerWithScan(ManagerConfig{
			WalDir: p,
		}, walpb.Snapshot{}, nil)
		assert.Nil(t, err)
		for i := uint64(0); i < 8; i++ {
			assert.Equal(t, i, mgr.filesDesc[i].Id)
			assert.Equal(t, i, mgr.filesDesc[i].StartLogIndex)
		}
	})

	t.Run("not continue", func(t *testing.T) {
		p := t.TempDir()
		for i := uint64(0); i < 8; i++ {
			desc := iwal.LogFileDesc{
				Id:            i,
				StartLogIndex: i,
			}
			f := filepath.Join(p, desc.GetLogFileName())
			assert.Nil(t, ioutil.WriteFile(f, []byte{1}, 0600))
		}
		desc := iwal.LogFileDesc{
			Id:            4,
			StartLogIndex: 4,
		}
		assert.Nil(t, os.Remove(filepath.Join(p, desc.GetLogFileName())))
		_, err := NewManagerWithScan(ManagerConfig{
			WalDir: p,
		}, walpb.Snapshot{}, nil)
		//assert.Nil(t, err)
		assert.Equal(t, "wal: file id is not continuous. expect(4) real(3)", err.Error())
	})
}

func TestManager_CreateNextTempLogFile(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		p := t.TempDir()
		cp := allocator.NewDefaultTwoLevelPool(1024, 1024*1024*4)
		defaultManagerConfig.WalDir = p
		m, err := NewManager(defaultManagerConfig, cp)
		assert.Nil(t, err)
		f, err := m.CreateNextTempLogFile(0, 1)
		assert.Nil(t, err)
		defer f.Close()
		fd, ok := m.filesDesc[0]
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(0), fd.Id)
		assert.Equal(t, uint64(1), fd.StartLogIndex)
		assert.Equal(t, true, fd.IsTempFile)
		assert.Equal(t, uint64(1), m.nextLogId)
		assert.Equal(t, true, fileutil.Exist(filepath.Join(m.cfg.WalDir, fd.GetTempLogFileName())))
	})

	t.Run("failure: not expected next id", func(t *testing.T) {
		p := t.TempDir()
		cp := allocator.NewDefaultTwoLevelPool(1024, 1024*1024*4)
		defaultManagerConfig.WalDir = p
		m, err := NewManager(defaultManagerConfig, cp)
		assert.Nil(t, err)
		_, err = m.CreateNextTempLogFile(1, 1)
		assert.Error(t, err)
	})
}

func TestManager_FinalizeTempLogFile(t *testing.T) {
	p := t.TempDir()
	cp := allocator.NewDefaultTwoLevelPool(1024, 1024*1024*4)
	defaultManagerConfig.WalDir = p
	m, err := NewManager(defaultManagerConfig, cp)
	assert.Nil(t, err)
	f, err := m.CreateNextTempLogFile(0, 1)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())
	// not found temp file id
	assert.Error(t, m.FinalizeTempLogFile(1))
	// found temp file id
	assert.Nil(t, m.FinalizeTempLogFile(0))
	fd, ok := m.filesDesc[0]
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(0), fd.Id)
	assert.Equal(t, uint64(1), fd.StartLogIndex)
	assert.Equal(t, false, fd.IsTempFile)
	assert.Equal(t, true, fileutil.Exist(filepath.Join(m.cfg.WalDir, fd.GetLogFileName())))
}

func TestManager_OpenLogFile(t *testing.T) {
	p := t.TempDir()
	cp := allocator.NewDefaultTwoLevelPool(1024, 1024*1024*4)
	defaultManagerConfig.WalDir = p
	m, err := NewManager(defaultManagerConfig, cp)
	assert.Nil(t, err)
	f, err := m.CreateNextTempLogFile(0, 1)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())
	assert.Nil(t, m.FinalizeTempLogFile(0))

	// not found file
	_, err = m.OpenLogFile(1, 0, 0)
	assert.Error(t, err)

	// found file
	f, err = m.OpenLogFile(0, 0, 0)
	assert.Nil(t, err)
	assert.Nil(t, f.Metadata([]byte{1, 2, 3, 4}))
	crc := f.LastCrc()
	offset := f.Offset()
	assert.Nil(t, f.Close())

	// seek offset and crc
	f, err = m.OpenLogFile(0, offset, crc)
	assert.Nil(t, err)
	assert.Equal(t, crc, f.LastCrc())
	assert.Equal(t, offset, f.Offset())
	assert.Nil(t, f.Close())
}

func TestManager_Purge(t *testing.T) {

	for _, tc := range []struct {
		logStartIndex []uint64
		remainIndex   []uint64
		purgeIndex    uint64
		maxKeepFiles  uint
	}{
		{
			logStartIndex: []uint64{1, 2, 3, 4, 5},
			remainIndex:   []uint64{3, 4, 5},
			purgeIndex:    3,
			maxKeepFiles:  3,
		},
		{
			logStartIndex: []uint64{1, 3, 5, 7, 9, 10},
			remainIndex:   []uint64{5, 7, 9, 10},
			purgeIndex:    5,
			maxKeepFiles:  3,
		},
		{
			logStartIndex: []uint64{1, 3, 5, 7, 9, 10},
			remainIndex:   []uint64{3, 5, 7, 9, 10},
			purgeIndex:    4,
			maxKeepFiles:  3,
		},
		{
			logStartIndex: []uint64{1, 3, 5, 7, 9, 10},
			remainIndex:   []uint64{7, 9, 10},
			purgeIndex:    9,
			maxKeepFiles:  3,
		},
		{
			logStartIndex: []uint64{1, 3, 5, 7, 9, 10},
			remainIndex:   []uint64{1, 3, 5, 7, 9, 10},
			purgeIndex:    1,
			maxKeepFiles:  6,
		},
	} {
		func() {
			p := t.TempDir()
			for id, snapIndex := range tc.logStartIndex {
				desc := iwal.LogFileDesc{
					Id:            uint64(id),
					StartLogIndex: snapIndex,
				}
				assert.Nil(t, ioutil.WriteFile(filepath.Join(p, desc.GetLogFileName()), nil, os.ModeTemporary))
			}
			mgr, err := NewManagerWithScan(ManagerConfig{
				WalDir: p,
			}, walpb.Snapshot{}, nil)
			assert.Nil(t, err)
			assert.Nil(t, mgr.Purge(tc.purgeIndex))
			shiftLogId := len(tc.logStartIndex) - len(tc.remainIndex)
			for index, snapIndex := range tc.remainIndex {
				desc := iwal.LogFileDesc{
					Id:            uint64(shiftLogId + index),
					StartLogIndex: snapIndex,
				}
				assert.Equal(t, true, fileutil.Exist(filepath.Join(p, desc.GetLogFileName())))
				_, ok := mgr.filesDesc[uint64(shiftLogId+index)]
				assert.Equal(t, true, ok)
			}
		}()

	}
}
