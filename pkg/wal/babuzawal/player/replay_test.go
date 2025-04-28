package player

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrycollection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

const (
	sectorSize = 512
)

type entrySectorInfo struct {
	index         uint64
	endFileOffset int64
}

func genLogFilesWithEntrySectorInfo(t *testing.T, dir string, segmentFileSize int, minEntrySize, maxEntrySize int) (iwal.LogFileDesc, int64, []entrySectorInfo, []raftpb.Entry) {
	cfg := logfile.ManagerConfig{
		LogFileChunkSize:  segmentFileSize,
		AlignmentPageSize: 4096,
		PageWriterBufSize: 4096,
	}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	cfg.WalDir = dir
	logMgr, err := logfile.NewManager(cfg, cp)
	assert.Nil(t, err)

	lg, err := logMgr.CreateNextTempLogFile(0, 0)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, lg.Close())
		assert.Nil(t, logMgr.FinalizeTempLogFile(0))

	}()
	assert.Nil(t, lg.Crc(0))
	assert.Nil(t, lg.Metadata([]byte{1, 2, 3, 4}))
	assert.Nil(t, lg.Snapshot(walpb.Snapshot{}))
	var entriesInfo []entrySectorInfo
	index := uint64(0)
	var newEnts []raftpb.Entry
	assert.Nil(t, lg.NextEntry(pb.WalNextEntry{
		NextTerm:  1,
		NextIndex: 1,
	}))
	for {
		entrySize := rand.Intn(maxEntrySize-minEntrySize+1) + minEntrySize
		data := make([]byte, entrySize)
		rand.Read(data)
		padding := (8 - ((codec.HeaderSize + entrySize) % 8)) % 8
		if int(lg.Offset())+codec.HeaderSize+entrySize+padding > cfg.LogFileChunkSize {
			break
		}
		assert.Nil(t, lg.Entry(pb.LogTypeNormalEntry, data))
		index++
		newEnts = append(newEnts, raftpb.Entry{
			Term:  1,
			Index: index,
			Type:  raftpb.EntryNormal,
			Data:  data,
		})
		entriesInfo = append(entriesInfo, entrySectorInfo{
			index:         index,
			endFileOffset: lg.Offset() - int64(padding),
		})
	}
	assert.Nil(t, lg.Sync(true))
	lm, _ := logMgr.LastLogFileDesc()
	return lm, lg.Offset(), entriesInfo, newEnts
}

func TestManager_RepairLogFile(t *testing.T) {
	dir, _ := ioutil.TempDir("", "repair")
	defer os.RemoveAll(dir)

	desc := iwal.LogFileDesc{
		Id:            1,
		StartLogIndex: 1,
	}
	filePath := filepath.Join(dir, desc.GetLogFileName())
	handle, err := utility.CreateLogFileHandle(filePath, 1024*4)
	assert.Nil(t, err)
	data := make([]byte, 1024)
	rand.Read(data)
	_, err = handle.Write(data)
	assert.Nil(t, err)
	assert.Nil(t, handle.Close())

	readAll, err := ioutil.ReadFile(filePath)
	assert.Nil(t, err)
	assert.Nil(t, utility.RepairLogFile(filepath.Join(dir, desc.GetLogFileName()), 512))

	// broken file
	brokenPath := filePath + ".broken"
	assert.Equal(t, true, fileutil.Exist(brokenPath))
	brokenData, err := ioutil.ReadFile(brokenPath)
	assert.Nil(t, err)
	assert.Equal(t, readAll, brokenData)

	// repair file
	readData, err := ioutil.ReadFile(filePath)
	assert.Nil(t, err)
	assert.Equal(t, readAll[:512], readData)
}

func TestPlayer_Replay_Repair_CorruptByTornWrite(t *testing.T) {
	dir, _ := ioutil.TempDir("", "player")
	defer os.RemoveAll(dir)
	desc, totalSize, entriesSectorInfo, ents := genLogFilesWithEntrySectorInfo(t, dir, 1024*128, 8, 1024)
	totalSector := int(totalSize / sectorSize)
	if totalSize%sectorSize != 0 {
		totalSector++
	}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	for i := 0; i < totalSector; i++ {
		func(tornSector int) {
			p, _ := ioutil.TempDir("", "player-torn")
			defer os.RemoveAll(p)

			reader, err := os.Open(filepath.Join(dir, desc.GetLogFileName()))
			assert.Nil(t, err)
			tornFile := filepath.Join(p, iwal.LogFileDesc{
				Id:            0,
				StartLogIndex: 0,
			}.GetLogFileName())
			writer, err := os.Create(tornFile)
			assert.Nil(t, err)
			_, err = io.Copy(writer, reader)
			assert.Nil(t, err)
			assert.Nil(t, reader.Close())
			assert.Nil(t, writer.Close())
			// torn write
			tornAddr := int64(tornSector * sectorSize)
			f, err := os.OpenFile(tornFile, os.O_RDWR, fileutil.FileMode)
			assert.Nil(t, zeroToEnd(f, tornAddr))
			assert.Nil(t, fileutil.Sync(f))
			assert.Nil(t, f.Close())
			var expectEnts []raftpb.Entry

			if tornSector > 0 {
				for _, ei := range entriesSectorInfo {
					if ei.endFileOffset > tornAddr {
						expectEnts = ents[:ei.index-1]
						if len(expectEnts) == 0 {
							expectEnts = nil
						}
						break
					}
				}
			}
			player, err := Create(p, walpb.Snapshot{}, cp)
			assert.Nil(t, err)
			pr := NewReplayResult(entrycollection.NewEntryStore())
			err = player.Replay(pr, true)
			if tornSector == 0 {
				assert.Equal(t, "wal: corrupted file.(magic header mismatch)", err.Error())
			} else {
				assert.Nil(t, err)
				entries, _ := pr.EntryCollection().Entries()
				realEnts := entries.([]raftpb.Entry)
				assert.Equal(t, expectEnts, realEnts)
			}
		}(i)
	}
}

func zeroToEnd(f *os.File, curOffset int64) error {
	endOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if err = f.Truncate(curOffset); err != nil {
		return err
	}
	if err = fileutil.AllocateFileSpace(f, 0, endOffset); err != nil {
		return err
	}
	return nil
}
