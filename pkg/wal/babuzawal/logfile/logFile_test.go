package logfile

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrycollection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile/page"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/player"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
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

type termEntriesIndex struct {
	nextEntry    pb.WalNextEntry
	entriesIndex []walbase.EntryIndex[storage.EntryMetadata]
}

type termEntries struct {
	nextEntry pb.WalNextEntry
	entries   []raftpb.Entry
}

type expectResult struct {
	crcs             []uint32
	metadata         [][]byte
	snapshots        []walpb.Snapshot
	hardStates       []raftpb.HardState
	nextEntries      []pb.WalNextEntry
	termEntriesIndex []termEntriesIndex
	termEntries      []termEntries
}

type logRWConfig struct {
	segmentSize         int
	firstBufferSize     int
	secondMaxBufferSize int
	alignmentPageSize   int
	pageBufferSize      int
}

var (
	defaultLogRWConfig = logRWConfig{
		segmentSize:         4 * 1024 * 1024,
		firstBufferSize:     1024 * 1024,
		secondMaxBufferSize: 2 * 1024 * 1024,
		alignmentPageSize:   4096,
		pageBufferSize:      1024,
	}
)

func newTestLogFile(t *testing.T, dir string, cfg logRWConfig, desc iwal.LogFileDesc) *LogFile {
	cp := allocator.NewDefaultTwoLevelPool(cfg.firstBufferSize, cfg.secondMaxBufferSize)
	handle, err := utility.CreateLogFileHandle(filepath.Join(dir, desc.GetLogFileName()), cfg.segmentSize)
	assert.Nil(t, err)
	pw, err := page.CreateWriter(cfg.segmentSize, cfg.alignmentPageSize, cfg.pageBufferSize, handle)
	assert.Nil(t, err)
	enc := codec.NewEncoder(pw, cp, 0)
	return &LogFile{
		pw:  pw,
		enc: enc,
	}
}

func newTestLogParser(t *testing.T, entryParser iwal.EntryCollection) (*player.Parser, *player.ReplayResult) {
	cp := allocator.NewDefaultTwoLevelPool(defaultLogRWConfig.firstBufferSize, defaultLogRWConfig.secondMaxBufferSize)
	r := player.NewReplayResult(entryParser)
	return player.NewParser(r, walpb.Snapshot{}, cp), r
}

func TestLogFileAndReplay_CRC(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var es expectResult
	es.crcs = append(es.crcs, 0x12345678)
	assert.Nil(t, logW.Crc(es.crcs[0]))
	es.crcs = append(es.crcs, 0x87654321)
	assert.Nil(t, logW.Crc(es.crcs[1]))
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	assert.Equal(t, es.crcs[1], parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_Metadata(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var es expectResult
	es.crcs = append(es.crcs, 0)
	assert.Nil(t, logW.Crc(es.crcs[0]))
	for i := 0; i < 8; i++ {
		m := make([]byte, 16)
		rand.Read(m)
		es.metadata = append(es.metadata, m)
		assert.Nil(t, logW.Metadata(es.metadata[i]))
	}
	es.crcs = append(es.crcs, 1)
	assert.Nil(t, logW.Crc(es.crcs[1]))
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	assert.Equal(t, es.metadata[7], parsedResult.Metadata())
	assert.Equal(t, es.crcs[1], parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_Snapshot(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var es expectResult
	m1 := make([]byte, 16)
	rand.Read(m1)
	es.crcs = append(es.crcs, 0)
	assert.Nil(t, logW.Crc(es.crcs[0]))
	es.metadata = append(es.metadata, m1)
	assert.Nil(t, logW.Metadata(es.metadata[0]))

	// must be failed
	assert.Error(t, logW.Snapshot(walpb.Snapshot{
		Index:     1,
		Term:      0,
		ConfState: nil, // if index > 1, the ConfState can not be nil.
	}))

	for i := uint64(0); i < 8; i++ {
		es.snapshots = append(es.snapshots, walpb.Snapshot{
			Index: 0,
			Term:  0,
		})
		assert.Nil(t, logW.Snapshot(es.snapshots[i]))
	}

	es.crcs = append(es.crcs, 1)
	assert.Nil(t, logW.Crc(es.crcs[1]))
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	assert.Equal(t, es.metadata[0], parsedResult.Metadata())
	assert.Equal(t, es.snapshots, parsedResult.WalSnapshots())
	assert.Equal(t, es.crcs[1], parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_HardState(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var es expectResult
	m1 := make([]byte, 16)
	rand.Read(m1)
	es.crcs = append(es.crcs, 0)
	assert.Nil(t, logW.Crc(es.crcs[0]))
	es.metadata = append(es.metadata, m1)
	assert.Nil(t, logW.Metadata(es.metadata[0]))
	for i := uint64(0); i < 8; i++ {
		es.hardStates = append(es.hardStates, raftpb.HardState{
			Term:   i,
			Vote:   i,
			Commit: i,
		})
		assert.Nil(t, logW.HardState(es.hardStates[i]))
	}

	es.crcs = append(es.crcs, 1)
	assert.Nil(t, logW.Crc(es.crcs[1]))
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	assert.Equal(t, es.metadata[0], parsedResult.Metadata())
	assert.Equal(t, es.hardStates[7], parsedResult.HardState())
	assert.Equal(t, es.crcs[1], parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_NextEntry(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var es expectResult
	m1 := make([]byte, 16)
	rand.Read(m1)
	es.crcs = append(es.crcs, 0)
	assert.Nil(t, logW.Crc(es.crcs[0]))
	es.metadata = append(es.metadata, m1)
	assert.Nil(t, logW.Metadata(es.metadata[0]))
	for i := uint64(0); i < 8; i++ {
		es.nextEntries = append(es.nextEntries, pb.WalNextEntry{
			NextTerm:  i + 1,
			NextIndex: i + 1,
		})
		assert.Nil(t, logW.NextEntry(es.nextEntries[i]))
	}

	es.crcs = append(es.crcs, 1)
	assert.Nil(t, logW.Crc(es.crcs[1]))
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	assert.Equal(t, es.metadata[0], parsedResult.Metadata())
	assert.Equal(t, es.nextEntries[7], parsedResult.NextEntry())
	assert.Equal(t, es.crcs[1], parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_EntryIndexFormat(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var expectSeg expectResult
	m1 := make([]byte, 16)
	rand.Read(m1)
	expectSeg.metadata = append(expectSeg.metadata, m1)
	assert.Nil(t, logW.Metadata(expectSeg.metadata[0]))

	var numTerm, numEntries uint64 = 8, 16
	for i := uint64(0); i < numTerm; i++ {
		expectSeg.crcs = append(expectSeg.crcs, uint32(i))
		assert.Nil(t, logW.Crc(expectSeg.crcs[len(expectSeg.crcs)-1]))
		exEntries := termEntriesIndex{
			entriesIndex: make([]walbase.EntryIndex[storage.EntryMetadata], numEntries),
		}
		exEntries.nextEntry = pb.WalNextEntry{
			NextTerm:  i + 1,
			NextIndex: i*numEntries + 1}
		assert.Nil(t, logW.NextEntry(exEntries.nextEntry))
		for j := uint64(0); j < numEntries/2; j++ {
			dataLen := rand.Intn(16) + 1
			data := make([]byte, dataLen)
			rand.Read(data)
			exEntries.entriesIndex[j].Term = exEntries.nextEntry.NextTerm
			exEntries.entriesIndex[j].Index = exEntries.nextEntry.NextIndex + j
			exEntries.entriesIndex[j].Metadata.FileId = fm.Id
			exEntries.entriesIndex[j].Metadata.Offset = logW.Offset() + codec.HeaderSize
			exEntries.entriesIndex[j].Metadata.DataLen = int64(dataLen)

			assert.Nil(t, logW.Entry(pb.LogTypeNormalEntry, data))
		}
		//test nil entry data
		for j := numEntries / 2; j < numEntries; j++ {
			exEntries.entriesIndex[j].Term = exEntries.nextEntry.NextTerm
			exEntries.entriesIndex[j].Index = exEntries.nextEntry.NextIndex + j
			exEntries.entriesIndex[j].Metadata.FileId = fm.Id
			exEntries.entriesIndex[j].Metadata.Offset = logW.Offset() + codec.HeaderSize
			assert.Nil(t, logW.Entry(pb.LogTypeNormalEntry, nil))
		}
		expectSeg.termEntriesIndex = append(expectSeg.termEntriesIndex, exEntries)
	}
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewIndexedEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	entries, _ := parsedResult.EntryCollection().Entries()
	resultEntries := entries.([]walbase.EntryIndex[storage.EntryMetadata])
	assert.Equal(t, numTerm*numEntries, uint64(len(resultEntries)))

	for term := uint64(0); term < numTerm; term++ {
		checkTermFirstEntry := resultEntries[term*numEntries]
		expectNextEntry := expectSeg.termEntriesIndex[term].nextEntry
		assert.Equal(t, expectNextEntry.NextTerm, checkTermFirstEntry.Term)
		assert.Equal(t, expectNextEntry.NextIndex, checkTermFirstEntry.Index)
		for eIndex := uint64(0); eIndex < numEntries; eIndex++ {
			checkEntry := resultEntries[term*numEntries+eIndex]
			assert.Equal(t, expectNextEntry.NextTerm, checkEntry.Term)
			assert.Equal(t, expectNextEntry.NextIndex+eIndex, checkEntry.Index)

			expectEntryIndex := expectSeg.termEntriesIndex[term].entriesIndex[eIndex]
			assert.Equal(t, expectEntryIndex.Metadata.FileId, checkEntry.Metadata.FileId)
			assert.Equal(t, expectEntryIndex.Index, checkEntry.Index)
			assert.Equal(t, expectEntryIndex.Metadata.Offset, checkEntry.Metadata.Offset)
			assert.Equal(t, expectEntryIndex.Metadata.DataLen, checkEntry.Metadata.DataLen)
		}

	}
	assert.Equal(t, expectSeg.metadata[0], parsedResult.Metadata())
	assert.Equal(t, logW.LastCrc(), parsedResult.LastValidLogCrc())
}

func TestLogFileAndReplay_EntryFormat(t *testing.T) {
	dir, _ := ioutil.TempDir("", "logFile")
	defer os.RemoveAll(dir)
	fm := iwal.LogFileDesc{}
	logW := newTestLogFile(t, dir, defaultLogRWConfig, fm)
	var expectSeg expectResult
	m1 := make([]byte, 16)
	rand.Read(m1)
	expectSeg.metadata = append(expectSeg.metadata, m1)
	assert.Nil(t, logW.Metadata(expectSeg.metadata[0]))

	var numTerm, numEntries uint64 = 8, 16
	for i := uint64(0); i < numTerm; i++ {
		expectSeg.crcs = append(expectSeg.crcs, uint32(i))
		assert.Nil(t, logW.Crc(expectSeg.crcs[len(expectSeg.crcs)-1]))
		exEntries := termEntries{
			entries: make([]raftpb.Entry, numEntries),
		}
		exEntries.nextEntry = pb.WalNextEntry{
			NextTerm:  i + 1,
			NextIndex: i*numEntries + 1}
		assert.Nil(t, logW.NextEntry(exEntries.nextEntry))
		for j := uint64(0); j < numEntries/2; j++ {
			dataLen := rand.Intn(16) + 1
			data := make([]byte, dataLen)
			rand.Read(data)
			exEntries.entries[j].Term = exEntries.nextEntry.NextTerm
			exEntries.entries[j].Index = exEntries.nextEntry.NextIndex + j
			exEntries.entries[j].Data = data
			exEntries.entries[j].Type = raftpb.EntryType(pb.LogTypeNormalEntry)
			assert.Nil(t, logW.Entry(pb.LogTypeNormalEntry, data))
		}
		//test nil entry data
		for j := numEntries / 2; j < numEntries; j++ {
			exEntries.entries[j].Term = exEntries.nextEntry.NextTerm
			exEntries.entries[j].Index = exEntries.nextEntry.NextIndex + j
			exEntries.entries[j].Type = raftpb.EntryType(pb.LogTypeNormalEntry)
			assert.Nil(t, logW.Entry(pb.LogTypeNormalEntry, nil))
		}
		expectSeg.termEntries = append(expectSeg.termEntries, exEntries)
	}
	assert.Nil(t, logW.Sync(true))
	assert.Nil(t, logW.Close())
	reader, err := utility.GetLogFileReader(dir, fm)
	assert.Nil(t, err)
	logR, parsedResult := newTestLogParser(t, entrycollection.NewEntryStore())
	assert.ErrorIs(t, logR.Parse(reader), io.EOF)
	entries, _ := parsedResult.EntryCollection().Entries()
	resultEntries := entries.([]raftpb.Entry)
	assert.Equal(t, numTerm*numEntries, uint64(len(resultEntries)))

	for term := uint64(0); term < numTerm; term++ {
		checkTermFirstEntry := resultEntries[term*numEntries]
		expectNextEntry := expectSeg.termEntries[term].nextEntry
		assert.Equal(t, expectNextEntry.NextTerm, checkTermFirstEntry.Term)
		assert.Equal(t, expectNextEntry.NextIndex, checkTermFirstEntry.Index)
		for eIndex := uint64(0); eIndex < numEntries; eIndex++ {
			checkEntry := resultEntries[term*numEntries+eIndex]
			assert.Equal(t, expectNextEntry.NextTerm, checkEntry.Term)
			assert.Equal(t, expectNextEntry.NextIndex+eIndex, checkEntry.Index)

			expectEntry := expectSeg.termEntries[term].entries[eIndex]
			assert.Equal(t, expectEntry.Index, checkEntry.Index)
			assert.Equal(t, expectEntry.Term, checkEntry.Term)
			assert.Equal(t, expectEntry.Type, checkEntry.Type)
			assert.Equal(t, expectEntry.Data, checkEntry.Data)
		}

	}
	assert.Equal(t, expectSeg.metadata[0], parsedResult.Metadata())
	assert.Equal(t, logW.LastCrc(), parsedResult.LastValidLogCrc())
}
