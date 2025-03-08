package babuzawal

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	collection2 "github.com/fanaujie/babuza/pkg/wal/babuzawal/collection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	player2 "github.com/fanaujie/babuza/pkg/wal/babuzawal/player"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

var (
	testWalMgrConfig = logfile.ManagerConfig{
		LogFileChunkSize:  4 * 1024 * 1024,
		AlignmentPageSize: 4096,
		PageWriterBufSize: 4096 * 8,
	}
)

func genLogFiles(t *testing.T, cfg logfile.ManagerConfig, enableEntryIndexStorage bool, cp *allocator.TwoLevelPool,
	metadata []byte, segs, entriesInSeg, entrySize uint64) (*Wal, []raftpb.Entry, uint64) {

	logMgr, err := logfile.NewManager(cfg, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)

	if enableEntryIndexStorage {
		w.SetEntryIndexStorage(entrystore.NewStorage(w.logMgr))
	}
	var expect []raftpb.Entry

	var lastEntryIndex uint64
	for e := uint64(0); e < segs; e++ {
		var newEnts []raftpb.Entry
		for i := uint64(0); i < entriesInSeg; i++ {
			data := make([]byte, entrySize)
			rand.Read(data)
			ent := raftpb.Entry{
				Term:  1,
				Index: (e * entriesInSeg) + i + 1,
				Type:  raftpb.EntryNormal,
				Data:  data,
			}
			newEnts = append(newEnts, ent)

			lastEntryIndex = ent.Index
		}
		expect = append(expect, newEnts...)
		assert.Nil(t, w.saveEntry(newEnts))
		if e+1 < segs {
			assert.Nil(t, w.cycle())
		}
	}
	assert.Nil(t, w.Sync())
	return w, expect, lastEntryIndex
}

func TestWal_Create(t *testing.T) {

	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	t.Run("success", func(t *testing.T) {
		p, err := ioutil.TempDir("", "wal-test")
		assert.Nil(t, err)
		defer os.RemoveAll(p)
		testWalMgrConfig.WalDir = p
		logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
		assert.Nil(t, err)
		w, err := CreateWal(metadata, logMgr)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		lastLogDesc := w.tailLogFileDesc()
		assert.Equal(t, uint64(0), lastLogDesc.Id)
		assert.Equal(t, uint64(0), lastLogDesc.StartLogIndex)
		fileSize, err := fileutil.FileSize(filepath.Join(p, lastLogDesc.GetLogFileName()))
		assert.Nil(t, err)
		assert.Equal(t, int64(testWalMgrConfig.LogFileChunkSize), fileSize)
	})

	t.Run("failure: exist tmp file", func(t *testing.T) {
		p, err := ioutil.TempDir("", "wal-test")
		assert.Nil(t, err)
		defer os.RemoveAll(p)
		testWalMgrConfig.WalDir = p
		desc := iwal.LogFileDesc{
			Id:            0,
			StartLogIndex: 0,
		}
		assert.Nil(t, ioutil.WriteFile(filepath.Join(p, desc.GetLogFileName())+".tmp",
			[]byte{1, 2, 3, 4}, os.ModeTemporary))
		logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
		assert.Nil(t, err)
		_, err = CreateWal(metadata, logMgr)
		assert.Error(t, err)
	})

	t.Run("success: replace the existing wal file ", func(t *testing.T) {
		p, err := ioutil.TempDir("", "wal-test")
		assert.Nil(t, err)
		defer os.RemoveAll(p)
		testWalMgrConfig.WalDir = p
		desc := iwal.LogFileDesc{
			Id:            0,
			StartLogIndex: 0,
		}
		wData := []byte{1, 2, 3, 4}
		assert.Nil(t, ioutil.WriteFile(filepath.Join(p, desc.GetLogFileName()),
			wData, os.ModeTemporary))
		logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
		assert.Nil(t, err)
		w, err := CreateWal(metadata, logMgr)
		assert.Nil(t, err)
		defer w.Close()

		rData, err := ioutil.ReadFile(filepath.Join(p, desc.GetLogFileName()))
		assert.Nil(t, err)
		assert.NotEqual(t, rData, wData)
	})

}

func TestMakeBroken_File(t *testing.T) {
	//p, err := ioutil.TempDir("", "wal-test")
	//assert.Nil(t, err)
	//defer os.RemoveAll(p)
	//cfg := Config{
	//	WalDir:          p,
	//	SegmentFileSize: 4 * 1024 * 1024,
	//	AlignmentPageSize:        4096,
	//}
	//cp, err := pool.NewDefaultTwoLevelPool(4096, 1024*1024)
	//assert.Nil(t, err)
	//LogFileDesc := []byte{1, 2, 3, 4}
	//w, err := CreateTempLogFile(cfg, cp, LogFileDesc)
	//assert.Nil(t, err)
	//assert.Nil(t, w.Teardown())
	//
	//makeBrokenFile()
}

func TestWal_Save(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	hs := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	entries := []raftpb.Entry{
		{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}
	assert.Nil(t, w.Save(hs, entries))
	assert.Nil(t, w.Sync())
	assert.Nil(t, w.Close())

	pr := player2.NewReplayResult(collection2.NewEntry())
	replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
	assert.Nil(t, err)
	assert.Nil(t, replay.Replay(pr, false))
	assert.Equal(t, hs, pr.HardState())
	ents, _ := pr.EntryCollection().Entries()
	assert.Equal(t, entries, ents.([]raftpb.Entry))

}

func TestWal_SaveSnapshot(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	snap := raftpb.Snapshot{
		Data: nil,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{},
			Index:     1,
			Term:      1,
		},
	}
	assert.Nil(t, w.SaveSnapshot(snap))
	assert.Nil(t, w.Sync())
	assert.Nil(t, w.Close())
	pr := player2.NewReplayResult(collection2.NewEntry())

	walsnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
	assert.Nil(t, err)
	assert.Nil(t, replay.Replay(pr, false))
	assert.Equal(t, walsnap, pr.WalSnapshots()[1])
}

func TestWal_SaveSnapshot_Cycle(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	snap := raftpb.Snapshot{
		Data: nil,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: raftpb.ConfState{},
			Index:     100,
			Term:      1,
		},
	}
	assert.Nil(t, w.SaveSnapshot(snap))
	assert.Nil(t, w.cycle())
	assert.Nil(t, w.Sync())
	assert.Nil(t, w.Close())
	wals, err := utility.ReadValidLogFiles(testWalMgrConfig.WalDir, EmptyWalpbSnapshot)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(wals))
	assert.Equal(t, uint64(101), wals[1].StartLogIndex)
}

func TestWal_ReadEntriesData(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	metadata := []byte{1, 2, 3, 4}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	testSegs := uint64(8)
	testEntries := uint64(64)
	w, expect, _ := genLogFiles(t, testWalMgrConfig, true, cp, metadata, testSegs, testEntries, 57)
	defer func() {
		assert.Nil(t, w.Close())
	}()
	s := w.entryIndexStorage.(*entrystore.Storage)
	readEntryIndex := s.EntryIndex()
	copyEnts := make([]raftpb.Entry, len(readEntryIndex))
	for i := 0; i < len(readEntryIndex); i++ {
		e := &readEntryIndex[i]
		copyEnts[i] = raftpb.Entry{
			Term:  e.Term,
			Index: e.Index,
			Type:  e.Type,
		}
	}
	assert.Nil(t, w.logMgr.ReadEntriesData(readEntryIndex, copyEnts))
	assert.Equal(t, expect, copyEnts)
}
func TestWal_ReadEntriesData_Fail(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	metadata := []byte{1, 2, 3, 4}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	w, _, _ := genLogFiles(t, testWalMgrConfig, true, cp, metadata, 1, 1, 8)
	defer func() {
		assert.Nil(t, w.Close())
	}()

	//invalid file id
	m := entrystore.EntryIndex{
		Term:  1,
		Index: 1,
		Type:  raftpb.EntryNormal,
		EntryDataMetadata: entrystore.EntryDataMetadata{
			FileId: 13,
		},
	}
	e := raftpb.Entry{
		Term:  m.Term,
		Index: m.Index,
		Type:  m.Type,
	}
	assert.Error(t, w.logMgr.ReadEntriesData([]entrystore.EntryIndex{m}, []raftpb.Entry{e}))

	//zero size
	assert.Error(t, w.logMgr.ReadEntriesData(nil, nil))

	//size did not match
	assert.Error(t, w.logMgr.ReadEntriesData([]entrystore.EntryIndex{m}, []raftpb.Entry{e, e}))

}

func TestWal_Open(t *testing.T) {

	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	metadata := []byte{1, 2, 3, 4}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	for _, tc := range []struct {
		segs             uint64
		entriesInSeg     uint64
		dataSize         uint64
		enableEntryIndex bool
		p                iwal.EntryCollection
		validateEntryFun func(t *testing.T, w *Wal, expect []raftpb.Entry, result *player2.ReplayResult)
	}{
		{
			segs:             8,
			entriesInSeg:     64,
			dataSize:         32,
			enableEntryIndex: false,
			p:                collection2.NewEntry(),
			validateEntryFun: func(t *testing.T, w *Wal, expect []raftpb.Entry, result *player2.ReplayResult) {
				ents, _ := result.EntryCollection().Entries()
				assert.Equal(t, expect, ents.([]raftpb.Entry))
			},
		},
		{
			segs:             8,
			entriesInSeg:     64,
			dataSize:         32,
			enableEntryIndex: true,
			p:                collection2.NewEntryIndex(),
			validateEntryFun: func(t *testing.T, w *Wal, expect []raftpb.Entry, result *player2.ReplayResult) {
				s := w.entryIndexStorage.(*entrystore.Storage)
				readEntryIndex := s.EntryIndex()
				copyEnts := make([]raftpb.Entry, len(readEntryIndex))
				for i := 0; i < len(readEntryIndex); i++ {
					e := &readEntryIndex[i]
					copyEnts[i] = raftpb.Entry{
						Term:  e.Term,
						Index: e.Index,
						Type:  e.Type,
					}
				}
				assert.Nil(t, w.logMgr.ReadEntriesData(readEntryIndex, copyEnts))
				assert.Equal(t, expect, copyEnts)
			},
		},
	} {
		assert.Nil(t, os.RemoveAll(p))
		assert.Nil(t, os.Mkdir(p, fileutil.DirMode))
		w, expect, lastEntryIndex := genLogFiles(t, testWalMgrConfig, tc.enableEntryIndex, cp, metadata, tc.segs, tc.entriesInSeg, tc.dataSize)
		assert.Nil(t, w.Close())
		result := player2.NewReplayResult(tc.p)

		replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
		assert.Nil(t, err)
		assert.Nil(t, replay.Replay(result, false))

		logMgr, err := logfile.NewManagerWithScan(testWalMgrConfig, EmptyWalpbSnapshot, cp)
		assert.Nil(t, err)

		ow, err := OpenWal(logMgr, result)
		assert.Nil(t, err)
		if tc.enableEntryIndex {
			es := entrystore.NewStorage(ow.logMgr)
			ents, _ := result.EntryCollection().Entries()
			es.AppendEntryIndex(ents.([]entrystore.EntryIndex))
			ow.SetEntryIndexStorage(es)
		}
		assert.Equal(t, ow.currentLogFile.LastCrc(), result.LastValidLogCrc())
		assert.Equal(t, metadata, result.Metadata())
		tc.validateEntryFun(t, ow, expect, result)

		// wal continues to write
		var newEnts []raftpb.Entry
		for i := uint64(0); i < tc.entriesInSeg; i++ {
			data := make([]byte, tc.dataSize)
			rand.Read(data)
			lastEntryIndex++
			ent := raftpb.Entry{
				Term:  2,
				Index: lastEntryIndex,
				Type:  raftpb.EntryNormal,
				Data:  data,
			}
			newEnts = append(newEnts, ent)
		}
		expect = append(expect, newEnts...)
		assert.Nil(t, ow.saveEntry(newEnts))
		assert.Nil(t, ow.Sync())
		assert.Nil(t, ow.Close())
		result = player2.NewReplayResult(tc.p)

		replay, err = player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
		assert.Nil(t, err)
		assert.Nil(t, replay.Replay(result, false))

		logMgr, err = logfile.NewManagerWithScan(testWalMgrConfig, EmptyWalpbSnapshot, cp)
		assert.Nil(t, err)
		ow, err = OpenWal(logMgr, result)
		assert.Nil(t, err)
		if tc.enableEntryIndex {
			es := entrystore.NewStorage(ow.logMgr)
			ents, _ := result.EntryCollection().Entries()
			es.AppendEntryIndex(ents.([]entrystore.EntryIndex))
			ow.SetEntryIndexStorage(es)
		}
		assert.Nil(t, ow.Close())
		assert.Equal(t, ow.currentLogFile.LastCrc(), result.LastValidLogCrc())
		assert.Equal(t, metadata, result.Metadata())
		tc.validateEntryFun(t, ow, expect, result)
	}

}

func TestWal_Open_Snapshot(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	w.SetEntryIndexStorage(entrystore.NewStorage(w.logMgr))
	var testSegs uint64 = 8
	var expect []raftpb.Entry
	for i := uint64(0); i < testSegs; i++ {
		snap := raftpb.Snapshot{
			Data: nil,
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{},
				Index:     i,
			},
		}
		assert.Nil(t, w.SaveSnapshot(snap))
		entries := []raftpb.Entry{
			{
				Term:  1,
				Index: i,
				Type:  raftpb.EntryNormal,
				Data:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
		}
		assert.Nil(t, w.saveEntry(entries))
		assert.Nil(t, w.cycle())
		expect = append(expect, entries...)
	}
	assert.Nil(t, w.Sync())
	assert.Nil(t, w.Close())

	for e := uint64(0); e < testSegs; e++ {
		for _, pr := range []struct {
			result           iwal.ReplayWalResult
			validateEntryFun func(t *testing.T, w *Wal, segId uint64, result iwal.ReplayWalResult)
		}{
			{
				result: player2.NewReplayResult(collection2.NewEntry()),
				validateEntryFun: func(t *testing.T, w *Wal, segId uint64, result iwal.ReplayWalResult) {
					ents, _ := result.EntryCollection().Entries()
					entries := ents.([]raftpb.Entry)
					assert.Equal(t, testSegs-1-segId, uint64(len(entries)))
					for eIndex, ent := range entries {
						assert.Equal(t, segId+uint64(eIndex)+1, ent.Index)
					}
				},
			},
			{
				result: player2.NewReplayResult(collection2.NewEntryIndex()),
				validateEntryFun: func(t *testing.T, w *Wal, segId uint64, result iwal.ReplayWalResult) {
					ents, _ := result.EntryCollection().Entries()
					entries := ents.([]entrystore.EntryIndex)
					assert.Equal(t, testSegs-1-segId, uint64(len(entries)))
					for eIndex, ent := range entries {
						assert.Equal(t, segId+uint64(eIndex)+1, ent.Index)
					}
				},
			},
		} {
			replay, err := player2.Create(testWalMgrConfig.WalDir, walpb.Snapshot{Index: e}, cp)
			assert.Nil(t, err)
			assert.Nil(t, replay.Replay(pr.result, false))
			logMgr, err := logfile.NewManagerWithScan(testWalMgrConfig, walpb.Snapshot{Index: e}, cp)
			assert.Nil(t, err)
			ow, err := OpenWal(logMgr, pr.result)
			assert.Nil(t, err)
			assert.NotNil(t, ow)
			assert.Equal(t, metadata, pr.result.Metadata())
			pr.validateEntryFun(t, ow, e, pr.result)
			assert.Nil(t, ow.Close())
		}
	}
}

func TestWal_NextEntryChange(t *testing.T) {

	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	var testEntries uint64 = 64
	var expect []raftpb.Entry
	for eIndex := uint64(1); eIndex <= testEntries; eIndex++ {
		data := make([]byte, 1024)
		rand.Read(data)
		ent := raftpb.Entry{
			Term:  eIndex,
			Index: eIndex,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		expect = append(expect, ent)
	}
	assert.Nil(t, w.saveEntry(expect))
	assert.Nil(t, w.saveHardState(raftpb.HardState{}))
	assert.Nil(t, w.cycle())
	assert.Nil(t, w.Close())
	pr := player2.NewReplayResult(collection2.NewEntry())
	replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
	assert.Nil(t, err)
	assert.Nil(t, replay.Replay(pr, false))

	assert.Equal(t, metadata, pr.Metadata())
	ents, _ := pr.EntryCollection().Entries()
	entries := ents.([]raftpb.Entry)
	assert.Equal(t, expect, entries)
}

func TestWal_NextEntry_NotContinuous(t *testing.T) {
	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	defer w.Close()
	var expect []raftpb.Entry
	for _, ne := range []struct {
		pb.WalNextEntry
		totalEntries uint64
		validEntries uint64
	}{
		{
			WalNextEntry: pb.WalNextEntry{
				NextTerm:  1,
				NextIndex: 1,
			},
			totalEntries: 8,
			validEntries: 5,
		},
		{
			WalNextEntry: pb.WalNextEntry{
				NextTerm:  2,
				NextIndex: 6,
			},
			totalEntries: 16,
			validEntries: 10,
		},
		{
			WalNextEntry: pb.WalNextEntry{
				NextTerm:  3,
				NextIndex: 16,
			},
			totalEntries: 10,
			validEntries: 10,
		},
	} {
		var batchEnts []raftpb.Entry
		for eIndex := ne.NextIndex; eIndex < ne.NextIndex+ne.totalEntries; eIndex++ {
			data := make([]byte, 64)
			rand.Read(data)
			if eIndex < ne.NextIndex+ne.validEntries {
				ent := raftpb.Entry{
					Term:  ne.NextTerm,
					Index: eIndex,
					Type:  raftpb.EntryNormal,
					Data:  data,
				}
				expect = append(expect, ent)
				batchEnts = append(batchEnts, ent)
			}
		}
		assert.Nil(t, w.Save(raftpb.HardState{}, batchEnts))
	}
	assert.Nil(t, w.Sync())
	pr := player2.NewReplayResult(collection2.NewEntry())
	replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
	assert.Nil(t, err)
	assert.Nil(t, replay.Replay(pr, false))

	assert.Equal(t, pb.WalNextEntry{
		NextTerm:  3,
		NextIndex: 26,
	}, pr.NextEntry())
	ents, _ := pr.EntryCollection().Entries()
	entries := ents.([]raftpb.Entry)
	assert.Equal(t, expect, entries)
}

func TestWal_CoverEntries(t *testing.T) {

	p, err := ioutil.TempDir("", "wal-test")
	assert.Nil(t, err)
	defer os.RemoveAll(p)
	testWalMgrConfig.WalDir = p
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte{1, 2, 3, 4}
	logMgr, err := logfile.NewManager(testWalMgrConfig, cp)
	assert.Nil(t, err)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(t, err)
	var testEntries uint64 = 64
	var expect []raftpb.Entry
	var coverEntryIndex uint64 = testEntries / 2

	var entries []raftpb.Entry
	for eIndex := uint64(0); eIndex < testEntries; eIndex++ {
		data := make([]byte, 1024)
		rand.Read(data)
		ent := raftpb.Entry{
			Term:  1,
			Index: eIndex + 1,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entries = append(entries, ent)
	}
	assert.Nil(t, w.saveEntry(entries))
	assert.Nil(t, w.saveHardState(raftpb.HardState{}))
	assert.Nil(t, w.cycle())
	expect = append(expect, entries[:coverEntryIndex]...)

	//cover
	entries = entries[:0]
	for eIndex := coverEntryIndex; eIndex < testEntries; eIndex++ {
		data := make([]byte, 1024)
		rand.Read(data)
		ent := raftpb.Entry{
			Term:  2,
			Index: eIndex + 1,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entries = append(entries, ent)
	}
	assert.Nil(t, w.saveEntry(entries))
	assert.Nil(t, w.saveHardState(raftpb.HardState{}))
	assert.Nil(t, w.Sync())
	assert.Nil(t, w.Close())
	expect = append(expect, entries...)
	pr := player2.NewReplayResult(collection2.NewEntry())

	replay, err := player2.Create(testWalMgrConfig.WalDir, EmptyWalpbSnapshot, cp)
	assert.Nil(t, err)
	assert.Nil(t, replay.Replay(pr, false))

	assert.Equal(t, metadata, pr.Metadata())
	ents, _ := pr.EntryCollection().Entries()
	entries = ents.([]raftpb.Entry)
	assert.Equal(t, expect, entries)
}
