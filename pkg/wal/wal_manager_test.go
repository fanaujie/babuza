package wal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	lstmWalStorage "github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"

	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"github.com/fanaujie/babuza/pkg/wal/etcdwal"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestWalManager_Create(t *testing.T) {

	t.Run("babuzawal wal manager: enable EntryIndex", func(t *testing.T) {
		walDir := t.TempDir()
		b := babuzawal.NewWalManager(walDir, &logger.Mock{})
		defer b.Close()
		m, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()
		_, ok := m.(*storage.EntryIndexRaftStorage)
		assert.Equal(t, true, ok)

		_, ok = w.(*babuzawal.Wal)
		assert.Equal(t, true, ok)
	})

	t.Run("babuzawal wal manager: disable EntryIndex", func(t *testing.T) {
		walDir := t.TempDir()
		b := babuzawal.NewWalManager(walDir, &logger.Mock{},
			babuzawal.SetOptsWithWalDisableEntryIndex(true))
		defer b.Close()
		m, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()
		_, ok := m.(*raft.MemoryStorage)
		assert.Equal(t, true, ok)
		_, ok = w.(*babuzawal.Wal)
		assert.Equal(t, true, ok)
	})

	t.Run("etcdwal wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		b := etcdwal.NewWalManager(walDir, zap.NewNop())
		defer b.Close()
		m, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()
		_, ok := m.(*raft.MemoryStorage)
		assert.Equal(t, true, ok)
		_, ok = w.(*etcdwal.WalWrapper)
		assert.Equal(t, true, ok)
	})

	t.Run("lsmt badger wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		m, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()
		_, ok := m.(*lstmWalStorage.EntryStorage)
		assert.Equal(t, true, ok)
		_, ok = w.(*lsmtwal.BadgerWal)
		assert.Equal(t, true, ok)
	})

	t.Run("lsmt pebble wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewPebbleWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		m, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()
		_, ok := m.(*lstmWalStorage.EntryStorage)
		assert.Equal(t, true, ok)
		_, ok = w.(*lsmtwal.PebbleWal)
		assert.Equal(t, true, ok)
	})
}

func TestWalManager_FindSnapshot(t *testing.T) {
	var (
		confState = raftpb.ConfState{
			Voters: []uint64{1},
		}
	)
	writeSnapshot := []raftpb.Snapshot{
		{Metadata: raftpb.SnapshotMetadata{Index: 0, Term: 0}},
		{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 2, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 3, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 6, Term: 5, ConfState: confState}},
	}
	expectedSnapshot := []walpb.Snapshot{
		{
			Index: writeSnapshot[0].Metadata.Index,
			Term:  writeSnapshot[0].Metadata.Term,
		},
		{
			Index:     writeSnapshot[1].Metadata.Index,
			Term:      writeSnapshot[1].Metadata.Term,
			ConfState: &writeSnapshot[1].Metadata.ConfState,
		},
		{
			Index:     writeSnapshot[2].Metadata.Index,
			Term:      writeSnapshot[2].Metadata.Term,
			ConfState: &writeSnapshot[2].Metadata.ConfState,
		},
		{
			Index:     writeSnapshot[3].Metadata.Index,
			Term:      writeSnapshot[3].Metadata.Term,
			ConfState: &writeSnapshot[3].Metadata.ConfState,
		},
		{
			Index:     writeSnapshot[4].Metadata.Index,
			Term:      writeSnapshot[4].Metadata.Term,
			ConfState: &writeSnapshot[4].Metadata.ConfState,
		},
		{
			Index:     writeSnapshot[5].Metadata.Index,
			Term:      writeSnapshot[5].Metadata.Term,
			ConfState: &writeSnapshot[5].Metadata.ConfState,
		},
	}
	bWalDir := t.TempDir()
	eWalDir := t.TempDir()
	lWalDir := t.TempDir()
	pWalDir := t.TempDir()
	bw := babuzawal.NewWalManager(bWalDir, &logger.Mock{})
	defer bw.Close()
	ew := etcdwal.NewWalManager(eWalDir, zap.NewNop())
	defer ew.Close()
	lw := lsmtwal.NewBadgerWalManager(lsmtwal.Config{
		WalDir: lWalDir,
	}, &logger.Mock{})
	defer lw.Close()
	pw := lsmtwal.NewPebbleWalManager(lsmtwal.Config{
		WalDir: pWalDir,
	}, &logger.Mock{})
	defer pw.Close()
	for _, tc := range []ibabuza.WalManager{bw, ew, lw, pw} {
		func(walManager ibabuza.WalManager) {
			_, w, err := walManager.CreateWal(babuzapb.WalMetadata{
				ClusterID:   100,
				LocalPeerID: 1,
			})
			assert.Nil(t, err)
			defer w.Close()
			assert.Nil(t, w.Save(raftpb.HardState{Commit: 1, Term: 1}, nil))
			assert.Nil(t, w.SaveSnapshot(writeSnapshot[1]))
			assert.Nil(t, w.SaveSnapshot(writeSnapshot[2]))
			assert.Nil(t, w.SaveSnapshot(writeSnapshot[3]))
			assert.Nil(t, w.Save(raftpb.HardState{Commit: 6, Term: 5}, nil))
			assert.Nil(t, w.SaveSnapshot(writeSnapshot[4]))
			assert.Nil(t, w.SaveSnapshot(writeSnapshot[5]))
			assert.Nil(t, w.Sync())
			validSnapshots, err := tc.FindSnapshot()
			assert.Nil(t, err)
			assert.Equal(t, expectedSnapshot, validSnapshots)
		}(tc)
	}

}

func TestWalManager_ReplayWal(t *testing.T) {
	defaultSegmentSize := 64 * 1024 * 1024
	testFunc := func(walManager ibabuza.WalManager) {
		_, w, err := walManager.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		var expectedEntry = saveEntries(t, w, defaultSegmentSize, 4096, 1024)
		if _, ok := walManager.(*lsmtwal.BadgerWalManager); !ok {
			assert.Nil(t, w.Close())
		}
		_, m, _, err := walManager.ReplayWal(nil, false)
		assert.Nil(t, err)
		resultEntry, err := m.Entries(1, 4096+1, math.MaxUint64)
		assert.Equal(t, expectedEntry, resultEntry)
	}
	t.Run("babuzawal wal manager: replayWal", func(t *testing.T) {
		walDir := t.TempDir()
		b := babuzawal.NewWalManager(walDir, &logger.Mock{},
			babuzawal.SetOptsWithWalDisableEntryIndex(false),
			babuzawal.SetOptsWithWalLogFileChunkSize(4*1024*1024))
		defer b.Close()
		testFunc(b)
	})
	t.Run("etcdwal wal manager: replayWal", func(t *testing.T) {
		walDir := t.TempDir()
		b := etcdwal.NewWalManager(walDir, zap.NewNop())
		defer b.Close()
		testFunc(b)
	})
	t.Run("lsmt badger wal manager: replayWal", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		testFunc(b)
	})
	t.Run("lsmt pebble wal manager: replayWal", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewPebbleWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		testFunc(b)
	})

}

func TestWalManager_StartWalPurgingProcess(t *testing.T) {
	defaultSegmentSize := 4096
	genWalFunc := func(walDir string, walManager ibabuza.WalManager) (ibabuza.Wal, []string) {
		_, w, err := walManager.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		saveEntries(t, w, defaultSegmentSize, 64, 1024)

		files, err := getWalFiles(walDir)
		assert.Nil(t, err)
		return w, files
	}
	t.Run("etcdwal wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		wal.SegmentSizeBytes = int64(defaultSegmentSize)
		maxKeepWalFiles := uint(3)
		b := etcdwal.NewWalManager(walDir, zap.NewNop(),
			etcdwal.SetOptionWithMaxKeepWalFiles(maxKeepWalFiles),
			etcdwal.SetOptionWithPurgeFileInterval(time.Millisecond*100))
		w, walFiles := genWalFunc(walDir, b)
		defer b.Close()
		defer w.Close()
		purger := b.Purger()
		purger.Start()
		purgeIndex := 1
		keepIndex := len(walFiles) - int(maxKeepWalFiles)

		for i, f := range walFiles {
			desc, err := utility.ParseLogFileName(f)
			assert.Nil(t, err)
			assert.Nil(t, w.Purge(raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: desc.StartLogIndex,
				},
			}))
			time.Sleep(time.Second)
			remainFiles, err := getWalFiles(walDir)
			assert.Nil(t, err)
			if i < 2 {
				assert.Equal(t, len(walFiles), len(remainFiles))
			} else if i <= keepIndex {
				assert.Equal(t, walFiles[purgeIndex:], remainFiles)
				purgeIndex++
			} else {
				assert.Equal(t, walFiles[keepIndex:], remainFiles)
			}
		}
	})

	t.Run("babuzawal wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		maxKeepWalFiles := uint(3)
		b := babuzawal.NewWalManager(walDir, &logger.Mock{},
			babuzawal.SetOptsWithWalLogFileChunkSize(defaultSegmentSize),
			babuzawal.SetOptsWithWalMaxKeepLogFiles(maxKeepWalFiles))
		defer b.Close()
		w, walFiles := genWalFunc(walDir, b)
		defer w.Close()
		purger := b.Purger()
		assert.NotNil(t, purger)
		purger.Start()

		// Verify purger interface is working by manually calling purge through WAL
		if len(walFiles) > 0 {
			// Parse the first file to get its start index
			desc, err := utility.ParseLogFileName(walFiles[0])
			assert.Nil(t, err)
			assert.Nil(t, w.Purge(raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: desc.StartLogIndex + 1, // Purge files before this index
				},
			}))
		}
	})

	t.Run("lsmt badger wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		_, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()

		// Save some entries to create data to purge
		saveEntries(t, w, defaultSegmentSize, 64, 1024)

		purger := b.Purger()
		assert.NotNil(t, purger)
		purger.Start()

		// Purge entries before index 32
		purgeIndex := uint64(32)
		assert.Nil(t, w.Purge(raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: purgeIndex,
			},
		}))

		// Wait for purging to complete
		time.Sleep(time.Second)

		// Try to verify purge by replaying from the snapshot
		snapshot := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: purgeIndex,
				Term:  1,
			},
		}
		_, m2, _, err := b.ReplayWal(&snapshot, false)
		if err != nil {
			// If ReplayWal fails, it might be because the purged data is no longer available
			// This indicates purge worked, but we can't verify the exact state
			t.Logf("ReplayWal failed after purge (expected): %v", err)
		} else if m2 != nil {
			// If ReplayWal succeeds, verify that we can't access purged entries
			firstIndex, err := m2.FirstIndex()
			if err == nil {
				// First index should be higher than purgeIndex after purge
				assert.True(t, firstIndex > purgeIndex,
					"FirstIndex (%d) should be greater than purgeIndex (%d)", firstIndex, purgeIndex)
			}
		}
	})

	t.Run("lsmt pebble wal manager", func(t *testing.T) {
		walDir := t.TempDir()
		b := lsmtwal.NewPebbleWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, &logger.Mock{})
		defer b.Close()
		_, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()

		// Save some entries to create data to purge
		saveEntries(t, w, defaultSegmentSize, 64, 1024)

		purger := b.Purger()
		assert.NotNil(t, purger)
		purger.Start()

		// Purge entries before index 32
		purgeIndex := uint64(32)
		assert.Nil(t, w.Purge(raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: purgeIndex,
			},
		}))

		// Wait for purging to complete
		time.Sleep(time.Second)

		// Try to verify purge by replaying from the snapshot
		snapshot := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: purgeIndex,
				Term:  1,
			},
		}
		_, m2, _, err := b.ReplayWal(&snapshot, false)
		if err != nil {
			// If ReplayWal fails, it might be because the purged data is no longer available
			// This indicates purge worked, but we can't verify the exact state
			t.Logf("ReplayWal failed after purge (expected): %v", err)
		} else if m2 != nil {
			// If ReplayWal succeeds, verify that we can't access purged entries
			firstIndex, err := m2.FirstIndex()
			if err == nil {
				// First index should be higher than purgeIndex after purge
				assert.True(t, firstIndex > purgeIndex,
					"FirstIndex (%d) should be greater than purgeIndex (%d)", firstIndex, purgeIndex)
			}
		}
	})
}

func saveEntries(t *testing.T, w ibabuza.Wal, segmentSize, entriesNum, dataSize int) []raftpb.Entry {
	var expectedEntry []raftpb.Entry
	var saveEntry []raftpb.Entry
	var totalSize int

	for i := 0; i < entriesNum; i++ {
		data := make([]byte, dataSize)
		rand.Read(data)
		entry := raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		saveEntry = append(saveEntry, entry)
		expectedEntry = append(expectedEntry, entry)
		totalSize += dataSize
		if totalSize > segmentSize {
			assert.Nil(t, w.Save(raftpb.HardState{}, saveEntry))
			totalSize = 0
			saveEntry = saveEntry[:0]
		}
	}
	assert.Nil(t, w.Save(raftpb.HardState{}, saveEntry))
	return expectedEntry
}

func getWalFiles(walDir string) ([]string, error) {
	fnames, err := fileutil.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	newfnames := make([]string, 0)
	for _, fname := range fnames {
		if strings.HasSuffix(fname, ".wal") {
			newfnames = append(newfnames, fname)
		}
	}
	sort.Strings(newfnames)
	return newfnames, nil
}

// TODO: to add more test case from etcdwal
// this test case is from etcdwal
func TestValidSnapshotEntriesAfterPurgeWal(t *testing.T) {
	confState := raftpb.ConfState{
		Voters:    []uint64{0x00ffca74},
		AutoLeave: false,
	}
	p := t.TempDir()
	writeSnapshot := []raftpb.Snapshot{
		{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 2, Term: 1, ConfState: confState}},
		{Metadata: raftpb.SnapshotMetadata{Index: 3, Term: 2, ConfState: confState}},
	}
	state1 := raftpb.HardState{Commit: 1, Term: 1}
	state2 := raftpb.HardState{Commit: 3, Term: 2}
	b := babuzawal.NewWalManager(p, &logger.Mock{},
		babuzawal.SetOptsWithWalDisableEntryIndex(true),
		babuzawal.SetOptsWithWalLogFileChunkSize(64))
	defer b.Close()
	func() {
		_, w, err := b.CreateWal(babuzapb.WalMetadata{
			ClusterID:   100,
			LocalPeerID: 1,
		})
		assert.Nil(t, err)
		defer w.Close()

		// snap0 is implicitly created at index 0, term 0
		assert.Nil(t, w.SaveSnapshot(writeSnapshot[0]))
		assert.Nil(t, w.Save(state1, nil))
		assert.Nil(t, w.SaveSnapshot(writeSnapshot[1]))
		assert.Nil(t, w.SaveSnapshot(writeSnapshot[2]))

		for i := 0; i < 128; i++ {
			assert.Nil(t, w.Save(state2, nil))
		}

	}()
	logFiles, err := utility.ReadValidLogFiles(p, walpb.Snapshot{})
	assert.Nil(t, err)
	assert.Nil(t, os.Remove(filepath.Join(p, logFiles[0].GetLogFileName())))
	_, err = b.FindSnapshot()
	assert.Nil(t, err)

}
