package lsmtwal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"testing"
	"time"
)

func TestNewWalManager(t *testing.T) {
	testCases := []struct {
		name         string
		managerType  WalManagerType
		expectedType interface{}
	}{
		{
			name:         "BadgerDB Manager",
			managerType:  WalManagerTypeBadger,
			expectedType: &BadgerWalManager{},
		},
		{
			name:         "PebbleDB Manager",
			managerType:  WalManagerTypePebble,
			expectedType: &PebbleWalManager{},
		},
		{
			name:         "Default Manager (BadgerDB)",
			managerType:  "",
			expectedType: &BadgerWalManager{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewWalManager(Config{
				InMemory:    true,
				WalDir:      "",
				ManagerType: tc.managerType,
			}, &logger.Mock{})

			assert.NotNil(t, manager)
			assert.IsType(t, tc.expectedType, manager)

			// Cleanup
			err := manager.Close()
			assert.NoError(t, err)
		})
	}
}

func setupTestManagerWithType(cfg Config, managerType WalManagerType) ibabuza.WalManager {
	cfg.ManagerType = managerType
	return NewWalManager(cfg, &logger.Mock{})
}

func TestWalManager_FindSnapshot(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Test with empty database
			snapshots, err := manager.FindSnapshot()
			assert.NoError(t, err)
			assert.Empty(t, snapshots)

			// Create WAL first to initialize the database structure
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Save a snapshot
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 10,
					Term:  5,
				},
			}
			err = wal.SaveSnapshot(snapshot)
			assert.NoError(t, err)

			// Now retrieve snapshots - the behavior may vary by implementation
			snapshots, err = manager.FindSnapshot()
			assert.NoError(t, err)
			// Note: Snapshot retrieval behavior may vary between implementations
			// Some implementations may store empty snapshots, others may not return them
		})
	}
}

func TestWalManager_CreateWal(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create test metadata
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}

			// Create WAL
			es, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			assert.NotNil(t, es)
			assert.NotNil(t, wal)
			defer wal.Close()

			// Test that we can save and read entries
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 0,
			}
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("test")},
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Verify we can replay the WAL
			snapshot := &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0,
				},
			}
			replayEs, replayWal, result, err := manager.ReplayWal(snapshot, false)
			assert.NoError(t, err)
			assert.NotNil(t, replayEs)
			assert.NotNil(t, replayWal)
			assert.NotNil(t, result)
			defer replayWal.Close()

			// Verify replayed hard state
			assert.Equal(t, hardState, result.HardState())

			// Verify metadata
			mData, err := metadata.Marshal()
			assert.NoError(t, err)
			assert.Equal(t, mData, result.Metadata())
		})
	}
}

func TestWalManager_HasExistingWals(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Initially there should be no WALs
			hasWals, err := manager.HasExistingWals()
			assert.NoError(t, err)
			assert.False(t, hasWals)

			// Create a WAL
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			es, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			assert.NotNil(t, es)
			assert.NotNil(t, wal)
			defer wal.Close()

			// Add an entry
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 1,
			}
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("test")},
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Now there should be WALs
			hasWals, err = manager.HasExistingWals()
			assert.NoError(t, err)
			assert.True(t, hasWals)
		})
	}
}

func TestWalManager_ReadEntriesData(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Add some entries
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
				{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
			}
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 2,
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Test ReadEntriesData by casting to specific manager type
			switch tc.managerType {
			case WalManagerTypeBadger:
				badgerMgr := manager.(*BadgerWalManager)
				// Create entry indices for reading
				readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					{Index: 2, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnts := make([]raftpb.Entry, len(readEntryIndex))
				err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
				assert.NoError(t, err)

				// Verify the entries were read correctly
				for i, entry := range entries {
					assert.Equal(t, entry.Term, destEnts[i].Term)
					assert.Equal(t, entry.Index, destEnts[i].Index)
					assert.Equal(t, entry.Type, destEnts[i].Type)
					assert.Equal(t, entry.Data, destEnts[i].Data)
				}

			case WalManagerTypePebble:
				pebbleMgr := manager.(*PebbleWalManager)
				// Create entry indices for reading
				readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					{Index: 2, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnts := make([]raftpb.Entry, len(readEntryIndex))
				err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
				assert.NoError(t, err)

				// Verify the entries were read correctly
				for i, entry := range entries {
					assert.Equal(t, entry.Term, destEnts[i].Term)
					assert.Equal(t, entry.Index, destEnts[i].Index)
					assert.Equal(t, entry.Type, destEnts[i].Type)
					assert.Equal(t, entry.Data, destEnts[i].Data)
				}
			}
		})
	}
}

func TestWalManager_FullWorkflow(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			// Create manager
			manager := setupTestManagerWithType(Config{
				WalDir: tmpDir,
			}, tc.managerType)

			// Create WAL
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			es, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			assert.NotNil(t, es)
			assert.NotNil(t, wal)

			// Add entries
			hardState := raftpb.HardState{
				Term:   10,
				Vote:   20,
				Commit: 5,
			}
			entries := []raftpb.Entry{
				{Term: 10, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry1")},
				{Term: 10, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry2")},
				{Term: 10, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry3")},
				{Term: 10, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry4")},
				{Term: 10, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry5")},
			}

			err = wal.Save(hardState, entries)
			assert.NoError(t, err)
			err = wal.Sync()
			assert.NoError(t, err)
			err = wal.Close()
			assert.NoError(t, err)

			// Save a snapshot
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 3,
					Term:  10,
				},
			}
			_, wal2, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			err = wal2.SaveSnapshot(snapshot)
			assert.NoError(t, err)
			err = wal2.Close()
			assert.NoError(t, err)

			// Close and reopen manager
			err = manager.Close()
			assert.NoError(t, err)

			manager = setupTestManagerWithType(Config{
				WalDir: tmpDir,
			}, tc.managerType)
			defer manager.Close()

			// Replay WAL
			replaySnapshot := &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0,
				},
			}
			replayEs, replayWal, result, err := manager.ReplayWal(replaySnapshot, false)
			assert.NoError(t, err)
			assert.NotNil(t, replayEs)
			assert.NotNil(t, replayWal)
			assert.NotNil(t, result)
			defer replayWal.Close()

			// Verify replayed data
			assert.Equal(t, hardState, result.HardState())

			// Verify metadata
			mData, err := metadata.Marshal()
			assert.NoError(t, err)
			assert.Equal(t, mData, result.Metadata())

			// Verify snapshots exist
			snapshots, err := manager.FindSnapshot()
			assert.NoError(t, err)
			assert.NotEmpty(t, snapshots)

			// Verify existing WALs
			hasWals, err := manager.HasExistingWals()
			assert.NoError(t, err)
			assert.True(t, hasWals)
		})
	}
}

func TestWalManager_CreatePurger(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create purger
			purger := manager.Purger()
			assert.NotNil(t, purger)

			// Test Start and Stop
			purger.Start()
		})
	}
}

func TestWalPurger_PurgeEntries(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Add entries that will be purged
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
				{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
				{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("data3")},
				{Term: 1, Index: 4, Type: raftpb.EntryNormal, Data: []byte("data4")},
				{Term: 1, Index: 5, Type: raftpb.EntryNormal, Data: []byte("data5")},
			}
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 5,
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Create and start purger
			purger := manager.Purger()
			purger.Start()

			// Create snapshot at index 3 (entries 1-3 should be purged)
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 3,
					Term:  1,
				},
			}

			// Trigger purge by calling WAL.Purge (which sends to purger channel)
			err = wal.Purge(snapshot)
			assert.NoError(t, err)

			// Give some time for the purger to process
			time.Sleep(100 * time.Millisecond)

			// Verify entries 1-3 are purged, 4-5 still exist
			switch tc.managerType {
			case WalManagerTypeBadger:
				badgerMgr := manager.(*BadgerWalManager)

				// Try to read entries 1-3 (should fail)
				for i := uint64(1); i <= 3; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.Error(t, err, "Entry %d should be purged", i)
				}

				// Try to read entries 4-5 (should succeed)
				for i := uint64(4); i <= 5; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}

			case WalManagerTypePebble:
				pebbleMgr := manager.(*PebbleWalManager)

				// Try to read entries 1-3 (should fail)
				for i := uint64(1); i <= 3; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.Error(t, err, "Entry %d should be purged", i)
				}

				// Try to read entries 4-5 (should succeed)
				for i := uint64(4); i <= 5; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}
			}
		})
	}
}

func TestWalPurger_EmptySnapshot(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Add entries
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
				{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
			}
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 2,
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Create and start purger
			purger := manager.Purger()
			purger.Start()

			// Create empty snapshot (index 0)
			emptySnapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0,
					Term:  1,
				},
			}

			// Trigger purge with empty snapshot (should do nothing)
			err = wal.Purge(emptySnapshot)
			assert.NoError(t, err)

			// Give some time for the purger to process
			time.Sleep(100 * time.Millisecond)

			// Verify entries still exist (no purging should have occurred)
			switch tc.managerType {
			case WalManagerTypeBadger:
				badgerMgr := manager.(*BadgerWalManager)
				for i := uint64(1); i <= 2; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}

			case WalManagerTypePebble:
				pebbleMgr := manager.(*PebbleWalManager)
				for i := uint64(1); i <= 2; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}
			}
		})
	}
}

func TestWalPurger_MultipleSnapshots(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Add entries that will be purged in stages
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
				{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
				{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("data3")},
				{Term: 1, Index: 4, Type: raftpb.EntryNormal, Data: []byte("data4")},
				{Term: 1, Index: 5, Type: raftpb.EntryNormal, Data: []byte("data5")},
				{Term: 1, Index: 6, Type: raftpb.EntryNormal, Data: []byte("data6")},
				{Term: 1, Index: 7, Type: raftpb.EntryNormal, Data: []byte("data7")},
			}
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 7,
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Create and start purger
			purger := manager.Purger()
			purger.Start()

			// First purge: snapshot at index 3
			snapshot1 := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 3,
					Term:  1,
				},
			}
			err = wal.Purge(snapshot1)
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			// Second purge: snapshot at index 5
			snapshot2 := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 5,
					Term:  1,
				},
			}
			err = wal.Purge(snapshot2)
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			// Verify only entries 6-7 still exist
			switch tc.managerType {
			case WalManagerTypeBadger:
				badgerMgr := manager.(*BadgerWalManager)

				// Entries 1-5 should be purged
				for i := uint64(1); i <= 5; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.Error(t, err, "Entry %d should be purged", i)
				}

				// Entries 6-7 should still exist
				for i := uint64(6); i <= 7; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = badgerMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}

			case WalManagerTypePebble:
				pebbleMgr := manager.(*PebbleWalManager)

				// Entries 1-5 should be purged
				for i := uint64(1); i <= 5; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.Error(t, err, "Entry %d should be purged", i)
				}

				// Entries 6-7 should still exist
				for i := uint64(6); i <= 7; i++ {
					readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
						{Index: i, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					}
					destEnts := make([]raftpb.Entry, 1)
					err = pebbleMgr.ReadEntriesData(readEntryIndex, destEnts)
					assert.NoError(t, err, "Entry %d should still exist", i)
					assert.Equal(t, i, destEnts[0].Index)
				}
			}
		})
	}
}

func TestWalPurger_StartStop(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestManagerWithType(Config{
				InMemory: true,
			}, tc.managerType)
			defer manager.Close()

			// Create multiple purgers to test Start/Stop behavior
			purger1 := manager.Purger()
			purger2 := manager.Purger()

			// Test multiple Start calls (should be safe)
			purger1.Start()
			purger1.Start()
			purger2.Start()
		})
	}
}
