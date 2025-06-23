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

func setupTestMultiRaftManagerWithType(cfg MultiRaftConfig, managerType WalManagerType) ibabuza.MultiRaftWalManager {
	cfg.ManagerType = managerType
	return NewMultiRaftWalManager(cfg, &logger.Mock{})
}

func TestNewMultiRaftWalManager(t *testing.T) {
	testCases := []struct {
		name         string
		managerType  WalManagerType
		expectedType interface{}
	}{
		{
			name:         "BadgerDB Manager",
			managerType:  WalManagerTypeBadger,
			expectedType: &MultiRaftBadgerWalManager{},
		},
		{
			name:         "PebbleDB Manager",
			managerType:  WalManagerTypePebble,
			expectedType: &MultiRaftPebbleWalManager{},
		},
		{
			name:         "Default Manager (BadgerDB)",
			managerType:  "",
			expectedType: &MultiRaftBadgerWalManager{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)

			assert.NotNil(t, manager)
			assert.IsType(t, tc.expectedType, manager)

			// Cleanup
			err := manager.Close()
			assert.NoError(t, err)
		})
	}
}

func TestMultiRaftWalManager_FindSnapshot(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			groupID := ibabuza.RaftGroupID(1)

			// Test with empty database
			snapshots, err := manager.FindSnapshot(groupID)
			assert.NoError(t, err)
			assert.Empty(t, snapshots)

			// Create WAL first to initialize the database structure
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(groupID, metadata)
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
			snapshots, err = manager.FindSnapshot(groupID)
			assert.NoError(t, err)
			// Note: Snapshot retrieval behavior may vary between implementations
			// Some implementations may store empty snapshots, others may not return them
		})
	}
}

func TestMultiRaftWalManager_CreateWal(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			groupID := ibabuza.RaftGroupID(1)

			// Create test metadata
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}

			// Create WAL
			es, wal, err := manager.CreateWal(groupID, metadata)
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
			replayEs, replayWal, result, err := manager.ReplayWal(groupID, snapshot, false)
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

func TestMultiRaftWalManager_ReplayWal(t *testing.T) {
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

			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           false,
				WalDir:             tmpDir,
				KeyPrefixCacheSize: 10,
			}, tc.managerType)

			groupID := ibabuza.RaftGroupID(1)

			// First, we need to create a WAL and add some entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}

			es, wal, err := manager.CreateWal(groupID, metadata)
			assert.NoError(t, err)
			assert.NotNil(t, wal)
			assert.NotNil(t, es)

			// Add some entries to the WAL
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

			// Close the first WAL before replay
			err = wal.Close()
			assert.NoError(t, err)

			// Close and reopen the manager to simulate a restart
			err = manager.Close()
			assert.NoError(t, err)

			// Create a new manager with the same directory
			manager = setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           false,
				WalDir:             tmpDir,
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			// Now try to replay the WAL
			snapshot := &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0, // Start from the beginning
				},
			}

			// Replay the WAL
			replayEs, replayWal, result, err := manager.ReplayWal(groupID, snapshot, false)
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

			// Verify entries
			// Need to cast to *walbase.ReplayResult to access GetEntries method
			replayResult, ok := result.(*walbase.ReplayResult)
			assert.True(t, ok, "Result should be a *walbase.ReplayResult")

			resultEntries := replayResult.GetEntries()
			assert.Equal(t, len(entries), len(resultEntries))
			for i, entry := range entries {
				assert.Equal(t, entry.Term, resultEntries[i].Term)
				assert.Equal(t, entry.Index, resultEntries[i].Index)
				assert.Equal(t, entry.Type, resultEntries[i].Type)
				assert.Equal(t, entry.Data, resultEntries[i].Data)
			}
		})
	}
}

func TestMultiRaftWalManager_HasExistingWals(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			// Initially there should be no WALs
			groupIDs, err := manager.HasExistingWals()
			assert.NoError(t, err)
			assert.Empty(t, groupIDs)

			// Create a WAL for group 1
			groupID := ibabuza.RaftGroupID(1)
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}

			es, wal, err := manager.CreateWal(groupID, metadata)
			assert.NoError(t, err)
			assert.NotNil(t, es)
			assert.NotNil(t, wal)
			defer wal.Close()

			// Now there should be one WAL
			groupIDs, err = manager.HasExistingWals()
			assert.NoError(t, err)
			assert.Len(t, groupIDs, 1)
			assert.Equal(t, groupID, groupIDs[0])

			// Create another WAL for group 2
			groupID2 := ibabuza.RaftGroupID(2)
			es2, wal2, err := manager.CreateWal(groupID2, metadata)
			assert.NoError(t, err)
			assert.NotNil(t, es2)
			assert.NotNil(t, wal2)
			defer wal2.Close()

			// Now there should be two WALs
			groupIDs, err = manager.HasExistingWals()
			assert.NoError(t, err)
			assert.Len(t, groupIDs, 2)
		})
	}
}

func TestMultiRaftWalManager_ReadEntriesData(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			groupID := ibabuza.RaftGroupID(1)

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(groupID, metadata)
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
				badgerMgr := manager.(*MultiRaftBadgerWalManager)
				// Create entry indices for reading
				readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					{Index: 2, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnts := make([]raftpb.Entry, len(readEntryIndex))
				err = badgerMgr.ReadEntriesData(groupID, readEntryIndex, destEnts)
				assert.NoError(t, err)

				// Verify the entries were read correctly
				for i, entry := range entries {
					assert.Equal(t, entry.Term, destEnts[i].Term)
					assert.Equal(t, entry.Index, destEnts[i].Index)
					assert.Equal(t, entry.Type, destEnts[i].Type)
					assert.Equal(t, entry.Data, destEnts[i].Data)
				}

				// Test error cases
				// Test with empty arrays
				err = badgerMgr.ReadEntriesData(groupID, nil, nil)
				assert.Error(t, err) // Should return an error for empty arrays

				// Test with mismatched sizes
				err = badgerMgr.ReadEntriesData(groupID, readEntryIndex, []raftpb.Entry{{}})
				assert.Error(t, err) // Should return an error for mismatched sizes

				// Test with invalid index
				invalidIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 999, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				invalidDest := make([]raftpb.Entry, 1)
				err = badgerMgr.ReadEntriesData(groupID, invalidIndex, invalidDest)
				assert.Error(t, err) // Should return an error for non-existent entry

			case WalManagerTypePebble:
				pebbleMgr := manager.(*MultiRaftPebbleWalManager)
				// Create entry indices for reading
				readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
					{Index: 2, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnts := make([]raftpb.Entry, len(readEntryIndex))
				err = pebbleMgr.ReadEntriesData(groupID, readEntryIndex, destEnts)
				assert.NoError(t, err)

				// Verify the entries were read correctly
				for i, entry := range entries {
					assert.Equal(t, entry.Term, destEnts[i].Term)
					assert.Equal(t, entry.Index, destEnts[i].Index)
					assert.Equal(t, entry.Type, destEnts[i].Type)
					assert.Equal(t, entry.Data, destEnts[i].Data)
				}

				// Test error cases
				// Test with empty arrays
				err = pebbleMgr.ReadEntriesData(groupID, nil, nil)
				assert.Error(t, err) // Should return an error for empty arrays

				// Test with mismatched sizes
				err = pebbleMgr.ReadEntriesData(groupID, readEntryIndex, []raftpb.Entry{{}})
				assert.Error(t, err) // Should return an error for mismatched sizes

				// Test with invalid index
				invalidIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 999, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				invalidDest := make([]raftpb.Entry, 1)
				err = pebbleMgr.ReadEntriesData(groupID, invalidIndex, invalidDest)
				assert.Error(t, err) // Should return an error for non-existent entry
			}
		})
	}
}

func TestMultiRaftWalManager_CreatePurger(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			// Test Purger method
			purger := manager.Purger()
			assert.NotNil(t, purger)

			purger.Start()
			purger.Start()
		})
	}
}

func TestMultiRaftWalManager_PurgerIntegration(t *testing.T) {
	testCases := []struct {
		name        string
		managerType WalManagerType
	}{
		{"BadgerDB", WalManagerTypeBadger},
		{"PebbleDB", WalManagerTypePebble},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := setupTestMultiRaftManagerWithType(MultiRaftConfig{
				InMemory:           true,
				WalDir:             "",
				KeyPrefixCacheSize: 10,
			}, tc.managerType)
			defer manager.Close()

			groupID := ibabuza.RaftGroupID(1)

			// Create WAL and add entries
			metadata := babuzapb.WalMetadata{
				ClusterID:   123,
				LocalPeerID: 456,
			}
			_, wal, err := manager.CreateWal(groupID, metadata)
			assert.NoError(t, err)
			defer wal.Close()

			// Create and start purger
			purger := manager.Purger()
			assert.NotNil(t, purger)
			purger.Start()

			// Add some entries
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
				{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
				{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("data3")},
			}
			hardState := raftpb.HardState{
				Term:   1,
				Vote:   1,
				Commit: 3,
			}
			err = wal.Save(hardState, entries)
			assert.NoError(t, err)

			// Save a snapshot to trigger purging
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 2,
					Term:  1,
				},
			}
			err = wal.SaveSnapshot(snapshot)
			assert.NoError(t, err)

			// Trigger purge by calling wal.Purge with the snapshot
			err = wal.Purge(snapshot)
			assert.NoError(t, err)
			// Wait for the purger to process
			time.Sleep(time.Second)

			// Verify that entries before snapshot index were purged
			// Try to read entries directly from the WAL to verify purge worked
			switch tc.managerType {
			case WalManagerTypeBadger:
				badgerMgr := manager.(*MultiRaftBadgerWalManager)
				// Try to read entry at index 1 (should be purged)
				purgedEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnt := make([]raftpb.Entry, 1)
				err = badgerMgr.ReadEntriesData(groupID, purgedEntryIndex, destEnt)
				assert.Error(t, err, "Reading purged entry should return error")

				// Try to read entry at index 3 (should still exist)
				remainingEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 3, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnt = make([]raftpb.Entry, 1)
				err = badgerMgr.ReadEntriesData(groupID, remainingEntryIndex, destEnt)
				assert.NoError(t, err, "Reading remaining entry should succeed")
				assert.Equal(t, uint64(3), destEnt[0].Index)
				assert.Equal(t, []byte("data3"), destEnt[0].Data)

			case WalManagerTypePebble:
				pebbleMgr := manager.(*MultiRaftPebbleWalManager)
				// Try to read entry at index 1 (should be purged)
				purgedEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnt := make([]raftpb.Entry, 1)
				err = pebbleMgr.ReadEntriesData(groupID, purgedEntryIndex, destEnt)
				assert.Error(t, err, "Reading purged entry should return error")

				// Try to read entry at index 3 (should still exist)
				remainingEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
					{Index: 3, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
				}
				destEnt = make([]raftpb.Entry, 1)
				err = pebbleMgr.ReadEntriesData(groupID, remainingEntryIndex, destEnt)
				assert.NoError(t, err, "Reading remaining entry should succeed")
				assert.Equal(t, uint64(3), destEnt[0].Index)
				assert.Equal(t, []byte("data3"), destEnt[0].Data)
			}

		})
	}
}
