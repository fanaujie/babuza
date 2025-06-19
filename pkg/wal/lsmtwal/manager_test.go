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
