package lsmtwal

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"os"
	"testing"
)

// TestNewWalManager tests the NewWalManager function
func TestNewWalManager(t *testing.T) {
	// Use an in-memory database for this test
	// Test with default options
	manager := NewBadgerWalManager(Config{
		InMemory: true,
		WalDir:   "",
	}, &logger.Mock{})
	assert.NotNil(t, manager)
	assert.IsType(t, &BadgerWalManager{}, manager)

	// Cleanup
	err := manager.(*BadgerWalManager).db.Close()
	assert.NoError(t, err)
}

// setupTestManager creates a test manager with an in-memory database
func setupTestManager(cfg Config) *BadgerWalManager {
	manager := NewBadgerWalManager(cfg, &logger.Mock{})
	return manager.(*BadgerWalManager)
}

// TestBadgerWalManager_FindSnapshot tests the FindSnapshot method
func TestBadgerWalManager_FindSnapshot(t *testing.T) {
	manager := setupTestManager(Config{
		InMemory: true,
	})
	defer manager.db.Close()

	// Test with empty database
	snapshots, err := manager.FindSnapshot()
	assert.NoError(t, err)
	assert.Empty(t, snapshots)

	// Add a snapshot entry and test again
	err = manager.db.Update(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keySnapshot)
		binary.BigEndian.PutUint64(key[8:], 10) // snapshot with index 10

		walsnap := walpb.Snapshot{
			Index: 10,
			Term:  5,
		}
		data, err := walsnap.Marshal()
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})
	assert.NoError(t, err)

	// Now retrieve snapshots
	snapshots, err = manager.FindSnapshot()
	assert.NoError(t, err)
	assert.NotEmpty(t, snapshots)
}

// TestBadgerWalManager_CreateWal tests the CreateWal method
func TestBadgerWalManager_CreateWal(t *testing.T) {
	manager := setupTestManager(Config{
		InMemory: true,
	})
	// Create test metadata
	metadata := babuzapb.WalMetadata{
		ClusterId:   123,
		LocalPeerId: 456,
	}

	// Create WAL
	es, wal, err := manager.CreateWal(metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es)
	assert.NotNil(t, wal)

	// Verify the metadata was saved
	err = manager.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyMetadata))
		assert.NoError(t, err)

		err = item.Value(func(val []byte) error {
			var storedMetadata babuzapb.WalMetadata
			err := storedMetadata.Unmarshal(val)
			assert.NoError(t, err)
			assert.Equal(t, metadata.ClusterId, storedMetadata.ClusterId)
			assert.Equal(t, metadata.LocalPeerId, storedMetadata.LocalPeerId)
			return nil
		})
		return err
	})
	assert.NoError(t, err)

	// Clean up
	err = wal.Close()
	assert.NoError(t, err)
}

// TestBadgerWalManager_ReplayWal tests the ReplayWal method
func TestBadgerWalManager_ReplayWal(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "badger-test")
	defer os.RemoveAll(tmpDir)
	manager := setupTestManager(Config{
		WalDir: tmpDir,
	})
	// First, we need to create a WAL and add some entries
	metadata := babuzapb.WalMetadata{
		ClusterId:   123,
		LocalPeerId: 456,
	}

	es, wal, err := manager.CreateWal(metadata)
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

	// Now try to replay the WAL
	snapshot := &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0, // Start from the beginning
		},
	}
	//renew the manager
	manager = setupTestManager(Config{
		WalDir: tmpDir,
	})
	replayEs, replayWal, result, err := manager.ReplayWal(snapshot, false)
	assert.NoError(t, err)
	assert.NotNil(t, replayEs)
	assert.NotNil(t, replayWal)
	assert.NotNil(t, result)

	// Verify replayed data
	assert.Equal(t, hardState, result.HardState())

	// Verify metadata
	mData, err := metadata.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, mData, result.Metadata())

	// Verify we can read entries from the replayed result
	var foundConfChangeEntries int
	err = result.ForEachConfChangeEntries(func(entry raftpb.Entry) error {
		if entry.Type == raftpb.EntryConfChange {
			foundConfChangeEntries++
		}
		return nil
	})
	assert.NoError(t, err)
	// We didn't add any conf change entries in our test data
	assert.Equal(t, 0, foundConfChangeEntries)

	// Clean up
	err = replayWal.Close()
	assert.NoError(t, err)
}

// TestBadgerWalManager_HasExistingWals tests the HasExistingWals method
func TestBadgerWalManager_HasExistingWals(t *testing.T) {
	manager := setupTestManager(Config{
		InMemory: true,
	})
	defer manager.db.Close()

	// Initially there should be no WALs
	hasWals, err := manager.HasExistingWals()
	assert.NoError(t, err)
	assert.False(t, hasWals)

	// Add some entries that look like WAL entries
	err = manager.db.Update(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keyEntry)
		binary.BigEndian.PutUint64(key[8:], 1)

		entry := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte("test"),
		}
		data, err := entry.Marshal()
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})
	assert.NoError(t, err)

	// Now there should be WALs
	hasWals, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.True(t, hasWals)
}

// TestBadgerWalManager_ReadEntriesData tests the ReadEntriesData method
func TestBadgerWalManager_ReadEntriesData(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "badger-test")
	defer os.RemoveAll(tmpDir)
	manager := setupTestManager(Config{
		WalDir: tmpDir,
	})
	defer manager.db.Close()

	// First, add some entries to the database
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
	}

	err := manager.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			key := make([]byte, 16)
			copy(key, keyEntry)
			binary.BigEndian.PutUint64(key[8:], entry.Index)

			data, err := entry.Marshal()
			if err != nil {
				return err
			}

			if err = txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)

	// Create entry indices for reading
	readEntryIndex := []walbase.EntryIndex[storage.EntryMetadata]{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
	}

	// Create destination slice
	destEnts := make([]raftpb.Entry, len(readEntryIndex))

	// Read the entries
	err = manager.ReadEntriesData(readEntryIndex, destEnts)
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
	err = manager.ReadEntriesData(nil, nil)
	assert.Error(t, err) // Should return an error for empty arrays

	// Test with mismatched sizes
	readEntryIndex[1].Index = 3
	err = manager.ReadEntriesData(readEntryIndex, []raftpb.Entry{{}})
	assert.Error(t, err) // Should return an error for mismatched sizes

	// Test with invalid index
	invalidIndex := []walbase.EntryIndex[storage.EntryMetadata]{
		{Index: 999, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
	}
	invalidDest := make([]raftpb.Entry, 1)
	err = manager.ReadEntriesData(invalidIndex, invalidDest)
	assert.Error(t, err) // Should return an error for non-existent entry
}
