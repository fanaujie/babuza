package lsmtwal

import (
	"encoding/binary"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// setupTestDB creates a temporary Badger database for testing
func setupTestDB(t *testing.T) (*badger.DB, string) {
	// Create a temporary directory
	dir, err := os.MkdirTemp("", "badger-wal-test")
	assert.NoError(t, err)

	// Open a Badger database with the temporary directory
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(false) // For faster tests

	db, err := badger.Open(opts)
	assert.NoError(t, err)

	return db, dir
}

// cleanup closes the database and removes the temporary directory
func cleanup(db *badger.DB, dir string) {
	db.Close()
	os.RemoveAll(dir)
}

// Test creating a new BadgerWal instance
func TestNewBadgerWal(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	assert.NotNil(t, wal)
	assert.Equal(t, db, wal.db)
	assert.False(t, wal.noFsync)
	assert.NotNil(t, wal.stopCh)
}

// Test setting the unsafe no fsync option
func TestBadgerWal_SetUnsafeNoFsync(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	assert.False(t, wal.noFsync)
	wal.SetUnsafeNoFsync()
	assert.True(t, wal.noFsync)
}

// Test saving entries to the WAL
func TestBadgerWal_Save(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Create test data
	hardState := raftpb.HardState{
		Term:   10,
		Vote:   20,
		Commit: 30,
	}

	entries := []raftpb.Entry{
		{
			Term:  10,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte("test data 1"),
		},
		{
			Term:  10,
			Index: 2,
			Type:  raftpb.EntryNormal,
			Data:  []byte("test data 2"),
		},
	}

	// Save the data
	err := wal.Save(hardState, entries)
	assert.NoError(t, err)

	// Verify hardState was saved correctly
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyHardState))
		assert.NoError(t, err)

		err = item.Value(func(val []byte) error {
			var storedHardState raftpb.HardState
			err := storedHardState.Unmarshal(val)
			assert.NoError(t, err)
			assert.Equal(t, hardState, storedHardState)
			return nil
		})
		return err
	})
	assert.NoError(t, err)

	// Verify entries were saved correctly

	err = db.View(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keyEntry)
		for _, entry := range entries {
			binary.BigEndian.PutUint64(key[8:], entry.Index)
			item, err := txn.Get(key)
			assert.NoError(t, err)
			if err = item.Value(func(val []byte) error {
				var storedEntry raftpb.Entry
				err := storedEntry.Unmarshal(val)
				assert.NoError(t, err)
				assert.Equal(t, entry, storedEntry)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)

}

// Test saving an empty hardState (should be skipped)
func TestBadgerWal_SaveEmptyHardState(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Create empty hardState
	hardState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: 0,
	}

	// Save the data
	err := wal.Save(hardState, nil)
	assert.NoError(t, err)

	// Verify hardState was not saved (as it's empty)
	err = db.View(func(txn *badger.Txn) error {
		_, err = txn.Get([]byte(keyHardState))
		// Should return error since the key doesn't exist
		return err
	})
	assert.Error(t, err)
	assert.Equal(t, badger.ErrKeyNotFound, err)
}

// Test saving a snapshot
func TestBadgerWal_SaveSnapshot(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Create a test snapshot
	snapshot := raftpb.Snapshot{
		Data: []byte("snapshot data"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 100,
			Term:  5,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
	}

	// Save the snapshot
	err := wal.SaveSnapshot(snapshot)
	assert.NoError(t, err)

	// Verify the snapshot was saved correctly
	err = db.View(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keySnapshot)
		binary.BigEndian.PutUint64(key[8:], snapshot.Metadata.Index)

		item, err := txn.Get(key)
		assert.NoError(t, err)

		return item.Value(func(val []byte) error {
			var walsnap walpb.Snapshot
			err = walsnap.Unmarshal(val)
			assert.NoError(t, err)
			assert.Equal(t, snapshot.Metadata.Index, walsnap.Index)
			assert.Equal(t, snapshot.Metadata.Term, walsnap.Term)
			assert.Equal(t, &snapshot.Metadata.ConfState, walsnap.ConfState)
			return nil
		})
	})
	assert.NoError(t, err)
}

// Test saving an empty snapshot (should be skipped)
func TestBadgerWal_SaveEmptySnapshot(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Create an empty snapshot
	snapshot := raftpb.Snapshot{
		Data: []byte("snapshot data"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 0, // Empty snapshot has Index = 0
			Term:  5,
		},
	}

	// Save the snapshot
	err := wal.SaveSnapshot(snapshot)
	assert.NoError(t, err)

	// Verify no snapshot was saved
	err = db.View(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keySnapshot)
		binary.BigEndian.PutUint64(key[8:], snapshot.Metadata.Index)

		_, err = txn.Get(key)
		// Should return error since the key doesn't exist
		return err
	})
	assert.Error(t, err)
	assert.Equal(t, badger.ErrKeyNotFound, err)
}

// Test purging entries up to a snapshot index
func TestBadgerWal_Purge(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Add some entries to purge later
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte("data1")},
		{Term: 1, Index: 2, Data: []byte("data2")},
		{Term: 1, Index: 3, Data: []byte("data3")},
		{Term: 1, Index: 4, Data: []byte("data4")},
		{Term: 1, Index: 5, Data: []byte("data5")},
	}

	// Save entries
	err := wal.Save(raftpb.HardState{}, entries)
	assert.NoError(t, err)

	// Create a snapshot at index 3
	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 3,
			Term:  1,
		},
	}

	// Purge entries up to snapshot index
	err = wal.Purge(snapshot)
	assert.NoError(t, err)

	// Verify entries 1-3 are deleted, 4-5 still exist
	err = db.View(func(txn *badger.Txn) error {
		// Check that entries 1-3 are purged
		for i := uint64(1); i <= 3; i++ {
			key := make([]byte, 16)
			copy(key, keyEntry)
			binary.BigEndian.PutUint64(key[8:], i)

			_, err = txn.Get(key)
			assert.Error(t, err, "Entry with index %d should be purged", i)
			assert.Equal(t, badger.ErrKeyNotFound, err)
		}

		// Check that entries 4-5 still exist
		for i := uint64(4); i <= 5; i++ {
			key := make([]byte, 16)
			copy(key, keyEntry)
			binary.BigEndian.PutUint64(key[8:], i)

			item, err := txn.Get(key)
			assert.NoError(t, err, "Entry with index %d should still exist", i)

			err = item.Value(func(val []byte) error {
				var entry raftpb.Entry
				err := entry.Unmarshal(val)
				assert.NoError(t, err)
				assert.Equal(t, i, entry.Index)
				return nil
			})
			assert.NoError(t, err)
		}
		return nil
	})
	assert.NoError(t, err)
}

// Test purging with an empty snapshot (should do nothing)
func TestBadgerWal_PurgeEmptySnapshot(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Add some entries
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Data: []byte("data1")},
		{Term: 1, Index: 2, Data: []byte("data2")},
	}

	// Save entries
	err := wal.Save(raftpb.HardState{}, entries)
	assert.NoError(t, err)

	// Create an empty snapshot
	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0, // Empty snapshot has Index = 0
		},
	}

	// Purge with empty snapshot (should do nothing)
	err = wal.Purge(snapshot)
	assert.NoError(t, err)

	// Verify entries still exist
	err = db.View(func(txn *badger.Txn) error {
		for i := uint64(1); i <= 2; i++ {
			key := make([]byte, 16)
			copy(key, keyEntry)
			binary.BigEndian.PutUint64(key[8:], i)

			item, err := txn.Get(key)
			assert.NoError(t, err, "Entry with index %d should still exist", i)

			err = item.Value(func(val []byte) error {
				var entry raftpb.Entry
				err := entry.Unmarshal(val)
				assert.NoError(t, err)
				assert.Equal(t, i, entry.Index)
				return nil
			})
			assert.NoError(t, err)
		}
		return nil
	})
	assert.NoError(t, err)
}

// Test the Sync method
func TestBadgerWal_Sync(t *testing.T) {
	db, dir := setupTestDB(t)
	defer cleanup(db, dir)

	wal := NewBadgerWal(db, nil)
	defer wal.Close()

	// Test normal sync
	err := wal.Sync()
	assert.NoError(t, err)

	// Test sync with noFsync set
	wal.SetUnsafeNoFsync()
	err = wal.Sync()
	assert.NoError(t, err)
}

// Test the Close method
func TestBadgerWal_Close(t *testing.T) {
	db, dir := setupTestDB(t)
	defer os.RemoveAll(dir) // We'll close the DB in the test

	wal := NewBadgerWal(db, nil)

	// Close should succeed
	err := wal.Close()
	assert.NoError(t, err)

	// Verify the stopCh is closed by trying to send to it (should panic if not closed)
	closeCheckDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Channel is closed, this is expected
				close(closeCheckDone)
			}
		}()
		wal.stopCh <- struct{}{}
	}()

	select {
	case <-closeCheckDone:
		// Channel was closed, as expected
	case <-time.After(time.Second):
		t.Fatal("stopCh was not closed")
	}
}

// Test helper functions
func TestIsEmptyHardStateAndSnapshot(t *testing.T) {
	// Test empty hardState
	emptyState := raftpb.HardState{
		Term:   0,
		Vote:   0,
		Commit: 0,
	}
	assert.True(t, isEmptyHardState(emptyState))

	// Test non-empty hardState
	nonEmptyState := raftpb.HardState{
		Term:   1,
		Vote:   0,
		Commit: 0,
	}
	assert.False(t, isEmptyHardState(nonEmptyState))

	// Test empty snapshot
	emptySnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0,
		},
	}
	assert.True(t, isEmptySnapshot(emptySnapshot))

	// Test non-empty snapshot
	nonEmptySnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
		},
	}
	assert.False(t, isEmptySnapshot(nonEmptySnapshot))
}
