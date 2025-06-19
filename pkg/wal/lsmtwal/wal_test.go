package lsmtwal

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"testing"
)

func setupTestDB(t *testing.T) (*badger.DB, string) {
	// Create a temporary directory
	dir := t.TempDir()
	// Open a Badger database with the temporary directory
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(false) // For faster tests

	db, err := badger.Open(opts)
	assert.NoError(t, err)

	return db, dir
}

func setupTestPebbleDB(t *testing.T) *pebble.DB {
	// Create in-memory database for testing
	opts := &pebble.Options{
		FS: vfs.NewMem(),
	}

	db, err := pebble.Open("", opts)
	assert.NoError(t, err)

	return db
}

func cleanup(db *badger.DB, _ string) {
	_ = db.Close()
}

func cleanupPebble(db *pebble.DB) {
	_ = db.Close()
}

func TestNewWal(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				assert.NotNil(t, wal)
				assert.Equal(t, db, wal.db)
				assert.False(t, wal.noFsync)

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				assert.NotNil(t, wal)
				assert.Equal(t, db, wal.db)
				assert.False(t, wal.noFsync)
			}
		})
	}
}

func TestWal_SetUnsafeNoFsync(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				assert.False(t, wal.noFsync)
				wal.SetUnsafeNoFsync()
				assert.True(t, wal.noFsync)

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				assert.False(t, wal.noFsync)
				wal.SetUnsafeNoFsync()
				assert.True(t, wal.noFsync)
			}
		})
	}
}

func TestWal_Save(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the data
				err := wal.Save(hardState, entries)
				assert.NoError(t, err)

				// Verify hardState was saved correctly
				err = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(wal.keyPrefix.hardState)
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
					for _, entry := range entries {
						var key [24]byte
						copy(key[:16], wal.keyPrefix.entry)
						binary.BigEndian.PutUint64(key[16:], entry.Index)
						item, err := txn.Get(key[:24])
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

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the data
				err := wal.Save(hardState, entries)
				assert.NoError(t, err)

				// Verify hardState was saved correctly
				val, closer, err := db.Get(wal.keyPrefix.hardState)
				assert.NoError(t, err)
				defer func() { _ = closer.Close() }()

				var storedHardState raftpb.HardState
				err = storedHardState.Unmarshal(val)
				assert.NoError(t, err)
				assert.Equal(t, hardState, storedHardState)

				// Verify entries were saved correctly
				for _, entry := range entries {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.entry)
					binary.BigEndian.PutUint64(key[16:], entry.Index)

					val, closer, err := db.Get(key[:24])
					assert.NoError(t, err)

					var storedEntry raftpb.Entry
					err = storedEntry.Unmarshal(val)
					_ = closer.Close()
					assert.NoError(t, err)
					assert.Equal(t, entry, storedEntry)
				}
			}
		})
	}
}

func TestWal_SaveEmptyHardState(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create empty hardState
			hardState := raftpb.HardState{
				Term:   0,
				Vote:   0,
				Commit: 0,
			}

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the data
				err := wal.Save(hardState, nil)
				assert.NoError(t, err)

				// Verify hardState was not saved (as it's empty)
				err = db.View(func(txn *badger.Txn) error {
					_, err = txn.Get(wal.keyPrefix.hardState)
					// Should return error since the key doesn't exist
					return err
				})
				assert.Error(t, err)
				assert.Equal(t, badger.ErrKeyNotFound, err)

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the data
				err := wal.Save(hardState, nil)
				assert.NoError(t, err)

				// Verify hardState was not saved (as it's empty)
				_, _, err = db.Get(wal.keyPrefix.hardState)
				assert.Error(t, err)
				assert.Equal(t, pebble.ErrNotFound, err)
			}
		})
	}
}

func TestWal_SaveSnapshot(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the snapshot
				err := wal.SaveSnapshot(snapshot)
				assert.NoError(t, err)

				// Verify the snapshot was saved correctly
				err = db.View(func(txn *badger.Txn) error {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.snapshot)
					binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)

					item, err := txn.Get(key[:24])
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

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the snapshot
				err := wal.SaveSnapshot(snapshot)
				assert.NoError(t, err)

				// Verify the snapshot was saved correctly
				var key [24]byte
				copy(key[:16], wal.keyPrefix.snapshot)
				binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)

				val, closer, err := db.Get(key[:24])
				assert.NoError(t, err)
				defer func() { _ = closer.Close() }()

				var walsnap walpb.Snapshot
				err = walsnap.Unmarshal(val)
				assert.NoError(t, err)
				assert.Equal(t, snapshot.Metadata.Index, walsnap.Index)
				assert.Equal(t, snapshot.Metadata.Term, walsnap.Term)
				assert.Equal(t, &snapshot.Metadata.ConfState, walsnap.ConfState)
			}
		})
	}
}

func TestWal_SaveEmptySnapshot(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create an empty snapshot
			snapshot := raftpb.Snapshot{
				Data: []byte("snapshot data"),
				Metadata: raftpb.SnapshotMetadata{
					Index: 0, // Empty snapshot has Index = 0
					Term:  5,
				},
			}

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the snapshot
				err := wal.SaveSnapshot(snapshot)
				assert.NoError(t, err)

				// Verify no snapshot was saved
				err = db.View(func(txn *badger.Txn) error {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.snapshot)
					binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)

					_, err = txn.Get(key[:24])
					// Should return error since the key doesn't exist
					return err
				})
				assert.Error(t, err)
				assert.Equal(t, badger.ErrKeyNotFound, err)

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save the snapshot
				err := wal.SaveSnapshot(snapshot)
				assert.NoError(t, err)

				// Verify no snapshot was saved
				var key [24]byte
				copy(key[:16], wal.keyPrefix.snapshot)
				binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)

				_, _, err = db.Get(key[:24])
				// Should return error since the key doesn't exist
				assert.Error(t, err)
				assert.Equal(t, pebble.ErrNotFound, err)
			}
		})
	}
}

func TestWal_Purge(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Add some entries to purge later
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Data: []byte("data1")},
				{Term: 1, Index: 2, Data: []byte("data2")},
				{Term: 1, Index: 3, Data: []byte("data3")},
				{Term: 1, Index: 4, Data: []byte("data4")},
				{Term: 1, Index: 5, Data: []byte("data5")},
			}

			// Create a snapshot at index 3
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 3,
					Term:  1,
				},
			}

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save entries
				err := wal.Save(raftpb.HardState{}, entries)
				assert.NoError(t, err)

				// Purge entries up to snapshot index
				err = wal.Purge(snapshot)
				assert.NoError(t, err)

				// Verify entries 1-3 are deleted, 4-5 still exist
				err = db.View(func(txn *badger.Txn) error {
					// Check that entries 1-3 are purged
					for i := uint64(1); i <= 3; i++ {
						var key [24]byte
						copy(key[:16], wal.keyPrefix.entry)
						binary.BigEndian.PutUint64(key[16:], i)

						_, err = txn.Get(key[:24])
						assert.Error(t, err, "Entry with index %d should be purged", i)
						assert.Equal(t, badger.ErrKeyNotFound, err)
					}

					// Check that entries 4-5 still exist
					for i := uint64(4); i <= 5; i++ {
						var key [24]byte
						copy(key[:16], wal.keyPrefix.entry)
						binary.BigEndian.PutUint64(key[16:], i)

						item, err := txn.Get(key[:24])
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

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save entries
				err := wal.Save(raftpb.HardState{}, entries)
				assert.NoError(t, err)

				// Purge entries up to snapshot index
				err = wal.Purge(snapshot)
				assert.NoError(t, err)

				// Verify entries 1-3 are deleted, 4-5 still exist
				// Check that entries 1-3 are purged
				for i := uint64(1); i <= 3; i++ {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.entry)
					binary.BigEndian.PutUint64(key[16:], i)

					_, _, err := db.Get(key[:24])
					assert.Error(t, err, "Entry with index %d should be purged", i)
					assert.Equal(t, pebble.ErrNotFound, err)
				}

				// Check that entries 4-5 still exist
				for i := uint64(4); i <= 5; i++ {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.entry)
					binary.BigEndian.PutUint64(key[16:], i)

					val, closer, err := db.Get(key[:24])
					assert.NoError(t, err, "Entry with index %d should still exist", i)

					var entry raftpb.Entry
					err = entry.Unmarshal(val)
					_ = closer.Close()
					assert.NoError(t, err)
					assert.Equal(t, i, entry.Index)
				}
			}
		})
	}
}

func TestWal_PurgeEmptySnapshot(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Add some entries
			entries := []raftpb.Entry{
				{Term: 1, Index: 1, Data: []byte("data1")},
				{Term: 1, Index: 2, Data: []byte("data2")},
			}

			// Create an empty snapshot
			snapshot := raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 0, // Empty snapshot has Index = 0
				},
			}

			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save entries
				err := wal.Save(raftpb.HardState{}, entries)
				assert.NoError(t, err)

				// Purge with empty snapshot (should do nothing)
				err = wal.Purge(snapshot)
				assert.NoError(t, err)

				// Verify entries still exist
				err = db.View(func(txn *badger.Txn) error {
					for i := uint64(1); i <= 2; i++ {
						var key [24]byte
						copy(key[:16], wal.keyPrefix.entry)
						binary.BigEndian.PutUint64(key[16:], i)

						item, err := txn.Get(key[:24])
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

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Save entries
				err := wal.Save(raftpb.HardState{}, entries)
				assert.NoError(t, err)

				// Purge with empty snapshot (should do nothing)
				err = wal.Purge(snapshot)
				assert.NoError(t, err)

				// Verify entries still exist
				for i := uint64(1); i <= 2; i++ {
					var key [24]byte
					copy(key[:16], wal.keyPrefix.entry)
					binary.BigEndian.PutUint64(key[16:], i)

					val, closer, err := db.Get(key[:24])
					assert.NoError(t, err, "Entry with index %d should still exist", i)

					var entry raftpb.Entry
					err = entry.Unmarshal(val)
					_ = closer.Close()
					assert.NoError(t, err)
					assert.Equal(t, i, entry.Index)
				}
			}
		})
	}
}

func TestWal_Sync(t *testing.T) {
	testCases := []struct {
		name    string
		walType string
	}{
		{"BadgerDB WAL", "badger"},
		{"PebbleDB WAL", "pebble"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.walType {
			case "badger":
				db, dir := setupTestDB(t)
				defer cleanup(db, dir)

				wal := NewBadgerWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Test normal sync
				err := wal.Sync()
				assert.NoError(t, err)

				// Test sync with noFsync set
				wal.SetUnsafeNoFsync()
				err = wal.Sync()
				assert.NoError(t, err)

			case "pebble":
				db := setupTestPebbleDB(t)
				defer cleanupPebble(db)

				wal := NewPebbleWal(db, nil, newKeyPrefix(0))
				defer func() { _ = wal.Close() }()

				// Test normal sync
				err := wal.Sync()
				assert.NoError(t, err)

				// Test sync with noFsync set
				wal.SetUnsafeNoFsync()
				err = wal.Sync()
				assert.NoError(t, err)
			}
		})
	}
}

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
