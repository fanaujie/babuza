package lsmtwal

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"testing"
)

// setupTestMultiRaftManager creates a test MultiRaftBadgerWalManager with an in-memory database
func setupTestMultiRaftManager() *MultiRaftBadgerWalManager {
	return NewMultiRaftBadgerWalManager(MultiRaftConfig{
		InMemory:           true,
		WalDir:             "",
		KeyPrefixCacheSize: 10,
	}, &logger.Mock{}).(*MultiRaftBadgerWalManager)
}

// setupTestMultiRaftManagerWithDir creates a test MultiRaftBadgerWalManager with a directory
func setupTestMultiRaftManagerWithDir(t *testing.T) (*MultiRaftBadgerWalManager, string) {
	dir := t.TempDir()
	manager := NewMultiRaftBadgerWalManager(MultiRaftConfig{
		InMemory:           false,
		WalDir:             dir,
		KeyPrefixCacheSize: 10,
	}, &logger.Mock{}).(*MultiRaftBadgerWalManager)
	return manager, dir
}

// TestNewMultiRaftBadgerWalManager tests the NewMultiRaftBadgerWalManager function
func TestNewMultiRaftBadgerWalManager(t *testing.T) {
	// Test with in-memory database
	manager := NewMultiRaftBadgerWalManager(MultiRaftConfig{
		InMemory:           true,
		WalDir:             "",
		KeyPrefixCacheSize: 10,
	}, &logger.Mock{})

	assert.NotNil(t, manager)
	assert.IsType(t, &MultiRaftBadgerWalManager{}, manager)

	// Cleanup
	m := manager.(*MultiRaftBadgerWalManager)
	err := m.db.Close()
	assert.NoError(t, err)

	// Test with directory
	dir := t.TempDir()
	manager = NewMultiRaftBadgerWalManager(MultiRaftConfig{
		InMemory:           false,
		WalDir:             dir,
		KeyPrefixCacheSize: 10,
	}, &logger.Mock{})

	assert.NotNil(t, manager)
	assert.IsType(t, &MultiRaftBadgerWalManager{}, manager)

	// Cleanup
	m = manager.(*MultiRaftBadgerWalManager)
	err = m.db.Close()
	assert.NoError(t, err)
}

// TestMultiRaftBadgerWalManager_FindSnapshot tests the FindSnapshot method
func TestMultiRaftBadgerWalManager_FindSnapshot(t *testing.T) {
	manager := setupTestMultiRaftManager()
	defer manager.db.Close()

	groupID := ibabuza.RaftGroupID(1)

	// Test with empty database
	snapshots, err := manager.FindSnapshot(groupID)
	assert.NoError(t, err)
	assert.Empty(t, snapshots)

	// Add a snapshot entry and test again
	groupPrefix := manager.prefixCache.get(groupID)

	err = manager.db.Update(func(txn *badger.Txn) error {
		var key [24]byte
		copy(key[:16], groupPrefix.snapshot)
		binary.BigEndian.PutUint64(key[16:], 10) // snapshot with index 10

		walsnap := walpb.Snapshot{
			Index: 10,
			Term:  5,
		}
		data, err := walsnap.Marshal()
		if err != nil {
			return err
		}

		return txn.Set(key[:24], data)
	})
	assert.NoError(t, err)

	// Now retrieve snapshots
	snapshots, err = manager.FindSnapshot(groupID)
	assert.NoError(t, err)
	assert.NotEmpty(t, snapshots)
	assert.Equal(t, uint64(10), snapshots[0].Index)
	assert.Equal(t, uint64(5), snapshots[0].Term)
}

// TestMultiRaftBadgerWalManager_CreateWal tests the CreateWal method
func TestMultiRaftBadgerWalManager_CreateWal(t *testing.T) {
	manager := setupTestMultiRaftManager()
	defer manager.db.Close()

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

	// Verify the metadata was saved
	groupPrefix := manager.prefixCache.get(groupID)

	err = manager.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(groupPrefix.metadata)
		assert.NoError(t, err)

		err = item.Value(func(val []byte) error {
			var storedMetadata babuzapb.WalMetadata
			err := storedMetadata.Unmarshal(val)
			assert.NoError(t, err)
			assert.Equal(t, metadata.ClusterID, storedMetadata.ClusterID)
			assert.Equal(t, metadata.LocalPeerID, storedMetadata.LocalPeerID)
			return nil
		})
		return err
	})
	assert.NoError(t, err)

	// Verify snapshot was initialized
	err = manager.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(groupPrefix.snapshot)
		assert.NoError(t, err)

		err = item.Value(func(val []byte) error {
			var snapshot walpb.Snapshot
			err := snapshot.Unmarshal(val)
			assert.NoError(t, err)
			assert.Equal(t, uint64(0), snapshot.Index)
			assert.Equal(t, uint64(0), snapshot.Term)
			return nil
		})
		return err
	})
	assert.NoError(t, err)

	// Clean up
	err = wal.Close()
	assert.NoError(t, err)
}

// TestMultiRaftBadgerWalManager_ReplayWal tests the ReplayWal method
func TestMultiRaftBadgerWalManager_ReplayWal(t *testing.T) {
	manager, dir := setupTestMultiRaftManagerWithDir(t)
	defer manager.db.Close()

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

	// Now try to replay the WAL
	snapshot := &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0, // Start from the beginning
		},
	}

	// Close and reopen the manager to simulate a restart
	err = manager.db.Close()
	assert.NoError(t, err)

	// Create a new manager with the same directory
	manager = NewMultiRaftBadgerWalManager(MultiRaftConfig{
		InMemory:           false,
		WalDir:             dir,
		KeyPrefixCacheSize: 10,
	}, &logger.Mock{}).(*MultiRaftBadgerWalManager)

	// Replay the WAL
	replayEs, replayWal, result, err := manager.ReplayWal(groupID, snapshot, false)
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

	// Clean up
	err = replayWal.Close()
	assert.NoError(t, err)
}

// TestMultiRaftBadgerWalManager_HasExistingWals tests the HasExistingWals method
func TestMultiRaftBadgerWalManager_HasExistingWals(t *testing.T) {
	manager := setupTestMultiRaftManager()
	defer manager.db.Close()

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

	// Now there should be two WALs
	groupIDs, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, groupIDs, 2)

	// Clean up
	err = wal.Close()
	assert.NoError(t, err)
	err = wal2.Close()
	assert.NoError(t, err)
}

// TestMultiRaftBadgerWalManager_ReadEntriesData tests the ReadEntriesData method
func TestMultiRaftBadgerWalManager_ReadEntriesData(t *testing.T) {
	manager := setupTestMultiRaftManager()
	defer manager.db.Close()

	groupID := ibabuza.RaftGroupID(1)
	groupPrefix := manager.prefixCache.get(groupID)

	// First, add some entries to the database
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("data1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("data2")},
	}

	err := manager.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			var key [24]byte
			copy(key[:16], groupPrefix.entry)
			binary.BigEndian.PutUint64(key[16:], entry.Index)

			data, err := entry.Marshal()
			if err != nil {
				return err
			}

			if err = txn.Set(key[:24], data); err != nil {
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
	err = manager.ReadEntriesData(groupID, readEntryIndex, destEnts)
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
	err = manager.ReadEntriesData(groupID, nil, nil)
	assert.Error(t, err) // Should return an error for empty arrays

	// Test with mismatched sizes
	err = manager.ReadEntriesData(groupID, readEntryIndex, []raftpb.Entry{{}})
	assert.Error(t, err) // Should return an error for mismatched sizes

	// Test with invalid index
	invalidIndex := []walbase.EntryIndex[storage.EntryMetadata]{
		{Index: 999, Term: 1, Type: raftpb.EntryNormal, Metadata: storage.EntryMetadata{}},
	}
	invalidDest := make([]raftpb.Entry, 1)
	err = manager.ReadEntriesData(groupID, invalidIndex, invalidDest)
	assert.Error(t, err) // Should return an error for non-existent entry
}
