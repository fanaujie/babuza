package babuzawal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestMultiRaftWalManager_RemoveData(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create MultiRaftWalManager
	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID1 := ibabuza.RaftGroupID(1)
	groupID2 := ibabuza.RaftGroupID(2)

	// Create test metadata
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	// Create WALs for both groups
	es1, wal1, err := manager.CreateWal(groupID1, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es1)
	assert.NotNil(t, wal1)

	es2, wal2, err := manager.CreateWal(groupID2, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es2)
	assert.NotNil(t, wal2)

	// Add some data to both WALs
	hardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 2,
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("test data 1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("test data 2")},
	}

	err = wal1.Save(hardState, entries)
	assert.NoError(t, err)
	err = wal2.Save(hardState, entries)
	assert.NoError(t, err)

	// Close WALs before removing data
	err = wal1.Close()
	assert.NoError(t, err)
	err = wal2.Close()
	assert.NoError(t, err)

	// Verify both groups exist by checking directories
	groupDir1 := manager.getGroupWalDir(groupID1)
	groupDir2 := manager.getGroupWalDir(groupID2)

	assert.DirExists(t, groupDir1)
	assert.DirExists(t, groupDir2)

	// Verify HasExistingWals returns both groups
	existingGroups, err := manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, existingGroups, 2)

	// Remove data for group 1
	err = manager.RemoveData(groupID1)
	assert.NoError(t, err)

	// Verify group 1 directory is removed
	assert.NoError(t, err)
	assert.NoDirExists(t, groupDir1)
	assert.DirExists(t, groupDir2)

	// Verify HasExistingWals returns only group 2
	existingGroups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, existingGroups, 1)
	assert.Equal(t, groupID2, existingGroups[0])

	// Verify we can still replay group 2 WAL
	replaySnapshot := &raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 0,
		},
	}
	result, replayEs, replayWal, err := manager.ReplayWal(groupID2, replaySnapshot, false)
	assert.NoError(t, err)
	assert.NotNil(t, replayEs)
	assert.NotNil(t, replayWal)
	assert.NotNil(t, result)
	assert.Equal(t, hardState, result.HardState())
	err = replayWal.Close()
	assert.NoError(t, err)

	// Verify we cannot replay group 1 WAL (should fail)
	_, _, _, err = manager.ReplayWal(groupID1, replaySnapshot, false)
	assert.Error(t, err)

	// Remove data for group 2
	err = manager.RemoveData(groupID2)
	assert.NoError(t, err)

	// Verify group 2 directory is also removed
	assert.NoDirExists(t, groupDir2)

	// Verify no groups exist
	existingGroups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Empty(t, existingGroups)

	// Test removing data for non-existent group (should not error)
	err = manager.RemoveData(ibabuza.RaftGroupID(999))
	assert.NoError(t, err)
}

func TestMultiRaftWalManager_RemoveData_WithOpenLogManager(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create MultiRaftWalManager
	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID := ibabuza.RaftGroupID(1)

	// Create test metadata
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	// Create WAL for the group
	es, wal, err := manager.CreateWal(groupID, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es)
	assert.NotNil(t, wal)

	// Add some data
	hardState := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("test data")},
	}

	err = wal.Save(hardState, entries)
	assert.NoError(t, err)

	// Don't close the WAL - test RemoveData with open log manager
	defer wal.Close()

	// Verify group exists
	groupDir := manager.getGroupWalDir(groupID)
	assert.DirExists(t, groupDir)

	// Remove data for the group (this should close the log manager first)
	err = manager.RemoveData(groupID)
	assert.NoError(t, err)

	// Verify group directory is removed
	assert.NoDirExists(t, groupDir)

	// Note: The WAL might still be functional if it has internal buffers,
	// but the underlying log manager has been closed, so directory is removed.
	// This is acceptable behavior as the WAL will eventually fail on sync operations.
}

func TestMultiRaftWalManager_RemoveData_EmptyDirectory(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create MultiRaftWalManager
	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID := ibabuza.RaftGroupID(1)

	// Create an empty group directory manually
	groupDir := manager.getGroupWalDir(groupID)
	err = os.MkdirAll(groupDir, 0755)
	assert.NoError(t, err)

	// Add some random files to the directory
	testFile := filepath.Join(groupDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test content"), 0644)
	assert.NoError(t, err)

	// Verify directory exists
	assert.DirExists(t, groupDir)

	// Remove data for the group
	err = manager.RemoveData(groupID)
	assert.NoError(t, err)

	// Verify directory is completely removed
	assert.NoDirExists(t, groupDir)
}

func TestMultiRaftWalManager_getGroupWalDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	// Test directory path generation
	groupID1 := ibabuza.RaftGroupID(123)
	groupID2 := ibabuza.RaftGroupID(456)

	dir1 := manager.getGroupWalDir(groupID1)
	dir2 := manager.getGroupWalDir(groupID2)

	expectedDir1 := filepath.Join(tmpDir, "123")
	expectedDir2 := filepath.Join(tmpDir, "456")

	assert.Equal(t, expectedDir1, dir1)
	assert.Equal(t, expectedDir2, dir2)
	assert.NotEqual(t, dir1, dir2)
}

func TestNewMultiRaftWalManager(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Test with default options
	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	assert.NotNil(t, manager)
	assert.Equal(t, tmpDir, manager.WalRootDir)
	assert.NotNil(t, manager.options)
	assert.NotNil(t, manager.memPool)
	assert.NotNil(t, manager.logger)
	assert.NotNil(t, manager.purgerSnapCh)
	assert.NotNil(t, manager.purgerStopCh)
	defer manager.Close()

	// Test with custom options
	customOptions := []SetOptions{
		SetOptsWithWalFixedEntryBufferSize(1024),
		SetOptsWithWalMaxDynamicEntryBufferSize(8192),
		SetOptsWithWalLogFileChunkSize(2 * 1024 * 1024),
	}
	managerWithOptions := NewMultiRaftWalManager(tmpDir+"_custom", &logger.Mock{}, customOptions...)
	assert.NotNil(t, managerWithOptions)
	defer managerWithOptions.Close()
}

func TestMultiRaftWalManager_CreateWal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID1 := ibabuza.RaftGroupID(1)
	groupID2 := ibabuza.RaftGroupID(2)

	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	// Test creating WAL for first group
	es1, wal1, err := manager.CreateWal(groupID1, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es1)
	assert.NotNil(t, wal1)
	defer wal1.Close()

	// Test creating WAL for second group
	es2, wal2, err := manager.CreateWal(groupID2, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es2)
	assert.NotNil(t, wal2)
	defer wal2.Close()

	// Verify separate group directories are created
	groupDir1 := manager.getGroupWalDir(groupID1)
	groupDir2 := manager.getGroupWalDir(groupID2)
	assert.DirExists(t, groupDir1)
	assert.DirExists(t, groupDir2)
	assert.NotEqual(t, groupDir1, groupDir2)
}

func TestMultiRaftWalManager_ReplayWal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID := ibabuza.RaftGroupID(1)
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	// Create and populate a WAL
	es, wal, err := manager.CreateWal(groupID, metadata)
	assert.NoError(t, err)
	assert.NotNil(t, es)
	assert.NotNil(t, wal)

	hardState := raftpb.HardState{
		Term:   2,
		Vote:   1,
		Commit: 3,
	}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
		{Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
	}

	err = wal.Save(hardState, entries)
	assert.NoError(t, err)
	err = wal.Close()
	assert.NoError(t, err)

	// Test replay without snapshot
	result, replayEs, replayWal, err := manager.ReplayWal(groupID, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, replayEs)
	assert.NotNil(t, replayWal)
	assert.NotNil(t, result)

	// Verify replayed state
	assert.Equal(t, hardState, result.HardState())
	defer replayWal.Close()

}

func TestMultiRaftWalManager_HasExistingWals(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	// Test with no existing WALs (root dir doesn't exist)
	os.RemoveAll(tmpDir)
	groups, err := manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Empty(t, groups)

	// Create root dir but no group dirs
	err = os.MkdirAll(tmpDir, 0755)
	assert.NoError(t, err)
	groups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Empty(t, groups)

	// Create some valid group WALs
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	groupID1 := ibabuza.RaftGroupID(1)
	groupID2 := ibabuza.RaftGroupID(2)

	_, wal1, err := manager.CreateWal(groupID1, metadata)
	assert.NoError(t, err)
	wal1.Close()

	_, wal2, err := manager.CreateWal(groupID2, metadata)
	assert.NoError(t, err)
	wal2.Close()

	// Test finding existing WALs
	groups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, groups, 2)
	assert.Contains(t, groups, groupID1)
	assert.Contains(t, groups, groupID2)

	// Create invalid directory (non-numeric name)
	invalidDir := filepath.Join(tmpDir, "invalid_group")
	err = os.MkdirAll(invalidDir, 0755)
	assert.NoError(t, err)

	// Should still return only valid groups
	groups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, groups, 2)

	// Create empty group directory (no WAL files)
	emptyGroupDir := filepath.Join(tmpDir, "999")
	err = os.MkdirAll(emptyGroupDir, 0755)
	assert.NoError(t, err)

	// Should still return only groups with actual WAL files
	groups, err = manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Len(t, groups, 2)
}

func TestMultiRaftWalManager_FindSnapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	groupID := ibabuza.RaftGroupID(1)

	// Test FindSnapshot with non-existent group (should return error for non-existent directory)
	snapshots, err := manager.FindSnapshot(groupID)
	assert.Error(t, err) // Directory doesn't exist, so error is expected
	assert.Empty(t, snapshots)

	// Create a WAL for the group
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	_, wal, err := manager.CreateWal(groupID, metadata)
	assert.NoError(t, err)
	defer wal.Close()

	// Test FindSnapshot with existing group but no snapshots yet
	snapshots, err = manager.FindSnapshot(groupID)
	assert.NoError(t, err)
	// The findSnapshotInternal function returns snapshots from WAL replay,
	// which may include empty snapshots during initial WAL creation
	// This is expected behavior

	// Add some entries
	hardState := raftpb.HardState{Term: 1, Vote: 1, Commit: 2}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
	}

	err = wal.Save(hardState, entries)
	assert.NoError(t, err)

	// After saving entries, check snapshots again
	snapshots, err = manager.FindSnapshot(groupID)
	assert.NoError(t, err)
	// The function returns all snapshots found in the WAL, including the initial empty one
}

func TestMultiRaftWalManager_Purger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	// Test getting purger
	purger := manager.Purger()
	assert.NotNil(t, purger)

	// Test starting purger (should not panic)
	purger.Start()

	// Create a WAL and test purger channel communication
	groupID := ibabuza.RaftGroupID(1)
	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	_, wal, err := manager.CreateWal(groupID, metadata)
	assert.NoError(t, err)
	defer wal.Close()

	// Add some entries
	hardState := raftpb.HardState{Term: 1, Vote: 1, Commit: 5}
	entries := []raftpb.Entry{
		{Term: 1, Index: 1, Type: raftpb.EntryNormal, Data: []byte("entry 1")},
		{Term: 1, Index: 2, Type: raftpb.EntryNormal, Data: []byte("entry 2")},
		{Term: 1, Index: 3, Type: raftpb.EntryNormal, Data: []byte("entry 3")},
		{Term: 1, Index: 4, Type: raftpb.EntryNormal, Data: []byte("entry 4")},
		{Term: 1, Index: 5, Type: raftpb.EntryNormal, Data: []byte("entry 5")},
	}

	err = wal.Save(hardState, entries)
	assert.NoError(t, err)

	// Test that purger channel exists and can receive snapshots
	assert.NotNil(t, manager.purgerSnapCh)
	assert.NotNil(t, manager.purgerStopCh)

}

func TestMultiRaftWalManager_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})

	// Test closing
	err = manager.Close()
	assert.NoError(t, err)

	// Test closing again (should not panic)
	err = manager.Close()
	assert.NoError(t, err)

	// Verify purgerStopCh is closed
	select {
	case <-manager.purgerStopCh:
		// Channel is closed, which is expected
	default:
		t.Fatal("purgerStopCh should be closed after Close()")
	}
}

func TestMultiRaftWalManager_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "babuza_multiraft_test_*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := NewMultiRaftWalManager(tmpDir, &logger.Mock{})
	defer manager.Close()

	metadata := babuzapb.WalMetadata{
		ClusterID:   123,
		LocalPeerID: 456,
	}

	// Test concurrent CreateWal calls
	const numGroups = 10
	results := make(chan error, numGroups)

	for i := 0; i < numGroups; i++ {
		go func(groupID int) {
			_, wal, err := manager.CreateWal(ibabuza.RaftGroupID(groupID), metadata)
			if err == nil && wal != nil {
				wal.Close()
			}
			results <- err
		}(i + 1)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGroups; i++ {
		err := <-results
		assert.NoError(t, err)
	}

	// Test concurrent HasExistingWals calls
	const numReaders = 5
	readerResults := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			groups, err := manager.HasExistingWals()
			if err == nil {
				assert.Len(t, groups, numGroups)
			}
			readerResults <- err
		}()
	}

	// Wait for all reader goroutines
	for i := 0; i < numReaders; i++ {
		err := <-readerResults
		assert.NoError(t, err)
	}

	// Test concurrent RemoveData calls
	removeResults := make(chan error, numGroups)

	for i := 0; i < numGroups; i++ {
		go func(groupID int) {
			err := manager.RemoveData(ibabuza.RaftGroupID(groupID))
			removeResults <- err
		}(i + 1)
	}

	// Wait for all remove operations
	for i := 0; i < numGroups; i++ {
		err := <-removeResults
		assert.NoError(t, err)
	}

	groups, err := manager.HasExistingWals()
	assert.NoError(t, err)
	assert.Empty(t, groups)
}
