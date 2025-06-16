package schedule

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/schedulers"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

// TestCoordinatorLifecycle tests the basic lifecycle of coordinator
func TestCoordinatorLifecycle(t *testing.T) {
	coordinator := NewCoordinator()

	// Verify initial state
	if coordinator.stopCh == nil {
		t.Fatal("stopCh should be initialized")
	}
	if coordinator.infoManager == nil {
		t.Fatal("infoManager should be initialized")
	}

	// Stop should not block
	coordinator.Stop()
}

// TestCoordinatorSchedulerManagement tests scheduler addition and management
func TestCoordinatorSchedulerManagement(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	scheduler1 := schedulers.NewTransferLeaderScheduler("scheduler-1")
	scheduler2 := schedulers.NewTransferLeaderScheduler("scheduler-2")

	// Test adding first scheduler
	err := coordinator.AddScheduleTask(scheduler1)
	if err != nil {
		t.Fatalf("Failed to add first scheduler: %v", err)
	}

	// Test adding second scheduler
	err = coordinator.AddScheduleTask(scheduler2)
	if err != nil {
		t.Fatalf("Failed to add second scheduler: %v", err)
	}

	// Test adding duplicate scheduler
	err = coordinator.AddScheduleTask(scheduler1)
	if err == nil {
		t.Error("Should fail when adding duplicate scheduler")
	}
	if err.Error() != "scheduler scheduler-1 already exists" {
		t.Errorf("Unexpected error message: %v", err)
	}

	// Give time for goroutines to start
	time.Sleep(10 * time.Millisecond)
}

// TestStoreHeartbeatDataFlow tests store heartbeat data processing
func TestStoreHeartbeatDataFlow(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	testCases := []struct {
		name        string
		req         pb.StoreHeartbeatReq
		expectError bool
	}{
		{
			name: "valid_store_heartbeat",
			req: pb.StoreHeartbeatReq{
				ClusterID:   1,
				StoreID:     100,
				LeaderCount: 5,
			},
			expectError: false,
		},
		{
			name: "zero_store_id",
			req: pb.StoreHeartbeatReq{
				ClusterID:   1,
				StoreID:     0,
				LeaderCount: 3,
			},
			expectError: false, // Should be handled gracefully
		},
		{
			name: "high_leader_count",
			req: pb.StoreHeartbeatReq{
				ClusterID:   1,
				StoreID:     200,
				LeaderCount: 10000,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := coordinator.DoStoreHeartbeat(tc.req)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			} else if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err == nil {
				if resp.ClusterID != tc.req.ClusterID {
					t.Errorf("Expected ClusterID %d, got %d", tc.req.ClusterID, resp.ClusterID)
				}

				// Verify store is registered
				store, found := coordinator.infoManager.Store(tc.req.StoreID)
				if !found {
					t.Fatal("Store should be registered after heartbeat")
				}
				if store.StoreID() != tc.req.StoreID {
					t.Errorf("Expected store ID %d, got %d", tc.req.StoreID, store.StoreID())
				}
				// Leader count should match what was sent in the heartbeat
				if store.LeaderCount() != tc.req.LeaderCount {
					t.Errorf("Expected leader count %d, got %d", tc.req.LeaderCount, store.LeaderCount())
				}
			}
		})
	}
}

// TestStoreHeartbeatConcurrency tests concurrent store heartbeats
func TestStoreHeartbeatConcurrency(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	numGoroutines := 10
	storesPerGoroutine := 5
	var wg sync.WaitGroup

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < storesPerGoroutine; j++ {
				storeID := uint64(routineID*storesPerGoroutine + j + 1)
				req := pb.StoreHeartbeatReq{
					ClusterID:   1,
					StoreID:     storeID,
					LeaderCount: uint64(j + 1),
				}
				_, err := coordinator.DoStoreHeartbeat(req)
				if err != nil {
					t.Errorf("Concurrent heartbeat failed for store %d: %v", storeID, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all stores are registered
	stores := coordinator.infoManager.Stores()
	expectedStoreCount := numGoroutines * storesPerGoroutine
	if len(stores) != expectedStoreCount {
		t.Errorf("Expected %d stores, got %d", expectedStoreCount, len(stores))
	}
}

// TestRaftGroupHeartbeatDataFlow tests raft group heartbeat data processing
func TestRaftGroupHeartbeatDataFlow(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	// First register stores
	storeIDs := []uint64{100, 200, 300}
	for _, storeID := range storeIDs {
		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     storeID,
			LeaderCount: 0,
		})
		if err != nil {
			t.Fatalf("Failed to register store %d: %v", storeID, err)
		}
	}

	testCases := []struct {
		name    string
		req     pb.RaftGroupLeaderHeartbeatReq
		wantErr bool
	}{
		{
			name: "valid_group_heartbeat",
			req: pb.RaftGroupLeaderHeartbeatReq{
				GroupID:  1001,
				StoreID:  100,
				LeaderID: 10001,
				Peers: []babuzapb.RaftPeerAttribute{
					{PeerID: 10001, StoreID: 100},
					{PeerID: 10002, StoreID: 200},
				},
			},
			wantErr: false,
		},
		{
			name: "group_with_single_peer",
			req: pb.RaftGroupLeaderHeartbeatReq{
				GroupID:  1002,
				StoreID:  200,
				LeaderID: 20001,
				Peers: []babuzapb.RaftPeerAttribute{
					{PeerID: 20001, StoreID: 200},
				},
			},
			wantErr: false,
		},
		{
			name: "group_with_multiple_peers",
			req: pb.RaftGroupLeaderHeartbeatReq{
				GroupID:  1003,
				StoreID:  300,
				LeaderID: 30001,
				Peers: []babuzapb.RaftPeerAttribute{
					{PeerID: 30001, StoreID: 300},
					{PeerID: 30002, StoreID: 100},
					{PeerID: 30003, StoreID: 200},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := coordinator.DoRaftGroupLeaderHeartbeat(tc.req)

			if tc.wantErr && err == nil {
				t.Error("Expected error but got none")
			} else if !tc.wantErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if err == nil {
				if resp == nil {
					t.Error("Response should not be nil")
				}

				// Verify group is registered
				group, found := coordinator.infoManager.RaftGroupOnStore(tc.req.GroupID, tc.req.StoreID)
				if !found {
					t.Fatal("Group should be registered after heartbeat")
				}
				if group.GroupID() != tc.req.GroupID {
					t.Errorf("Expected group ID %d, got %d", tc.req.GroupID, group.GroupID())
				}
				leader, hasLeader := group.Leader()
				if !hasLeader {
					t.Error("Group should have a leader")
				} else if leader.PeerID != tc.req.LeaderID {
					t.Errorf("Expected leader ID %d, got %d", tc.req.LeaderID, leader.PeerID)
				}
				if len(group.Peers()) != len(tc.req.Peers) {
					t.Errorf("Expected %d peers, got %d", len(tc.req.Peers), len(group.Peers()))
				}
			}
		})
	}
}

// TestRaftGroupHeartbeatWithOperator tests raft group heartbeat with pending operations
func TestRaftGroupHeartbeatWithOperator(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	// Setup stores
	storeIDs := []uint64{100, 200}
	for _, storeID := range storeIDs {
		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     storeID,
			LeaderCount: 3,
		})
		if err != nil {
			t.Fatalf("Failed to register store %d: %v", storeID, err)
		}
	}

	// Setup raft group
	groupID := uint64(1001)
	_, err := coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
		GroupID:  groupID,
		StoreID:  100,
		LeaderID: 10001,
		Peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10001, StoreID: 100},
			{PeerID: 10002, StoreID: 200},
		},
	})
	if err != nil {
		t.Fatalf("Failed to register group: %v", err)
	}

	// Add scheduler to generate operations
	scheduler := schedulers.NewTransferLeaderScheduler("test-scheduler")
	err = coordinator.AddScheduleTask(scheduler)
	if err != nil {
		t.Fatalf("Failed to add scheduler: %v", err)
	}

	// Give scheduler time to generate operations
	time.Sleep(20 * time.Millisecond)

	// Send heartbeat again to test operator processing
	resp, err := coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
		GroupID:  groupID,
		StoreID:  100,
		LeaderID: 10001,
		Peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10001, StoreID: 100},
			{PeerID: 10002, StoreID: 200},
		},
	})
	if err != nil {
		t.Fatalf("Second heartbeat failed: %v", err)
	}

	// Response should be valid regardless of whether operation exists
	if resp == nil {
		t.Error("Response should not be nil")
	}
}

// TestDataConsistencyAfterUpdates tests data consistency after multiple updates
func TestDataConsistencyAfterUpdates(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	storeID := uint64(100)
	groupID := uint64(1001)

	// Initial store registration
	_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
		ClusterID:   1,
		StoreID:     storeID,
		LeaderCount: 5,
	})
	if err != nil {
		t.Fatalf("Initial store heartbeat failed: %v", err)
	}

	// Verify initial state - store should be registered
	store, found := coordinator.infoManager.Store(storeID)
	if !found {
		t.Fatal("Store should be found after registration")
	}
	if store.StoreID() != storeID {
		t.Errorf("Expected store ID %d, got %d", storeID, store.StoreID())
	}

	// Update store with different leader count
	_, err = coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
		ClusterID:   1,
		StoreID:     storeID,
		LeaderCount: 8,
	})
	if err != nil {
		t.Fatalf("Store update failed: %v", err)
	}

	// Verify update - store should still be found
	store, found = coordinator.infoManager.Store(storeID)
	if !found {
		t.Fatal("Store should still be found after update")
	}
	if store.StoreID() != storeID {
		t.Errorf("Expected store ID %d after update, got %d", storeID, store.StoreID())
	}

	// Add raft group
	_, err = coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
		GroupID:  groupID,
		StoreID:  storeID,
		LeaderID: 10001,
		Peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10001, StoreID: storeID},
		},
	})
	if err != nil {
		t.Fatalf("Group registration failed: %v", err)
	}

	// Verify group registration
	group, found := coordinator.infoManager.RaftGroupOnStore(groupID, storeID)
	if !found {
		t.Fatal("Group should be found after registration")
	}
	if group.GroupID() != groupID {
		t.Errorf("Expected group ID %d, got %d", groupID, group.GroupID())
	}

	// After group registration, store should maintain the last set leader count (8)
	store, found = coordinator.infoManager.Store(storeID)
	if !found {
		t.Fatal("Store should still be found after group registration")
	}
	if store.LeaderCount() != 8 {
		t.Errorf("Expected leader count 8 after group registration, got %d", store.LeaderCount())
	}

	// Update group with new leader
	_, err = coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
		GroupID:  groupID,
		StoreID:  storeID,
		LeaderID: 10002,
		Peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10002, StoreID: storeID},
		},
	})
	if err != nil {
		t.Fatalf("Group update failed: %v", err)
	}

	// Verify group update
	group, found = coordinator.infoManager.RaftGroupOnStore(groupID, storeID)
	if !found {
		t.Fatal("Group should still be found after update")
	}
	leader, hasLeader := group.Leader()
	if !hasLeader {
		t.Error("Group should have a leader after update")
	} else if leader.PeerID != 10002 {
		t.Errorf("Expected updated leader ID 10002, got %d", leader.PeerID)
	}

	// Leader count should still be 8 (unchanged by group updates)
	store, found = coordinator.infoManager.Store(storeID)
	if !found {
		t.Fatal("Store should still be found after group update")
	}
	if store.LeaderCount() != 8 {
		t.Errorf("Expected leader count 8 after group update, got %d", store.LeaderCount())
	}
}

// TestMultipleStoresDataConsistency tests data consistency across multiple stores
func TestMultipleStoresDataConsistency(t *testing.T) {
	coordinator := NewCoordinator()
	defer coordinator.Stop()

	stores := []struct {
		storeID     uint64
		leaderCount uint64
	}{
		{100, 3},
		{200, 5},
		{300, 2},
		{400, 7},
		{500, 1},
	}

	// Register all stores
	for _, store := range stores {
		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     store.storeID,
			LeaderCount: store.leaderCount,
		})
		if err != nil {
			t.Fatalf("Store %d registration failed: %v", store.storeID, err)
		}
	}

	// Verify all stores are registered
	allStores := coordinator.infoManager.Stores()
	if len(allStores) != len(stores) {
		t.Fatalf("Expected %d stores, got %d", len(stores), len(allStores))
	}

	// Create maps for store ID and leader count verification
	expectedStoreIDs := make(map[uint64]bool)
	expectedLeaderCounts := make(map[uint64]uint64)
	for _, store := range stores {
		expectedStoreIDs[store.storeID] = true
		expectedLeaderCounts[store.storeID] = store.leaderCount
	}

	// Verify each store has correct ID and leader count
	for _, actualStore := range allStores {
		if !expectedStoreIDs[actualStore.StoreID()] {
			t.Errorf("Unexpected store ID %d found", actualStore.StoreID())
			continue
		}
		expectedCount := expectedLeaderCounts[actualStore.StoreID()]
		if actualStore.LeaderCount() != expectedCount {
			t.Errorf("Store %d: expected leader count %d, got %d", actualStore.StoreID(), expectedCount, actualStore.LeaderCount())
		}
	}

	// Test concurrent updates - this tests thread safety
	var wg sync.WaitGroup
	wg.Add(len(stores))

	for _, store := range stores {
		go func(storeID, newLeaderCount uint64) {
			defer wg.Done()
			_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
				ClusterID:   1,
				StoreID:     storeID,
				LeaderCount: newLeaderCount,
			})
			if err != nil {
				t.Errorf("Concurrent update failed for store %d: %v", storeID, err)
			}
		}(store.storeID, store.leaderCount+10)
	}

	wg.Wait()

	// Verify concurrent updates - all stores should still be present
	updatedStores := coordinator.infoManager.Stores()
	if len(updatedStores) != len(stores) {
		t.Fatalf("Expected %d stores after update, got %d", len(stores), len(updatedStores))
	}

	// Verify all expected store IDs are still present after concurrent updates
	actualStoreIDs := make(map[uint64]bool)
	for _, actualStore := range updatedStores {
		actualStoreIDs[actualStore.StoreID()] = true
	}

	for expectedID := range expectedStoreIDs {
		if !actualStoreIDs[expectedID] {
			t.Errorf("Store ID %d missing after concurrent updates", expectedID)
		}
	}
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("empty_peers_list", func(t *testing.T) {
		coordinator := NewCoordinator()
		defer coordinator.Stop()

		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     100,
			LeaderCount: 1,
		})
		if err != nil {
			t.Fatalf("Store registration failed: %v", err)
		}

		// Group with empty peers list
		_, err = coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
			GroupID:  1001,
			StoreID:  100,
			LeaderID: 10001,
			Peers:    []babuzapb.RaftPeerAttribute{},
		})
		if err != nil {
			t.Fatalf("Group with empty peers failed: %v", err)
		}

		// Verify group is still registered
		group, found := coordinator.infoManager.RaftGroupOnStore(1001, 100)
		if !found {
			t.Error("Group should be registered even with empty peers")
		} else if len(group.Peers()) != 0 {
			t.Errorf("Expected 0 peers, got %d", len(group.Peers()))
		}
	})

	t.Run("duplicate_peer_ids", func(t *testing.T) {
		coordinator := NewCoordinator()
		defer coordinator.Stop()

		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     100,
			LeaderCount: 1,
		})
		if err != nil {
			t.Fatalf("Store registration failed: %v", err)
		}

		// Group with duplicate peer IDs
		_, err = coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
			GroupID:  1001,
			StoreID:  100,
			LeaderID: 10001,
			Peers: []babuzapb.RaftPeerAttribute{
				{PeerID: 10001, StoreID: 100},
				{PeerID: 10001, StoreID: 100}, // Duplicate
			},
		})
		if err != nil {
			t.Fatalf("Group with duplicate peers failed: %v", err)
		}

		// Verify group is registered
		group, found := coordinator.infoManager.RaftGroupOnStore(1001, 100)
		if !found {
			t.Error("Group should be registered")
		} else if len(group.Peers()) != 2 {
			t.Errorf("Expected 2 peers (including duplicate), got %d", len(group.Peers()))
		}
	})

	t.Run("zero_values", func(t *testing.T) {
		coordinator := NewCoordinator()
		defer coordinator.Stop()

		// Store with zero leader count
		_, err := coordinator.DoStoreHeartbeat(pb.StoreHeartbeatReq{
			ClusterID:   1,
			StoreID:     100,
			LeaderCount: 0,
		})
		if err != nil {
			t.Fatalf("Store with zero leaders failed: %v", err)
		}

		store, found := coordinator.infoManager.Store(100)
		if !found {
			t.Error("Store should be registered")
		} else if store.LeaderCount() != 0 {
			t.Errorf("Expected 0 leaders, got %d", store.LeaderCount())
		}

		// Group with zero IDs
		_, err = coordinator.DoRaftGroupLeaderHeartbeat(pb.RaftGroupLeaderHeartbeatReq{
			GroupID:  0,
			StoreID:  100,
			LeaderID: 0,
			Peers: []babuzapb.RaftPeerAttribute{
				{PeerID: 0, StoreID: 100},
			},
		})
		if err != nil {
			t.Fatalf("Group with zero IDs failed: %v", err)
		}

		// Verify group is registered
		_, found = coordinator.infoManager.RaftGroupOnStore(0, 100)
		if !found {
			t.Error("Group should be registered even with zero IDs")
		}
	})
}

// TestCoordinatorStopBehavior tests proper shutdown behavior
func TestCoordinatorStopBehavior(t *testing.T) {
	coordinator := NewCoordinator()

	// Add multiple schedulers
	for i := 0; i < 3; i++ {
		scheduler := schedulers.NewTransferLeaderScheduler(fmt.Sprintf("scheduler-%d", i))
		err := coordinator.AddScheduleTask(scheduler)
		if err != nil {
			t.Fatalf("Failed to add scheduler %d: %v", i, err)
		}
	}

	// Give goroutines time to start
	time.Sleep(10 * time.Millisecond)

	// Stop should complete without hanging
	done := make(chan bool)
	go func() {
		coordinator.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Coordinator.stop() did not complete within timeout")
	}
}
