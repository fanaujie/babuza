package infostore

import (
	"testing"

	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

func TestNewInfoManager(t *testing.T) {
	manager := NewInfoManager()
	if manager == nil {
		t.Fatal("NewInfoManager should not return nil")
	}
	
	if manager.group.groups == nil {
		t.Error("groups map should be initialized")
	}
	
	if manager.group.followerIndex == nil {
		t.Error("followerIndex map should be initialized")
	}
	
	if manager.group.leaderIndex == nil {
		t.Error("leaderIndex map should be initialized")
	}
	
	if manager.store.stores == nil {
		t.Error("stores map should be initialized")
	}
}

func TestAddOrUpdateGroup(t *testing.T) {
	manager := NewInfoManager()
	
	group1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
			{PeerID: 30, StoreID: 3},
		},
	}
	
	group2 := GroupInfo{
		storeID:  2,
		groupID:  200,
		leaderID: 21,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 21, StoreID: 2},
			{PeerID: 22, StoreID: 4},
			{PeerID: 23, StoreID: 5},
		},
	}
	
	manager.AddOrUpdateGroup(100, group1)
	manager.AddOrUpdateGroup(200, group2)
	
	storedGroup1, exists := manager.group.groups[100]
	if !exists {
		t.Fatal("Group 100 should be stored")
	}
	
	if storedGroup1.groupID != 100 {
		t.Errorf("Expected groupID 100, got %d", storedGroup1.groupID)
	}
	
	if storedGroup1.storeID != 1 {
		t.Errorf("Expected storeID 1, got %d", storedGroup1.storeID)
	}
	
	if storedGroup1.leaderID != 10 {
		t.Errorf("Expected leaderID 10, got %d", storedGroup1.leaderID)
	}
	
	if len(storedGroup1.peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(storedGroup1.peers))
	}
	
	storedGroup2, exists := manager.group.groups[200]
	if !exists {
		t.Fatal("Group 200 should be stored")
	}
	
	if storedGroup2.groupID != 200 {
		t.Errorf("Expected groupID 200, got %d", storedGroup2.groupID)
	}
	
	if storedGroup2.storeID != 2 {
		t.Errorf("Expected storeID 2, got %d", storedGroup2.storeID)
	}
	
	if storedGroup2.leaderID != 21 {
		t.Errorf("Expected leaderID 21, got %d", storedGroup2.leaderID)
	}
	
	if len(storedGroup2.peers) != 3 {
		t.Errorf("Expected 3 peers, got %d", len(storedGroup2.peers))
	}
	
	if len(manager.group.groups) != 2 {
		t.Errorf("Expected 2 groups in total, got %d", len(manager.group.groups))
	}
	
	updatedGroup1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 30,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 30, StoreID: 1},
			{PeerID: 40, StoreID: 4},
		},
	}
	
	manager.AddOrUpdateGroup(100, updatedGroup1)
	
	updatedStoredGroup1, exists := manager.group.groups[100]
	if !exists {
		t.Fatal("Updated group 100 should be stored")
	}
	
	if updatedStoredGroup1.leaderID != 30 {
		t.Errorf("Expected updated leaderID 30, got %d", updatedStoredGroup1.leaderID)
	}
	
	if len(updatedStoredGroup1.peers) != 2 {
		t.Errorf("Expected 2 peers after update, got %d", len(updatedStoredGroup1.peers))
	}
	
	if len(manager.group.groups) != 2 {
		t.Errorf("Expected still 2 groups after update, got %d", len(manager.group.groups))
	}
}

func TestAddOrUpdateGroupIndexes(t *testing.T) {
	manager := NewInfoManager()
	
	group1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
			{PeerID: 30, StoreID: 3},
		},
	}
	
	group2 := GroupInfo{
		storeID:  2,
		groupID:  200,
		leaderID: 21,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 21, StoreID: 2},
			{PeerID: 22, StoreID: 3},
			{PeerID: 23, StoreID: 4},
		},
	}
	
	manager.AddOrUpdateGroup(100, group1)
	manager.AddOrUpdateGroup(200, group2)
	
	if _, exists := manager.group.leaderIndex[1][100]; !exists {
		t.Error("Leader index should contain group 100 for store 1")
	}
	
	if _, exists := manager.group.leaderIndex[2][200]; !exists {
		t.Error("Leader index should contain group 200 for store 2")
	}
	
	if _, exists := manager.group.followerIndex[2][100]; !exists {
		t.Error("Follower index should contain group 100 for store 2")
	}
	
	if _, exists := manager.group.followerIndex[3][100]; !exists {
		t.Error("Follower index should contain group 100 for store 3")
	}
	
	if _, exists := manager.group.followerIndex[3][200]; !exists {
		t.Error("Follower index should contain group 200 for store 3")
	}
	
	if _, exists := manager.group.followerIndex[4][200]; !exists {
		t.Error("Follower index should contain group 200 for store 4")
	}
	
	updatedGroup1 := GroupInfo{
		storeID:  3,
		groupID:  100,
		leaderID: 30,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 30, StoreID: 3},
			{PeerID: 40, StoreID: 4},
			{PeerID: 50, StoreID: 5},
		},
	}
	
	manager.AddOrUpdateGroup(100, updatedGroup1)
	
	if _, exists := manager.group.leaderIndex[1][100]; exists {
		t.Error("Old leader index should be removed for group 100 from store 1")
	}
	
	if _, exists := manager.group.leaderIndex[3][100]; !exists {
		t.Error("New leader index should be added for group 100 on store 3")
	}
	
	if _, exists := manager.group.followerIndex[2][100]; exists {
		t.Error("Old follower index should be removed for group 100 from store 2")
	}
	
	if _, exists := manager.group.followerIndex[3][100]; exists {
		t.Error("Store 3 should not be in follower index since it's now the leader")
	}
	
	if _, exists := manager.group.followerIndex[4][100]; !exists {
		t.Error("New follower index should be added for group 100 on store 4")
	}
	
	if _, exists := manager.group.followerIndex[5][100]; !exists {
		t.Error("New follower index should be added for group 100 on store 5")
	}
	
	updatedGroup2 := GroupInfo{
		storeID:  4,
		groupID:  200,
		leaderID: 23,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 23, StoreID: 4},
			{PeerID: 24, StoreID: 6},
		},
	}
	
	manager.AddOrUpdateGroup(200, updatedGroup2)
	
	if _, exists := manager.group.leaderIndex[2][200]; exists {
		t.Error("Old leader index should be removed for group 200 from store 2")
	}
	
	if _, exists := manager.group.leaderIndex[4][200]; !exists {
		t.Error("New leader index should be added for group 200 on store 4")
	}
	
	if _, exists := manager.group.followerIndex[3][200]; exists {
		t.Error("Old follower index should be removed for group 200 from store 3")
	}
	
	if _, exists := manager.group.followerIndex[4][200]; exists {
		t.Error("Store 4 should not be in follower index since it's now the leader")
	}
	
	if _, exists := manager.group.followerIndex[6][200]; !exists {
		t.Error("New follower index should be added for group 200 on store 6")
	}
	
	if len(manager.group.followerIndex[4]) != 1 {
		t.Errorf("Expected 1 follower group on store 4, got %d", len(manager.group.followerIndex[4]))
	}
}

func TestRandomLeaderRaftGroupOnStore(t *testing.T) {
	manager := NewInfoManager()
	
	group1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
		},
	}
	
	group2 := GroupInfo{
		storeID:  1,
		groupID:  200,
		leaderID: 11,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 11, StoreID: 1},
			{PeerID: 21, StoreID: 3},
		},
	}
	
	group3 := GroupInfo{
		storeID:  2,
		groupID:  300,
		leaderID: 30,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 30, StoreID: 2},
			{PeerID: 31, StoreID: 4},
		},
	}
	
	manager.AddOrUpdateGroup(100, group1)
	manager.AddOrUpdateGroup(200, group2)
	manager.AddOrUpdateGroup(300, group3)
	
	result, found := manager.RandomLeaderRaftGroupOnStore(1)
	if !found {
		t.Fatal("Should find leader group on store 1")
	}
	
	if result.groupID != 100 && result.groupID != 200 {
		t.Errorf("Expected groupID 100 or 200, got %d", result.groupID)
	}
	
	result, found = manager.RandomLeaderRaftGroupOnStore(2)
	if !found {
		t.Fatal("Should find leader group on store 2")
	}
	
	if result.groupID != 300 {
		t.Errorf("Expected groupID 300, got %d", result.groupID)
	}
	
	_, found = manager.RandomLeaderRaftGroupOnStore(3)
	if found {
		t.Error("Should not find leader group on store 3")
	}
	
	_, found = manager.RandomLeaderRaftGroupOnStore(999)
	if found {
		t.Error("Should not find leader group on non-existent store")
	}
}

func TestRandomFollowerRaftGroupOnStore(t *testing.T) {
	manager := NewInfoManager()
	
	group1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
			{PeerID: 30, StoreID: 3},
		},
	}
	
	group2 := GroupInfo{
		storeID:  2,
		groupID:  200,
		leaderID: 21,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 21, StoreID: 2},
			{PeerID: 22, StoreID: 3},
			{PeerID: 23, StoreID: 4},
		},
	}
	
	group3 := GroupInfo{
		storeID:  4,
		groupID:  300,
		leaderID: 40,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 40, StoreID: 4},
			{PeerID: 41, StoreID: 2},
			{PeerID: 42, StoreID: 5},
		},
	}
	
	manager.AddOrUpdateGroup(100, group1)
	manager.AddOrUpdateGroup(200, group2)
	manager.AddOrUpdateGroup(300, group3)
	
	result, found := manager.RandomFollowerRaftGroupOnStore(2)
	if !found {
		t.Fatal("Should find follower group on store 2")
	}
	
	if result.groupID != 100 && result.groupID != 300 {
		t.Errorf("Expected groupID 100 or 300, got %d", result.groupID)
	}
	
	result, found = manager.RandomFollowerRaftGroupOnStore(3)
	if !found {
		t.Fatal("Should find follower group on store 3")
	}
	
	if result.groupID != 100 && result.groupID != 200 {
		t.Errorf("Expected groupID 100 or 200, got %d", result.groupID)
	}
	
	result, found = manager.RandomFollowerRaftGroupOnStore(5)
	if !found {
		t.Fatal("Should find follower group on store 5")
	}
	
	if result.groupID != 300 {
		t.Errorf("Expected groupID 300, got %d", result.groupID)
	}
	
	_, found = manager.RandomFollowerRaftGroupOnStore(1)
	if found {
		t.Error("Should not find follower group on leader-only store")
	}
	
	_, found = manager.RandomFollowerRaftGroupOnStore(999)
	if found {
		t.Error("Should not find follower group on non-existent store")
	}
}

func TestRaftGroupOnStore(t *testing.T) {
	manager := NewInfoManager()
	
	groupInfo := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
		},
	}
	
	manager.AddOrUpdateGroup(100, groupInfo)
	
	result, found := manager.RaftGroupOnStore(100, 1)
	if !found {
		t.Fatal("Should find group 100 on store 1")
	}
	
	if result.groupID != 100 {
		t.Errorf("Expected groupID 100, got %d", result.groupID)
	}
	
	_, found = manager.RaftGroupOnStore(100, 2)
	if found {
		t.Error("Should not find group 100 on store 2")
	}
	
	_, found = manager.RaftGroupOnStore(999, 1)
	if found {
		t.Error("Should not find non-existent group")
	}
}

func TestAddOrUpdateStore(t *testing.T) {
	manager := NewInfoManager()
	
	storeInfo1 := StoreInfo{
		storeID:     1,
		leaderCount: 5,
	}
	
	storeInfo2 := StoreInfo{
		storeID:     2,
		leaderCount: 3,
	}
	
	manager.AddOrUpdateStore(1, storeInfo1)
	manager.AddOrUpdateStore(2, storeInfo2)
	
	stored1, exists := manager.store.stores[1]
	if !exists {
		t.Fatal("Store 1 should be stored")
	}
	
	if stored1.storeID != 1 {
		t.Errorf("Expected storeID 1, got %d", stored1.storeID)
	}
	
	if stored1.leaderCount != 5 {
		t.Errorf("Expected leaderCount 5, got %d", stored1.leaderCount)
	}
	
	stored2, exists := manager.store.stores[2]
	if !exists {
		t.Fatal("Store 2 should be stored")
	}
	
	if stored2.storeID != 2 {
		t.Errorf("Expected storeID 2, got %d", stored2.storeID)
	}
	
	if stored2.leaderCount != 3 {
		t.Errorf("Expected leaderCount 3, got %d", stored2.leaderCount)
	}
	
	updatedStoreInfo1 := StoreInfo{
		storeID:     1,
		leaderCount: 10,
	}
	
	manager.AddOrUpdateStore(1, updatedStoreInfo1)
	
	updatedStored1, exists := manager.store.stores[1]
	if !exists {
		t.Fatal("Updated store 1 should be stored")
	}
	
	if updatedStored1.leaderCount != 10 {
		t.Errorf("Expected updated leaderCount 10, got %d", updatedStored1.leaderCount)
	}
	
	if len(manager.store.stores) != 2 {
		t.Errorf("Expected still 2 stores after update, got %d", len(manager.store.stores))
	}
}

func TestStore(t *testing.T) {
	manager := NewInfoManager()
	
	storeInfo := StoreInfo{
		storeID:     1,
		leaderCount: 5,
	}
	
	manager.AddOrUpdateStore(1, storeInfo)
	
	result, found := manager.Store(1)
	if !found {
		t.Fatal("Should find store 1")
	}
	
	if result.storeID != 1 {
		t.Errorf("Expected storeID 1, got %d", result.storeID)
	}
	
	_, found = manager.Store(999)
	if found {
		t.Error("Should not find non-existent store")
	}
}

func TestStores(t *testing.T) {
	manager := NewInfoManager()
	
	store1 := StoreInfo{storeID: 1, leaderCount: 5}
	store2 := StoreInfo{storeID: 2, leaderCount: 3}
	
	manager.AddOrUpdateStore(1, store1)
	manager.AddOrUpdateStore(2, store2)
	
	stores := manager.Stores()
	if len(stores) != 2 {
		t.Errorf("Expected 2 stores, got %d", len(stores))
	}
	
	storeIDs := make(map[uint64]bool)
	for _, store := range stores {
		storeIDs[store.storeID] = true
	}
	
	if !storeIDs[1] || !storeIDs[2] {
		t.Error("Should contain both store 1 and store 2")
	}
}

func TestUpdateExistingGroup(t *testing.T) {
	manager := NewInfoManager()
	
	originalGroup := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
		},
	}
	
	manager.AddOrUpdateGroup(100, originalGroup)
	
	updatedGroup := GroupInfo{
		storeID:  2,
		groupID:  100,
		leaderID: 20,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 20, StoreID: 2},
			{PeerID: 30, StoreID: 3},
		},
	}
	
	manager.AddOrUpdateGroup(100, updatedGroup)
	
	if _, exists := manager.group.leaderIndex[1][100]; exists {
		t.Error("Old leader index should be removed")
	}
	
	if _, exists := manager.group.leaderIndex[2][100]; !exists {
		t.Error("New leader index should be added")
	}
	
	if _, exists := manager.group.followerIndex[2][100]; exists {
		t.Error("Old follower index should be removed")
	}
	
	if _, exists := manager.group.followerIndex[3][100]; !exists {
		t.Error("New follower index should be added")
	}
	
	result, found := manager.RandomLeaderRaftGroupOnStore(2)
	if !found {
		t.Fatal("Should find leader on store 2")
	}
	
	if result.leaderID != 20 {
		t.Errorf("Expected leaderID 20, got %d", result.leaderID)
	}
}

func TestMultipleGroupsOnSameStore(t *testing.T) {
	manager := NewInfoManager()
	
	group1 := GroupInfo{
		storeID:  1,
		groupID:  100,
		leaderID: 10,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 10, StoreID: 1},
			{PeerID: 20, StoreID: 2},
		},
	}
	
	group2 := GroupInfo{
		storeID:  1,
		groupID:  200,
		leaderID: 11,
		peers: []babuzapb.RaftPeerAttribute{
			{PeerID: 11, StoreID: 1},
			{PeerID: 21, StoreID: 2},
		},
	}
	
	manager.AddOrUpdateGroup(100, group1)
	manager.AddOrUpdateGroup(200, group2)
	
	if len(manager.group.leaderIndex[1]) != 2 {
		t.Errorf("Expected 2 leader groups on store 1, got %d", len(manager.group.leaderIndex[1]))
	}
	
	if len(manager.group.followerIndex[2]) != 2 {
		t.Errorf("Expected 2 follower groups on store 2, got %d", len(manager.group.followerIndex[2]))
	}
	
	result, found := manager.RandomLeaderRaftGroupOnStore(1)
	if !found {
		t.Fatal("Should find leader group on store 1")
	}
	
	if result.groupID != 100 && result.groupID != 200 {
		t.Errorf("Should return either group 100 or 200, got %d", result.groupID)
	}
}