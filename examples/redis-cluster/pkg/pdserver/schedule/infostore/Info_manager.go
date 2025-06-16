package infostore

import (
	"sync"
)

type InfoManager interface {
	AddOrUpdateGroup(groupID uint64, groupInfo GroupInfo)
	AddOrUpdateStore(storeID uint64, storeInfo StoreInfo)
	RandomLeaderRaftGroupOnStore(storeID uint64) (GroupInfo, bool)
	RandomFollowerRaftGroupOnStore(storeID uint64) (GroupInfo, bool)
	RaftGroupOnStore(groupID, storeID uint64) (GroupInfo, bool)
	RaftGroup(groupID uint64) (GroupInfo, bool)
	AllGroups() []GroupInfo
	Store(storeID uint64) (StoreInfo, bool)
	Stores() []StoreInfo
}

var _ InfoManager = (*InfoManagerImpl)(nil)

type InfoManagerImpl struct {
	group struct {
		mu            sync.Mutex
		groups        map[uint64]GroupInfo
		followerIndex map[uint64]map[uint64]bool
		leaderIndex   map[uint64]map[uint64]bool
	}
	store struct {
		mu     sync.RWMutex
		stores map[uint64]StoreInfo
	}
}

func NewInfoManager() *InfoManagerImpl {
	return &InfoManagerImpl{
		group: struct {
			mu            sync.Mutex
			groups        map[uint64]GroupInfo
			followerIndex map[uint64]map[uint64]bool
			leaderIndex   map[uint64]map[uint64]bool
		}{
			groups:        make(map[uint64]GroupInfo),
			followerIndex: make(map[uint64]map[uint64]bool),
			leaderIndex:   make(map[uint64]map[uint64]bool),
		},
		store: struct {
			mu     sync.RWMutex
			stores map[uint64]StoreInfo
		}{
			stores: make(map[uint64]StoreInfo),
		},
	}
}

func (i *InfoManagerImpl) Stores() []StoreInfo {
	i.store.mu.RLock()
	defer i.store.mu.RUnlock()

	stores := make([]StoreInfo, 0, len(i.store.stores))
	for _, storeInfo := range i.store.stores {
		stores = append(stores, storeInfo)
	}
	return stores
}

func (i *InfoManagerImpl) AddOrUpdateGroup(groupID uint64, groupInfo GroupInfo) {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	if oldGroup, exists := i.group.groups[groupID]; exists {
		if groups, ok := i.group.leaderIndex[oldGroup.storeID]; ok {
			delete(groups, groupID)
			if len(groups) == 0 {
				delete(i.group.leaderIndex, oldGroup.storeID)
			}
		}
		for _, peer := range oldGroup.peers {
			if peer.PeerID != oldGroup.leaderID {
				if groups, ok := i.group.followerIndex[peer.StoreID]; ok {
					delete(groups, groupID)
					if len(groups) == 0 {
						delete(i.group.followerIndex, peer.StoreID)
					}
				}
			}
		}
	}

	i.group.groups[groupID] = groupInfo
	if i.group.leaderIndex[groupInfo.storeID] == nil {
		i.group.leaderIndex[groupInfo.storeID] = make(map[uint64]bool)
	}
	i.group.leaderIndex[groupInfo.storeID][groupID] = true

	for _, peer := range groupInfo.peers {
		if peer.PeerID != groupInfo.leaderID {
			if i.group.followerIndex[peer.StoreID] == nil {
				i.group.followerIndex[peer.StoreID] = make(map[uint64]bool)
			}
			i.group.followerIndex[peer.StoreID][groupID] = true
		}
	}
}

func (i *InfoManagerImpl) AddOrUpdateStore(storeID uint64, storeInfo StoreInfo) {
	i.store.mu.Lock()
	defer i.store.mu.Unlock()
	i.store.stores[storeID] = storeInfo
}

func (i *InfoManagerImpl) RandomLeaderRaftGroupOnStore(storeID uint64) (GroupInfo, bool) {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	groupIDs, exists := i.group.leaderIndex[storeID]
	if !exists || len(groupIDs) == 0 {
		return GroupInfo{}, false
	}

	for groupID := range groupIDs {
		if groupInfo, exists := i.group.groups[groupID]; exists {
			return groupInfo, true
		}
	}

	return GroupInfo{}, false
}

func (i *InfoManagerImpl) RandomFollowerRaftGroupOnStore(storeID uint64) (GroupInfo, bool) {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	groupIDs, exists := i.group.followerIndex[storeID]
	if !exists || len(groupIDs) == 0 {
		return GroupInfo{}, false
	}

	for groupID := range groupIDs {
		if groupInfo, exists := i.group.groups[groupID]; exists {
			return groupInfo, true
		}
	}

	return GroupInfo{}, false
}

func (i *InfoManagerImpl) RaftGroupOnStore(groupID, storeID uint64) (GroupInfo, bool) {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	groupInfo, exists := i.group.groups[groupID]
	if !exists || groupInfo.storeID != storeID {
		return GroupInfo{}, false
	}
	return groupInfo, true
}

func (i *InfoManagerImpl) RaftGroup(groupID uint64) (GroupInfo, bool) {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	groupInfo, exists := i.group.groups[groupID]
	return groupInfo, exists
}

func (i *InfoManagerImpl) AllGroups() []GroupInfo {
	i.group.mu.Lock()
	defer i.group.mu.Unlock()

	groups := make([]GroupInfo, 0, len(i.group.groups))
	for _, groupInfo := range i.group.groups {
		groups = append(groups, groupInfo)
	}
	return groups
}

func (i *InfoManagerImpl) Store(storeID uint64) (StoreInfo, bool) {
	i.store.mu.RLock()
	defer i.store.mu.RUnlock()

	storeInfo, exists := i.store.stores[storeID]
	if !exists {
		return StoreInfo{}, false
	}
	return storeInfo, true
}
