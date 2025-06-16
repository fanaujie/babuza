package multiraft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"sort"
	"sync"
)

type StoreManager struct {
	storeMap sync.Map // map[StoreID]Store
}

func NewStoreManager() *StoreManager {
	return &StoreManager{}
}

func (m *StoreManager) Add(store *Store) error {
	storeID := store.config.StoreID
	if _, loaded := m.storeMap.LoadOrStore(storeID, store); loaded {
		return fmt.Errorf("store already exists: %d", storeID)
	}
	return nil
}

func (m *StoreManager) Clear() {
	m.storeMap.Clear()
}

func (m *StoreManager) Remove(storeID uint64) error {
	if _, loaded := m.storeMap.LoadAndDelete(storeID); !loaded {
		return fmt.Errorf("store not found: %d", storeID)
	}
	return nil
}

func (m *StoreManager) GetStoreIDsByGroupID(groupID ibabuza.RaftGroupID) []uint64 {
	allStores := make([]uint64, 0)
	m.storeMap.Range(func(key, value interface{}) bool {
		s := value.(*Store)
		if s.HasGroupID(groupID) {
			allStores = append(allStores, s.config.StoreID)
		}
		return true
	})
	if len(allStores) > 1 {
		sort.Slice(allStores, func(i, j int) bool { return allStores[i] < allStores[j] })
	}
	return allStores
}

func (m *StoreManager) GetAllStores() []*Store {
	allStores := make([]*Store, 0)
	m.storeMap.Range(func(key, value interface{}) bool {
		allStores = append(allStores, value.(*Store))
		return true
	})
	return allStores
}

func (m *StoreManager) GetStore(storeID uint64) (*Store, error) {
	v, ok := m.storeMap.Load(storeID)
	if !ok {
		return nil, fmt.Errorf("store not found: %d", storeID)
	}
	return v.(*Store), nil
}

func (m *StoreManager) CheckSameLeader(groupID ibabuza.RaftGroupID) (uint64, error) {
	stores := m.GetStoreIDsByGroupID(groupID)
	if len(stores) == 0 {
		return 0, errors.New("no stores found")
	}
	leaderID := uint64(0)
	for _, storeID := range stores {
		v, ok := m.storeMap.Load(storeID)
		if !ok {
			return 0, fmt.Errorf("store not found: %d", storeID)
		}
		s := v.(*Store)
		if !s.HasGroupID(groupID) {
			continue
		}
		status, err := s.RaftGroupStatus(groupID)
		if err != nil {
			return 0, err
		}
		if status.LeaderID == 0 {
			return 0, fmt.Errorf("groupID %d has no leader", groupID)
		}
		if leaderID == 0 {
			leaderID = status.LeaderID
		}
		if status.LeaderID != leaderID {
			return 0, fmt.Errorf("groupID %d has different leader %d", groupID, status.LeaderID)
		}
	}
	return leaderID, nil
}
