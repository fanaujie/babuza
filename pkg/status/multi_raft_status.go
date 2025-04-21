package status

import (
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

type MultiRaftStatus struct {
	mu    sync.RWMutex
	store map[ibabuza.RaftGroupID]ibabuza.Status
}

func NewMultiRaftStatus() *MultiRaftStatus {
	return &MultiRaftStatus{
		store: make(map[ibabuza.RaftGroupID]ibabuza.Status),
	}
}

func (m *MultiRaftStatus) Set(id ibabuza.RaftGroupID, s ibabuza.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.store[id]; !ok {
		m.store[id] = s
	}
}

func (m *MultiRaftStatus) Get(id ibabuza.RaftGroupID) ibabuza.Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if status, ok := m.store[id]; ok {
		return status
	}
	return nil
}
