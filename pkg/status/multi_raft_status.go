package status

import (
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

type CreateStatusFactory func() ibabuza.Status

type MultiRaftStatus struct {
	mu      sync.RWMutex
	store   map[ibabuza.RaftGroupID]ibabuza.Status
	factory CreateStatusFactory
}

func NewMultiRaftStatus(factory CreateStatusFactory) *MultiRaftStatus {
	return &MultiRaftStatus{
		store: make(map[ibabuza.RaftGroupID]ibabuza.Status),
	}
}

func (m *MultiRaftStatus) Set(id ibabuza.RaftGroupID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.store[id]; !ok {
		m.store[id] = m.factory()
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
