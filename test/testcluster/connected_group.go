package testcluster

import (
	"sync"
)

type ConnectedGroup struct {
	ids map[uint64]struct{}
	mu  sync.RWMutex
}

func NewConnectedGroup(peerIDs []uint64) *ConnectedGroup {
	g := &ConnectedGroup{
		ids: make(map[uint64]struct{}),
	}
	for _, id := range peerIDs {
		g.Add(id)
	}
	return g
}

func (g *ConnectedGroup) Add(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ids[id] = struct{}{}
}

func (g *ConnectedGroup) Remove(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.ids, id)
}

func (g *ConnectedGroup) GetIds() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]uint64, 0, len(g.ids))
	for id := range g.ids {
		result = append(result, id)
	}
	return result
}
