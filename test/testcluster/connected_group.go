// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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

func (g *ConnectedGroup) GetIDs() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]uint64, 0, len(g.ids))
	for id := range g.ids {
		result = append(result, id)
	}
	return result
}
