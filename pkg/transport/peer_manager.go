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

package transport

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

type RaftPeerIdentifier struct {
	GroupID ibabuza.RaftGroupID
	PeerID  uint64
}

type PeerManagerImpl[Peer PeerAction[Reporter], Reporter any] struct {
	mu        sync.RWMutex
	peers     map[string]Peer
	addresses map[RaftPeerIdentifier]string
	refCounts map[string]int
}

func NewPeerManager[Peer PeerAction[Reporter], Reporter any]() *PeerManagerImpl[Peer, Reporter] {
	return &PeerManagerImpl[Peer, Reporter]{
		peers:     make(map[string]Peer),
		addresses: make(map[RaftPeerIdentifier]string),
		refCounts: make(map[string]int),
	}
}

func (m *PeerManagerImpl[Peer, Reporter]) GetPeer(groupID ibabuza.RaftGroupID, peerID uint64) (Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	address, exists := m.addresses[identifier]
	if !exists {
		var empty Peer
		return empty, fmt.Errorf("peer not found for group %v and peer %v", groupID, peerID)
	}

	peer, exists := m.peers[address]
	if !exists {
		var empty Peer
		return empty, fmt.Errorf("peer address %s exists but peer not found", address)
	}

	return peer, nil
}

func (m *PeerManagerImpl[Peer, Reporter]) AddPeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddress string, factory PeerFactory[Peer]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	if _, ok := m.addresses[identifier]; !ok {
		m.addresses[identifier] = peerAddress
		m.retainAddressLocked(peerAddress, factory)
		return nil
	}

	return fmt.Errorf("peer already exists for group %v and peer %v", groupID, peerID)
}

func (m *PeerManagerImpl[Peer, Reporter]) UpdatePeer(groupID ibabuza.RaftGroupID, peerID uint64, updatePeerAddress string,
	factory PeerFactory[Peer]) error {

	m.mu.Lock()

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	oldAddress, exists := m.addresses[identifier]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("peer not found for group %v and peer %v", groupID, peerID)

	}
	if oldAddress == updatePeerAddress {
		m.mu.Unlock()
		return nil
	}

	peerToStop, stopPeer := m.releaseAddressLocked(oldAddress)

	m.addresses[identifier] = updatePeerAddress
	m.retainAddressLocked(updatePeerAddress, factory)
	m.mu.Unlock()

	stopPeerIfNeeded(peerToStop, stopPeer)
	return nil
}

func (m *PeerManagerImpl[Peer, Reporter]) RemovePeer(groupID ibabuza.RaftGroupID, peerID uint64) error {
	m.mu.Lock()

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}

	address, exists := m.addresses[identifier]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("peer not found for group %v and peer %v", groupID, peerID)
	}
	delete(m.addresses, identifier)
	peerToStop, stopPeer := m.releaseAddressLocked(address)
	m.mu.Unlock()

	stopPeerIfNeeded(peerToStop, stopPeer)
	return nil
}

func (m *PeerManagerImpl[Peer, Reporter]) RemoveAllPeers() {
	m.mu.Lock()
	peers := m.peers
	m.peers = make(map[string]Peer)
	m.addresses = make(map[RaftPeerIdentifier]string)
	m.refCounts = make(map[string]int)
	m.mu.Unlock()

	for _, peer := range peers {
		peer.Stop()
	}
}

func (m *PeerManagerImpl[Peer, Reporter]) retainAddressLocked(peerAddress string, factory PeerFactory[Peer]) {
	if _, exists := m.peers[peerAddress]; exists {
		m.refCounts[peerAddress]++
		return
	}
	m.peers[peerAddress] = factory.CreatePeer(peerAddress)
	m.refCounts[peerAddress] = 1
}

func (m *PeerManagerImpl[Peer, Reporter]) releaseAddressLocked(peerAddress string) (Peer, bool) {
	var peerToStop Peer
	m.refCounts[peerAddress]--
	if m.refCounts[peerAddress] > 0 {
		return peerToStop, false
	}
	peerToStop = m.peers[peerAddress]
	delete(m.peers, peerAddress)
	delete(m.refCounts, peerAddress)
	return peerToStop, true
}

func stopPeerIfNeeded[Peer PeerAction[Reporter], Reporter any](peer Peer, shouldStop bool) {
	if shouldStop {
		peer.Stop()
	}
}

func (m *PeerManagerImpl[Peer, Reporter]) ResolvePeerAddress(groupID ibabuza.RaftGroupID, peerID uint64) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	address, exists := m.addresses[identifier]
	if !exists {
		return "", fmt.Errorf("peer address not found for group %v and peer %v", groupID, peerID)
	}

	return address, nil
}

func (m *PeerManagerImpl[Peer, Reporter]) GetPeerByAddress(peerAddr string) (Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, exists := m.peers[peerAddr]
	if !exists {
		var empty Peer
		return empty, fmt.Errorf("peer not found for address %s", peerAddr)
	}
	return peer, nil
}

func (m *PeerManagerImpl[Peer, Reporter]) UpdatePeerRaftReport(reporter Reporter) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, peer := range m.peers {
		peer.UpdateRaftReport(reporter)
	}
}
