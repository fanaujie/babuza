package transport

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"sync"
)

type ManagerImpl struct {
	peers     map[uint64]peer.Peer
	addresses map[uint64]string
	mu        sync.RWMutex
}

func NewPeerManager() PeerManager {
	return &ManagerImpl{
		peers:     make(map[uint64]peer.Peer),
		addresses: make(map[uint64]string),
	}
}

func (m *ManagerImpl) GetPeer(id uint64) peer.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[id]
	if !ok {
		return nil
	}
	return p
}

func (m *ManagerImpl) AddPeer(peerId uint64, peerAddress string, factory PeerFactory) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.peers[peerId]
	if ok {
		return fmt.Errorf("peer with id %d already exists", peerId)
	}
	p := factory.CreatePeer(peerId)
	m.peers[peerId] = p
	m.addresses[peerId] = peerAddress

	return nil
}

func (m *ManagerImpl) UpdatePeer(peerId uint64, peerAddress string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Only restart the peer if the address changed
	if currentAddr, _ := m.addresses[peerId]; currentAddr != peerAddress {
		m.addresses[peerId] = peerAddress
	}
	return nil
}

func (m *ManagerImpl) RemovePeer(peerId uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[peerId]
	if !ok {
		return fmt.Errorf("peer with id %d not found", peerId)
	}
	p.Stop()
	delete(m.peers, peerId)
	delete(m.addresses, peerId)
	return nil
}

func (m *ManagerImpl) RemoveAllPeers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeId, p := range m.peers {
		p.Stop()
		delete(m.peers, nodeId)
	}
	for id := range m.addresses {
		delete(m.addresses, id)
	}
}

func (m *ManagerImpl) ResolvePeerAddress(id uint64) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addr, ok := m.addresses[id]
	if !ok {
		return "", fmt.Errorf("peer with id %d not found", id)
	}
	return addr, nil
}

func (m *ManagerImpl) UpdatePeerRaftReport(raft ibabuza.RaftStatusReporter) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		p.UpdateRaftReport(raft)
	}
}
