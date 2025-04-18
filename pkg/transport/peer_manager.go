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

func (m *ManagerImpl) AddPeer(peerID uint64, peerAddress string, factory PeerFactory) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.peers[peerID]
	if ok {
		return fmt.Errorf("peer with id %d already exists", peerID)
	}
	p := factory.CreatePeer(peerID)
	m.peers[peerID] = p
	m.addresses[peerID] = peerAddress

	return nil
}

func (m *ManagerImpl) UpdatePeer(peerID uint64, peerAddress string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.addresses[peerID]
	if ok {
		m.addresses[peerID] = peerAddress
		return nil
	}
	return fmt.Errorf("peer with id %d not found", peerID)
}

func (m *ManagerImpl) RemovePeer(peerID uint64) error {
	m.mu.Lock()
	p, ok := m.peers[peerID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("peer with id %d not found", peerID)
	}
	delete(m.peers, peerID)
	delete(m.addresses, peerID)
	m.mu.Unlock()
	p.Stop()
	return nil
}

func (m *ManagerImpl) RemoveAllPeers() {
	var peers []peer.Peer
	m.mu.Lock()
	for nodeId, p := range m.peers {
		peers = append(peers, p)
		delete(m.peers, nodeId)
	}
	for id := range m.addresses {
		delete(m.addresses, id)
	}
	m.mu.Unlock()
	for _, p := range peers {
		p.Stop()
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
