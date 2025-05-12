package transport

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/puzpuzpuz/xsync/v4"
)

type MultiRaftManagerImpl struct {
	peers     *xsync.Map[uint64, peer.MultiRaftPeer]
	addresses *xsync.Map[uint64, string]
}

func NewMultiRaftPeerManager() *MultiRaftManagerImpl {
	return &MultiRaftManagerImpl{
		peers:     xsync.NewMap[uint64, peer.MultiRaftPeer](),
		addresses: xsync.NewMap[uint64, string](),
	}
}

func (m *MultiRaftManagerImpl) GetPeer(id uint64) (peer.MultiRaftPeer, error) {
	p, ok := m.peers.Load(id)
	if !ok {
		return nil, fmt.Errorf("peer with id %d not found", id)
	}
	return p.(peer.MultiRaftPeer), nil
}

func (m *MultiRaftManagerImpl) AddPeer(peerID uint64, peerAddress string, factory MultiRaftPeerFactory) error {
	_, ok := m.peers.Load(peerID)
	if ok {
		return fmt.Errorf("peer with id %d already exists", peerID)
	}
	p := factory.CreatePeer(peerID)
	m.peers.Store(peerID, p)
	m.addresses.Store(peerID, peerAddress)
	return nil
}

func (m *MultiRaftManagerImpl) UpdatePeer(peerID uint64, peerAddress string) error {
	_, ok := m.addresses.Load(peerID)
	if ok {
		m.addresses.Store(peerID, peerAddress)
		return nil
	}
	return fmt.Errorf("peer with id %d not found", peerID)
}

func (m *MultiRaftManagerImpl) RemovePeer(peerID uint64) error {
	p, ok := m.peers.Load(peerID)
	if !ok {
		return fmt.Errorf("peer with id %d not found", peerID)
	}
	m.peers.Delete(peerID)
	m.addresses.Delete(peerID)
	p.(peer.MultiRaftPeer).Stop()
	return nil
}

func (m *MultiRaftManagerImpl) RemoveAllPeers() {
	var peersToStop []peer.MultiRaftPeer
	m.peers.Range(func(key uint64, value peer.MultiRaftPeer) bool {
		peersToStop = append(peersToStop, value)
		m.peers.Delete(key)
		return true
	})
	m.addresses.Range(func(key uint64, value string) bool {
		m.addresses.Delete(key)
		return true
	})
	for _, p := range peersToStop {
		p.Stop()
	}
}

func (m *MultiRaftManagerImpl) ResolvePeerAddress(id uint64) (string, error) {
	addr, ok := m.addresses.Load(id)
	if !ok {
		return "", fmt.Errorf("peer with id %d not found", id)
	}
	return addr, nil
}

func (m *MultiRaftManagerImpl) UpdatePeerRaftReport(raft ibabuza.MultiRaftStatusReporter) {
	m.peers.Range(func(key uint64, value peer.MultiRaftPeer) bool {
		value.UpdateRaftReport(raft)
		return true
	})
}
