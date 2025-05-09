package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/peer"
)

type PeerManager interface {
	GetPeer(id uint64) peer.Peer
	AddPeer(peerID uint64, peerAddress string, factory PeerFactory) error
	UpdatePeer(peerID uint64, peerAddress string) error
	RemovePeer(peerID uint64) error
	RemoveAllPeers()
	ResolvePeerAddress(id uint64) (string, error)
	UpdatePeerRaftReport(raft ibabuza.RaftStatusReporter)
}

type PeerFactory interface {
	CreatePeer(peerID uint64) peer.Peer
}

type MultiRaftPeerFactory interface {
	CreatePeer(peerID uint64) peer.MultiRaftPeer
}

type MultiRaftPeerManager interface {
	GetPeer(id uint64) (peer.MultiRaftPeer, error)
	AddPeer(peerID uint64, peerAddress string, factory MultiRaftPeerFactory) error
	UpdatePeer(peerID uint64, peerAddress string) error
	RemovePeer(peerID uint64) error
	RemoveAllPeers()
	ResolvePeerAddress(id uint64) (string, error)
	UpdatePeerRaftReport(raft ibabuza.MultiRaftStatusReporter)
}
