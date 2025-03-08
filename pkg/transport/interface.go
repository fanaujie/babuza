package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/peer"
)

type PeerManager interface {
	GetPeer(id uint64) peer.Peer
	AddPeer(peerId uint64, peerAddress string, factory PeerFactory) error
	UpdatePeer(peerId uint64, peerAddress string) error
	RemovePeer(peerId uint64) error
	RemoveAllPeers()
	GetPeerAddress(id uint64) (string, error)
	UpdatePeerRaftReport(raft ibabuza.RaftStatusReporter)
}

type PeerFactory interface {
	CreatePeer(peerId uint64) peer.Peer
}
