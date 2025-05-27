package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
)

type PeerAction[Reporter any] interface {
	Stop()
	UpdateRaftReport(reporter Reporter)
}

type PeerManager[Peer PeerAction[Reporter], Reporter any] interface {
	GetPeer(groupID ibabuza.RaftGroupID, peerID uint64) (Peer, error)
	GetPeerByAddress(peerAddr string) (Peer, error)
	AddPeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddr string, factory PeerFactory[Peer]) error
	UpdatePeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddr string, factory PeerFactory[Peer]) error
	RemovePeer(groupID ibabuza.RaftGroupID, peerID uint64) error
	RemoveAllPeers()
	ResolvePeerAddress(groupID ibabuza.RaftGroupID, peerID uint64) (string, error)
	UpdatePeerRaftReport(Reporter)
}

type PeerFactory[Peer any] interface {
	CreatePeer(peerAddress string) Peer
}
