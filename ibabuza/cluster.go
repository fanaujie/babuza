package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"io"
)

type ClusterRestoreSnapshot interface {
	Cluster() (io.Reader, error)
}

type Cluster interface {
	SetClusterID(clusterID uint64)
	SetGroupID(groupID RaftGroupID)
	SetLocalPeerID(localPeerID uint64)
	Peer(peerID uint64) (babuzapb.Peer, error)
	Snapshot(io.Writer) error
	Restore(io.Reader) error
	Peers() []babuzapb.Peer
	ClusterID() uint64
	GroupID() RaftGroupID
	LocalPeerID() uint64
	Add(babuzapb.RaftPeerAttribute) error
	Update(babuzapb.RaftPeerAttribute) error
	Remove(peerID uint64) error
	Promote(peerID uint64) error
	UpdateAppServiceAddresses(peerID uint64, addresses []string) error
}
