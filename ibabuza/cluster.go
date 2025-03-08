package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"io"
)

type ClusterRestoreSnapshot interface {
	Cluster() (io.Reader, error)
}

type Cluster interface {
	SetClusterId(clusterId uint64)
	SetLocalPeerId(localPeerId uint64)
	Peer(peerId uint64) (babuzapb.Peer, error)
	Snapshot(io.Writer) error
	Restore(io.Reader) error
	Peers() []babuzapb.Peer
	ClusterId() uint64
	LocalPeerID() uint64
	Add(babuzapb.RaftPeerAttribute) error
	Update(babuzapb.RaftPeerAttribute) error
	Remove(peerId uint64) error
	Promote(peerId uint64) error
	UpdateAppServiceAddresses(peerId uint64, addresses []string) error
}
