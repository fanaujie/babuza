package peer

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
)

type TransportClientFactory interface {
	CreateTransportClient() (ibabuza.TransportClient, error)
}

type SnapshotFileReader interface {
	ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error
	Metadata() babuzapb.SnapshotMetadata
}

type Peer interface {
	SendRaftMessage(msg *raftpb.Message) error
	SendSnapshot(snapMsg *raftpb.Message, snapReader SnapshotFileReader)
	UpdateRaftReport(report ibabuza.RaftStatusReporter)
	Stop()
}
