package peer

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
)

type Dialer interface {
	Dial(ctx context.Context, peerId uint64) (ibabuza.TransportClient, error)
}

type SnapshotFileReader interface {
	ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error
	Metadata() babuzapb.SnapshotMetadata
}

type Peer interface {
	SendRaftMessage(msg *raftpb.Message) error
	SendSnapshot(msg *raftpb.Message, snapReader SnapshotFileReader)
	UpdateRaftReport(report ibabuza.RaftStatusReporter)
	Stop()
	Run()
}
