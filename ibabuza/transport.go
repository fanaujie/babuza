package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RaftMessageHandler interface {
	ProcessBatchMessage(babuzapb.BatchMessage)
	ProcessSnapshotMessage(babuzapb.SnapshotMessage)
	GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse
}

type RaftStatusReporter interface {
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type SnapshotStorage interface {
	CreateSnapshotReader(snapshotIndex uint64) (SnapshotReader, error)
}

type RaftNodeHandler interface {
	RaftMessageHandler
	RaftStatusReporter
	SnapshotStorage
}

type TransportResolver interface {
	ResolvePeerAddress(peerID uint64) (string, error)
}

type Transport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(RaftNodeHandler) error
	Start() error
	Stop() error
	Send(raftpb.Message)
	SendSnapshot(raftpb.Message)
	CreateTransportClient() (TransportClient, error)
	AddPeer(uint64, string)
	UpdatePeer(uint64, string)
	RemovePeer(uint64)
	RemovePeers()
}

type TransportServer interface {
	Start() error
	Stop() error
}

type TransportClient interface {
	SendBatchMessage(babuzapb.BatchMessage) error
	SendSnapshotMessage(babuzapb.SnapshotMessage) error
	GetClusterPeers(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse
	Close() error
}

type TLSConfig struct {
	EnableTLS bool
	MutualTLS bool
	TLSCert   string
	TLSKey    string
	TLSRootCA string
}

type TransportConfig struct {
	PeerId      uint64
	PeerAddress string
	TLSConfig
}

type TransportProtocol interface {
	Setup(TransportConfig) error
	CreateServer(RaftMessageHandler) (TransportServer, error)
	CreateClient(TransportResolver) (TransportClient, error)
	Close() error
}
