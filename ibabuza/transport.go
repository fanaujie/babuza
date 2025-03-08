package ibabuza

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RaftMessageHandler interface {
	ProcessBatchMessage(babuzapb.BatchMessage)
	ProcessSnapshotMessage(babuzapb.SnapshotMessage)
	GetClusterPeersRequest(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	PublishApplicationServiceRequest(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse
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

type Transport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(RaftNodeHandler) error
	Start() error
	Stop() error
	Send(raftpb.Message)
	SendSnapshot(raftpb.Message)
	DialPeer(context.Context, uint64) (TransportClient, error)
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
	GetClusterPeers(babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error)
	PublishApplicationService(babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error)
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
	Dial(context.Context, string) (TransportClient, error)
}
