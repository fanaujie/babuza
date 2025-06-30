package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type RaftGroupID uint64

type MultiRaftListener interface {
	OnAcquiredLeader(groupID RaftGroupID, term, leaderID uint64)
	OnLostLeader(groupID RaftGroupID, term, leaderID uint64)
	OnLeaderChange(groupID RaftGroupID, term, leaderID uint64)
	OnMemberChange(memberEvent int, groupID RaftGroupID, term, peerID uint64)
}

type MultiRaftTransportResolver interface {
	ResolvePeerAddress(groupID RaftGroupID, peerID uint64) (string, error)
}

type MultiRaftTransportClient interface {
	SendMultiRaftMessage(babuzapb.MultiRaftBatchMessage) error
	SendSnapshotMessage(babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error)
	GetClusterPeers(babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error)
	Close() error
}

type MultiRaftTransport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(MultiRaftStoreHandler) error
	Start() error
	Stop() error
	Send(RaftGroupID, raftpb.Message)
	SendSnapshot(RaftGroupID, raftpb.Message)
	SendHeartbeat(toAddress string, heartbeats []babuzapb.MultiRaftHeartbeatMessage, heartbeatResponse []babuzapb.MultiRaftHeartbeatMessage)
	CreateTransportClient() (MultiRaftTransportClient, error)
	AddPeer(RaftGroupID, uint64, string)
	UpdatePeer(RaftGroupID, uint64, string)
	RemovePeer(RaftGroupID, uint64)
	RemovePeers()
	ResolvePeerAddress(groupID RaftGroupID, peerID uint64) (string, error)
}

type MultiRaftTransportProtocol interface {
	Setup(TransportConfig) error
	CreateServer(MultiRaftStoreHandler) (TransportServer, error)
	CreateClient(MultiRaftTransportResolver) (MultiRaftTransportClient, error)
	Close() error
}

type MultiSnapshotStorage interface {
	CreateSnapshotReader(groupID RaftGroupID, snapshotIndex uint64) (SnapshotReader, error)
}

type MultiRaftStatusReporter interface {
	ReportUnreachable(groupID RaftGroupID, peerID uint64)
	ReportSnapshot(groupID RaftGroupID, peerID uint64, status raft.SnapshotStatus)
}

type MultiRaftStoreHandler interface {
	ProcessMultiRaftMessage(babuzapb.MultiRaftBatchMessage)
	ProcessSnapshotMessage(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse
	GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	MultiRaftStatusReporter
	MultiSnapshotStorage
}

type MultiRaftWalManager interface {
	FindSnapshot(groupID RaftGroupID) ([]walpb.Snapshot, error)
	CreateWal(groupID RaftGroupID, metadata babuzapb.WalMetadata) (EntryStorage, Wal, error)
	ReplayWal(groupID RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (ReplayWalResult, EntryStorage, Wal, error)
	HasExistingWals() ([]RaftGroupID, error)
	RemoveData(groupID RaftGroupID) error
	Purger() WalPurger
	Close() error
}

type MultiRaftSnapshotManager interface {
	ScanInstalledSnapshots(groupIDs []RaftGroupID, removeUnfinishedSnapshotDir bool) (map[RaftGroupID]SnapshotManager, error)
	RemoveData(groupID RaftGroupID) error
	Purger() SnapshotPurger
	CreateSnapshotManager(groupID RaftGroupID) SnapshotManager
	Close() error
}
