package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type RaftGroupID uint64

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
	SetupTransportRaft(MultiRaftNodeHandler) error
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
	CreateServer(MultiRaftNodeHandler) (TransportServer, error)
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

type MultiRaftNodeHandler interface {
	ProcessMultiRaftMessage(babuzapb.MultiRaftBatchMessage)
	ProcessSnapshotMessage(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse
	GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	MultiRaftStatusReporter
	MultiSnapshotStorage
}

type MultiRaftWalManager interface {
	FindSnapshot(groupID RaftGroupID) ([]walpb.Snapshot, error)
	CreateWal(groupID RaftGroupID, metadata babuzapb.WalMetadata) (EntryStorage, Wal, error)
	ReplayWal(groupID RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (EntryStorage,
		Wal, ReplayWalResult, error)
	HasExistingWals() ([]RaftGroupID, error)
	PurgeWals(WalPurgeConfig)
	Close() error
}

type MultiRaftSnapshotManager interface {
	ScanInstalledSnapshots(groupIDs []RaftGroupID, removeUnfinishedSnapshotDir bool) error
	LoadLastValidSnapshot(groupID RaftGroupID, walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	CreateAtomicSnapshotWriter(groupID RaftGroupID, snapshotTerm, snapshotIndex uint64) (AtomicSnapshotWriter, error)
	CreateInstalledSnapshotReader(groupID RaftGroupID, snapshotIndex uint64, validateFsmFiles bool) (SnapshotReader, error)
	CreateAtomicSnapshotReceiver(groupID RaftGroupID, metadata babuzapb.SnapshotMetadata) (AtomicSnapshotReceiver, error)
	Purge(groupID RaftGroupID, snapshot raftpb.Snapshot) error
	GetGroupSnapshot(groupID RaftGroupID) SnapshotManager
	Close() error
}
