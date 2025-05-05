package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type RaftGroupID uint64

type MultiRaftTransport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(MultiRaftNodeHandler) error
	Start() error
	Stop() error
	Send(babuzapb.MultiRaftMessage)
	SendSnapshot(babuzapb.MultiRaftMessage)
	CreateTransportClient() (TransportClient, error)
	AddPeer(uint64, string)
	UpdatePeer(uint64, string)
	RemovePeer(uint64)
	RemovePeers()
}

type MultiSnapshotStorage interface {
	CreateSnapshotReader(groupID RaftGroupID, snapshotIndex uint64) (SnapshotReader, error)
}

type MultiRaftStatusReporter interface {
	ReportUnreachable(groupID RaftGroupID, nodeID uint64)
	ReportSnapshot(groupID RaftGroupID, nodeID uint64, status raft.SnapshotStatus)
}
type MultiRaftNodeHandler interface {
	RaftMessageHandler
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
