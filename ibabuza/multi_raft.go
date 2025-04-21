package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type RaftGroupID uint64
type NodeID uint64

type RaftGroup struct {
	GroupID RaftGroupID
	PeerID  uint64
}

type MultiRaftSchedulerQueue interface {
	EnqueueBatchTickState(groupIDs []RaftGroupID) error
	EnqueueState(groupID RaftGroupID, state int) error
	Stop()
}

type MultiRaftReplicaApplyJob func()

type MultiRaftReplicaApplyJobQueue interface {
	Put(groupID RaftGroupID, job MultiRaftReplicaApplyJob) error
	Stop()
}

type MultiRaftReplicaStateProcessor interface {
	ProcessTick(groupID RaftGroupID)
	ProcessReady(groupID RaftGroupID)
	ProcessStep(groupID RaftGroupID)
	ProcessProposal(groupID RaftGroupID)
	ProcessConfigChange(groupID RaftGroupID)
	ProcessApplyConfChange(groupID RaftGroupID)
}

type MultiRaftTransport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(RaftNodeHandler) error
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

type MultiRaftNodeHandler interface {
	RaftMessageHandler
	RaftStatusReporter
	MultiSnapshotStorage
}

type MultiRaftWalManager interface {
	FindSnapshot(groupID RaftGroupID) ([]walpb.Snapshot, error)
	CreateWal(groupID RaftGroupID, metadata babuzapb.WalMetadata) (EntryStorage, Wal, error)
	ReplayWal(groupID RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (EntryStorage,
		Wal, ReplayWalResult, error)
	HasExistingWals() (bool, error)
	PurgeWals(WalPurgeConfig)
}
