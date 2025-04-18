package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

type RaftGroupID uint64
type NodeID uint64

type RaftGroup struct {
	ID         RaftGroupID
	RaftPeerID uint64
	Metadata   []byte
}

type MultiRaftScheduler interface {
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
}

type MultiRaftStatus interface {
	Get(groupID RaftGroupID) Status
	Set(groupID RaftGroupID)
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
