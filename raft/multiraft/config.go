package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
)

type NodeConfig struct {
	ClusterID         uint64
	NodeID            uint64
	NodeHostDir       string
	RaftListenAddress string
	EnableWalNoSync   bool
	SnapshotCount     uint64
	babuza.RaftConfig
	LearnerReadyPercent float64
	ibabuza.TLSConfig

	// setup scheduler
	SchedulerWorkerNum int
	SchedulerQueueSize uint64
	SchedulerMaxTicks  int

	// setup apply job queue
	ApplyJobQueueSize int64
}

type ReplicaRaftConfig struct {
	EnableWalNoSync bool
	SnapshotCount   uint64
	babuza.RaftConfig
	LearnerReadyPercent float64
}

func (r *ReplicaRaftConfig) convertToRaftConfig(nodeID uint64, logger raft.Logger, ms raft.Storage) *raft.Config {
	return &raft.Config{
		ID:                        nodeID,
		ElectionTick:              r.ElectionTicks,
		HeartbeatTick:             r.HeartbeatTicks,
		Storage:                   ms,
		MaxSizePerMsg:             r.MaxSizePerMsg,
		MaxCommittedSizePerReady:  r.MaxCommittedSizePerReady,
		MaxUncommittedEntriesSize: r.MaxUncommittedEntriesSize,
		MaxInflightMsgs:           r.MaxInflightMsgs,
		CheckQuorum:               r.CheckQuorum,
		PreVote:                   r.PreVote,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    logger,
		DisableProposalForwarding: r.DisableProposalForwarding,
	}
}
