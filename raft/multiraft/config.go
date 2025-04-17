package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft"
)

type NodeConfig struct {
	ClusterId         uint64
	NodeId            uint64
	RaftListenAddress string
	EnableWalNoSync   bool
	SnapshotCount     uint64
	raft.RaftConfig
	LearnerReadyPercent float64
	ibabuza.TLSConfig

	// setup scheduler
	SchedulerWorkerNum int
	SchedulerQueueSize uint64
	SchedulerMaxTicks  int
}

type ReplicaRaftConfig struct {
	EnableWalNoSync bool
	SnapshotCount   uint64
	raft.RaftConfig
	LearnerReadyPercent float64
}
