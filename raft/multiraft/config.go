package multiraft

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
	"time"
)

var (
	ErrInvalidNodeID            = errors.New("invalid node id")
	ErrInvalidRaftListenAddress = errors.New("invalid raft listen address")
)

type NodeConfig struct {
	ClusterID         uint64
	NodeID            uint64
	NodeHostDir       string
	RaftListenAddress string
	EnableWalNoSync   bool
	SnapshotCount     uint64
	babuza.RaftConfig
	LearnerReadyPercent          float64
	CoalescedHeartbeatTickMs     int
	CoalescedHeartbeatQueueSize  uint64
	LinearizedReadRequestTimeout time.Duration
	LinearizedReadRetryTimeout   time.Duration
	ibabuza.TLSConfig
	// setup raftScheduler
	SchedulerShardNum       int
	SchedulerShardWorkerNum int
	SchedulerQueueSize      uint64
	SchedulerMaxTicks       int

	// setup job queue
	JobQueueSize int64
}

func DefaultNodeConfig(ClusterID, nodeID uint64, nodeHostDir string, raftListenAddr string) NodeConfig {
	return NodeConfig{
		ClusterID:                    ClusterID,
		NodeID:                       nodeID,
		NodeHostDir:                  nodeHostDir,
		RaftListenAddress:            raftListenAddr,
		EnableWalNoSync:              false,
		SnapshotCount:                10000,
		RaftConfig:                   babuza.DefaultRaftConfig(),
		LearnerReadyPercent:          0.95,
		CoalescedHeartbeatTickMs:     50,
		CoalescedHeartbeatQueueSize:  512,
		LinearizedReadRequestTimeout: time.Second * 3,
		LinearizedReadRetryTimeout:   time.Millisecond * 500,
		TLSConfig:                    ibabuza.TLSConfig{},
		SchedulerShardNum:            2,
		SchedulerShardWorkerNum:      3,
		SchedulerQueueSize:           64,
		SchedulerMaxTicks:            5,
		JobQueueSize:                 128,
	}
}

func (c *NodeConfig) Validate() error {
	if c.NodeID == 0 {
		return ErrInvalidNodeID
	}
	if !netutil.IsValidAddress(c.RaftListenAddress) {
		return ErrInvalidRaftListenAddress
	}
	// TODO: validate other fields
	return nil
}

type ReplicaRaftConfig struct {
	EnableWalNoSync bool
	SnapshotCount   uint64
	babuza.RaftConfig
	LearnerReadyPercent          float64
	CoalescedHeartbeatQueueSize  uint64
	LinearizedReadRequestTimeout time.Duration
	LinearizedReadRetryTimeout   time.Duration
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
