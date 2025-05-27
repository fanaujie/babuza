package multiraft

import (
	"context"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
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

func (r *ReplicaRaftConfig) convertToRaftConfig(localPeerID uint64, logger raft.Logger, ms raft.Storage) *raft.Config {
	return &raft.Config{
		ID:                        localPeerID,
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

type PeersConfiguration struct {
	groupID ibabuza.RaftGroupID
	config  *babuza.PeersConfiguration
}

func NewPeersConfiguration() *PeersConfiguration {
	return &PeersConfiguration{
		config: babuza.NewPeersConfiguration(),
	}
}

func (c *PeersConfiguration) GroupID() ibabuza.RaftGroupID {
	return c.groupID
}

func (c *PeersConfiguration) SetGroupID(groupID ibabuza.RaftGroupID) {
	c.groupID = groupID
}

func (c *PeersConfiguration) AddPeer(id uint64, raftListenAddr string, isLearner bool) error {
	return c.config.AddPeer(id, raftListenAddr, isLearner)
}

func (c *PeersConfiguration) GetPeer(id uint64) (babuzapb.RaftPeerAttribute, error) {
	return c.config.GetPeer(id)
}

func (c *PeersConfiguration) RemovePeer(id uint64) {
	c.config.RemovePeer(id)
}

func (c *PeersConfiguration) RaftPeersAttribute() []babuzapb.RaftPeerAttribute {
	return c.config.RaftPeersAttribute()
}

func (c *PeersConfiguration) PeerIds() []uint64 {
	return c.config.PeerIds()
}

func (c *PeersConfiguration) ToRaftPeers() ([]raft.Peer, error) {
	peersMap := c.config.RaftPeerAttributeMap()
	var peers []raft.Peer
	if len(peersMap) == 0 {
		return nil, fmt.Errorf("no peers found in group %d", c.groupID)
	}
	if c.groupID == 0 {
		return nil, fmt.Errorf("group ID is not set")
	}
	for _, peer := range peersMap {
		req := babuzapb.ConfChangeRequest{
			GroupID:      uint64(c.groupID),
			RaftPeerAttr: peer,
		}
		data, err := req.Marshal()
		if err != nil {
			return nil, err
		}
		peers = append(peers, raft.Peer{
			ID:      peer.PeerID,
			Context: data,
		})
	}
	return peers, nil
}

func (c *PeersConfiguration) Clone() *PeersConfiguration {
	clone := NewPeersConfiguration()
	for _, raftPeerAttr := range c.RaftPeersAttribute() {
		if err := clone.AddPeer(raftPeerAttr.PeerID, raftPeerAttr.RaftListenAddr, raftPeerAttr.IsLearner); err != nil {
			return nil
		}
	}
	return clone
}

func (c *PeersConfiguration) Validate() error {
	if err := c.config.Validate(); err != nil {
		return err
	}
	if c.groupID == 0 {
		return fmt.Errorf("group ID is not set")
	}
	return nil
}

func (c *PeersConfiguration) Visit(visitor func(babuzapb.RaftPeerAttribute) error) error {
	for _, raftPeerAttr := range c.RaftPeersAttribute() {
		if err := visitor(raftPeerAttr); err != nil {
			return err
		}
	}
	return nil
}

func (c *PeersConfiguration) MatchRemoteCluster(remoteCtx context.Context, clusterID, fromID uint64,
	groupID ibabuza.RaftGroupID, client ibabuza.MultiRaftTransportClient) error {
	req := babuzapb.GetClusterPeersRequest{
		ClusterID: clusterID,
		GroupID:   uint64(groupID),
		From:      fromID,
	}
	for _, raftPeerAttr := range c.RaftPeersAttribute() {
		if raftPeerAttr.PeerID == fromID {
			continue
		}
		select {
		case <-remoteCtx.Done():
			return remoteCtx.Err()
		default:
		}
		res, err := func(to uint64) (babuzapb.GetClusterPeersResponse, error) {
			req.To = to
			return client.GetClusterPeers(req)
		}(raftPeerAttr.PeerID)
		if err != nil || res.Status != babuzapb.SUCCESS {
			continue
		}
		if !c.equal(res.Peers) {
			continue
		}
		return nil
	}
	return fmt.Errorf("bootstrap: could not get remote cluster from %v", c.RaftPeersAttribute())
}
func (c *PeersConfiguration) equal(other []babuzapb.Peer) bool {
	if len(c.RaftPeersAttribute()) != len(other) {
		return false
	}
	peersMap := c.config.RaftPeerAttributeMap()
	for _, peer := range other {
		if peerAttr, ok := peersMap[peer.RaftPeerAttr.PeerID]; !ok || peerAttr.RaftListenAddr != peer.RaftPeerAttr.RaftListenAddr ||
			peerAttr.IsLearner != peer.RaftPeerAttr.IsLearner {
			return false
		}
	}
	return true
}
