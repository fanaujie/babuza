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
	ErrInvalidStoreID           = errors.New("invalid store id")
	ErrInvalidRaftListenAddress = errors.New("invalid raft listen address")
)

type StoreConfig struct {
	ClusterID         uint64
	StoreID           uint64
	StoreHostDir      string
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
	// setup leader transfer checker
	LeaderTransferCheckerShardNum int
}

func DefaultStoreConfig(ClusterID, storeID uint64, storeHostDir string, raftListenAddr string) StoreConfig {
	return StoreConfig{
		ClusterID:                     ClusterID,
		StoreID:                       storeID,
		StoreHostDir:                  storeHostDir,
		RaftListenAddress:             raftListenAddr,
		EnableWalNoSync:               false,
		SnapshotCount:                 10000,
		RaftConfig:                    babuza.DefaultRaftConfig(),
		LearnerReadyPercent:           0.95,
		CoalescedHeartbeatTickMs:      50,
		CoalescedHeartbeatQueueSize:   512,
		LinearizedReadRequestTimeout:  time.Second * 3,
		LinearizedReadRetryTimeout:    time.Millisecond * 500,
		TLSConfig:                     ibabuza.TLSConfig{},
		SchedulerShardNum:             2,
		SchedulerShardWorkerNum:       3,
		SchedulerQueueSize:            64,
		SchedulerMaxTicks:             5,
		JobQueueSize:                  128,
		LeaderTransferCheckerShardNum: 4,
	}
}

func (c *StoreConfig) Validate() error {
	if c.StoreID == 0 {
		return ErrInvalidStoreID
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
	groupID     ibabuza.RaftGroupID
	peersConfig *babuza.PeersConfiguration
}

func NewPeersConfiguration() *PeersConfiguration {
	return &PeersConfiguration{
		peersConfig: babuza.NewPeersConfiguration(),
	}
}

func (c *PeersConfiguration) GroupID() ibabuza.RaftGroupID {
	return c.groupID
}

func (c *PeersConfiguration) SetGroupID(groupID ibabuza.RaftGroupID) {
	c.groupID = groupID
}

func (c *PeersConfiguration) AddPeer(peerID, storeID uint64, raftListenAddr string, isLearner bool) error {
	if err := c.peersConfig.AddPeer(peerID, raftListenAddr, isLearner); err != nil {
		return err
	}
	peerAttr, _ := c.peersConfig.GetPeer(peerID)
	peerAttr.StoreID = storeID
	return c.peersConfig.UpdatePeer(peerID, peerAttr)
}

func (c *PeersConfiguration) GetPeer(id uint64) (babuzapb.RaftPeerAttribute, error) {
	return c.peersConfig.GetPeer(id)
}

func (c *PeersConfiguration) RemovePeer(id uint64) {
	c.peersConfig.RemovePeer(id)
}

func (c *PeersConfiguration) RaftPeersAttribute() []babuzapb.RaftPeerAttribute {
	return c.peersConfig.RaftPeersAttribute()
}

func (c *PeersConfiguration) PeerIds() []uint64 {
	return c.peersConfig.PeerIds()
}

func (c *PeersConfiguration) ToRaftPeers() ([]raft.Peer, error) {
	peersMap := c.peersConfig.RaftPeerAttributeMap()
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
		if err := clone.AddPeer(raftPeerAttr.PeerID, raftPeerAttr.StoreID,
			raftPeerAttr.RaftListenAddr, raftPeerAttr.IsLearner); err != nil {
			return nil
		}
	}
	return clone
}

func (c *PeersConfiguration) Validate() error {
	if err := c.peersConfig.Validate(); err != nil {
		return err
	}
	if c.groupID == 0 {
		return fmt.Errorf("group ID is not set")
	}
	// check store IDs
	for _, raftPeerAttr := range c.RaftPeersAttribute() {
		if raftPeerAttr.StoreID == 0 {
			return fmt.Errorf("store ID is not set for peer %d", raftPeerAttr.PeerID)
		}
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
	peersMap := c.peersConfig.RaftPeerAttributeMap()
	for _, peer := range other {
		if peerAttr, ok := peersMap[peer.RaftPeerAttr.PeerID]; !ok || peerAttr.RaftListenAddr != peer.RaftPeerAttr.RaftListenAddr ||
			peerAttr.IsLearner != peer.RaftPeerAttr.IsLearner {
			return false
		}
	}
	return true
}
