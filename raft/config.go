package raft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"math"
	"time"
)

type VotingPeersConfiguration struct {
	raftPeersAttr map[uint64]babuzapb.RaftPeerAttribute
}

func NewVotingPeersConfiguration() *VotingPeersConfiguration {
	return &VotingPeersConfiguration{
		raftPeersAttr: make(map[uint64]babuzapb.RaftPeerAttribute),
	}
}

func (c *VotingPeersConfiguration) AddPeer(id uint64, raftListenAddr string) error {
	if _, ok := c.raftPeersAttr[id]; ok {
		return errors.New("")
	}
	c.raftPeersAttr[id] = babuzapb.RaftPeerAttribute{
		Id:             id,
		RaftListenAddr: raftListenAddr,
	}

	return nil
}

func (c *VotingPeersConfiguration) RemovePeer(id uint64) {
	delete(c.raftPeersAttr, id)
}

func (c *VotingPeersConfiguration) RaftPeersAttribute() []babuzapb.RaftPeerAttribute {
	var raftPeersAttr []babuzapb.RaftPeerAttribute
	for _, raftPeerAttr := range c.raftPeersAttr {
		raftPeersAttr = append(raftPeersAttr, raftPeerAttr)
	}
	return raftPeersAttr
}

func (c *VotingPeersConfiguration) PeerIds() []uint64 {
	var raftPeers []uint64
	for _, raftPeerAttr := range c.raftPeersAttr {
		raftPeers = append(raftPeers, raftPeerAttr.Id)
	}
	return raftPeers
}

func (c *VotingPeersConfiguration) ToRaftPeers() ([]raft.Peer, error) {
	var peers []raft.Peer
	for _, raftPeerAttr := range c.raftPeersAttr {
		data, err := raftPeerAttr.Marshal()
		if err != nil {
			return nil, err
		}
		req := babuzapb.ConfChangeRequest{
			RaftPeerAttr: raftPeerAttr,
		}
		data, err = req.Marshal()
		if err != nil {
			return nil, err
		}
		peers = append(peers, raft.Peer{
			ID:      raftPeerAttr.Id,
			Context: data,
		})
	}
	return peers, nil
}

func (c *VotingPeersConfiguration) Clone() *VotingPeersConfiguration {
	conf := NewVotingPeersConfiguration()
	for k, v := range c.raftPeersAttr {
		conf.raftPeersAttr[k] = v
	}
	return conf
}

func (c *VotingPeersConfiguration) Validate() error {
	idSet := make(map[uint64]struct{})
	endpointSet := make(map[string]struct{})
	for _, raftPeerAttr := range c.raftPeersAttr {
		if raftPeerAttr.Id == 0 {
			return fmt.Errorf("VotingPeersConfiguration: empty peer id in votingPeersCfg: %v", *c)
		}
		if raftPeerAttr.RaftListenAddr == "" {
			return fmt.Errorf("VotingPeersConfiguration: empty RaftListenAddr in votingPeersCfg: %v", *c)
		}
		if raftPeerAttr.IsLearner {
			return fmt.Errorf("VotingPeersConfiguration: peer can not be a learner in votingPeersCfg: %v", *c)
		}
		if _, ok := idSet[raftPeerAttr.Id]; ok {
			return fmt.Errorf("VotingPeersConfiguration: found duplicate ID in votingPeersCfg: %v", *c)
		}
		idSet[raftPeerAttr.Id] = struct{}{}
		if _, ok := endpointSet[raftPeerAttr.RaftListenAddr]; ok {
			return fmt.Errorf("VotingPeersConfiguration: found duplicate RaftListenAddr in votingPeersCfg: %v", *c)
		}
		endpointSet[raftPeerAttr.RaftListenAddr] = struct{}{}
	}
	return nil
}

func (c *VotingPeersConfiguration) MatchRemoteCluster(remotePeers []babuzapb.Peer) error {
	//TODO:
	return nil
}

type RaftConfig struct {
	LogicalTickMs             int    `yaml:"logical-interval-ms"`
	ElectionTicks             int    `yaml:"election-timeout"`
	HeartbeatTicks            int    `yaml:"heartbeat-interval"`
	MaxSizePerMsg             uint64 `yaml:"max-bytes-per-message"`
	MaxCommittedSizePerReady  uint64 `yaml:"max-committed-bytes-per-ready"`
	MaxUncommittedEntriesSize uint64 `yaml:"max-uncommitted-entries-bytes"`
	MaxInflightMsgs           int    `yaml:"max-inflight-messages"`
	CheckQuorum               bool   `yaml:"check-quorum"`
	PreVote                   bool   `yaml:"pre-vote"`
	DisableProposalForwarding bool   `yaml:"disable-proposal-forwarding"`
}

type BabuzaConfig struct {
	ClusterId         uint64
	LocalPeerId       uint64
	RaftListenAddress string
	Join              bool
	EnableWalNoSync   bool
	SnapshotCount     uint64
	RaftConfig
	ibabuza.TLSConfig
	LearnerReadyPercent          float64
	LinearizedReadRequestTimeout time.Duration
	LinearizedReadRetryTimeout   time.Duration
}

func DefaultBabuzaConfig(ClusterId, localPeerId uint64, raftListenAddr string) BabuzaConfig {
	return BabuzaConfig{
		ClusterId:                    ClusterId,
		LocalPeerId:                  localPeerId,
		RaftListenAddress:            raftListenAddr,
		SnapshotCount:                10000,
		RaftConfig:                   DefaultRaftConfig(),
		TLSConfig:                    ibabuza.TLSConfig{},
		LearnerReadyPercent:          0.95,
		LinearizedReadRequestTimeout: time.Second * 3,
		LinearizedReadRetryTimeout:   time.Millisecond * 500,
	}
}

func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		LogicalTickMs:             100,
		ElectionTicks:             10,
		HeartbeatTicks:            1,
		MaxSizePerMsg:             1 * 1024 * 1024,
		MaxCommittedSizePerReady:  math.MaxUint64,
		MaxUncommittedEntriesSize: 1 << 30,
		MaxInflightMsgs:           512,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,
	}
}

func (c *BabuzaConfig) convertToRaftConfig(logger raft.Logger, ms raft.Storage) raft.Config {
	return raft.Config{
		ID:                        c.LocalPeerId,
		ElectionTick:              c.ElectionTicks,
		HeartbeatTick:             c.HeartbeatTicks,
		Storage:                   ms,
		MaxSizePerMsg:             c.MaxSizePerMsg,
		MaxCommittedSizePerReady:  c.MaxCommittedSizePerReady,
		MaxUncommittedEntriesSize: c.MaxUncommittedEntriesSize,
		MaxInflightMsgs:           c.MaxInflightMsgs,
		CheckQuorum:               c.CheckQuorum,
		PreVote:                   c.PreVote,
		ReadOnlyOption:            raft.ReadOnlySafe,
		Logger:                    logger,
		DisableProposalForwarding: c.DisableProposalForwarding,
	}
}

func (c *BabuzaConfig) validateConfig() (bool, error) {
	//TODO: if has scheme? ex. https://localshot:1421
	//for k, v := range c.ClusterAdvertiseAddress {
	//	if netUtil.ValidateTcpAddr(v, false) == false {
	//		return false, fmt.Errorf("failed to validate ClusterAdvertiseAddress(node=%v addr=%s)", k, v)
	//	}
	//}
	//if len(c.ClusterAdvertiseAddress) > 0 {
	//	if duplicateClusterPeerEndpoint(c.ClusterAdvertiseAddress) {
	//		return false, fmt.Errorf("failed to validate ClusterAdvertiseAddress: foud duplicate addr %v ", c.ClusterAdvertiseAddress)
	//	}
	//}
	//if netUtil.ValidateTcpAddr(c.ServerEndpoint, true) == false {
	//	return false, fmt.Errorf("failed to validate ServerEndpoint(%s)", c.ServerEndpoint)
	//}
	//
	//advertiseAddr := c.ClusterAdvertiseAddress[c.LocalPeerId]
	//advertiseTcpAddr, err := netUtil.ResolveTcpAddr(advertiseAddr)
	//if err != nil {
	//	return false, fmt.Errorf("failed to resolve ClusterAdvertiseAddress(local member id=%d addr=%s): err=(%s)",
	//		c.LocalPeerId, advertiseAddr, err)
	//}
	//if advertiseTcpAddr != c.PeerListenAddress {
	//	return false, fmt.Errorf("failed to match PeerListenAddr(%s) and ClusterAdvertiseAddress(local member id=%d addr=%s)",
	//		c.PeerListenAddress, c.LocalPeerId, advertiseAddr)
	//}
	//
	//existWal := fileUtil.Exist(c.WalDir)
	//if existWal {
	//	if err = fileUtil.IsDirWriteable(c.WalDir); err != nil {
	//		return false, err
	//	}
	//}

	return false, nil
}

func duplicateClusterPeerEndpoint(peers map[uint64]string) bool {
	m := make(map[string]struct{})
	for _, v := range peers {
		if _, ok := m[v]; ok {
			return true
		}
		m[v] = struct{}{}
	}
	return false
}
