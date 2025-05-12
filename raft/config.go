package raft

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"math"
	"time"
)

type PeersConfiguration struct {
	raftPeersAttr map[uint64]babuzapb.RaftPeerAttribute
}

func NewPeersConfiguration() *PeersConfiguration {
	return &PeersConfiguration{
		raftPeersAttr: make(map[uint64]babuzapb.RaftPeerAttribute),
	}
}

func (c *PeersConfiguration) AddPeer(id uint64, raftListenAddr string, isLearner bool) error {
	if _, ok := c.raftPeersAttr[id]; ok {
		return fmt.Errorf("peer already exists: %d", id)
	}
	c.raftPeersAttr[id] = babuzapb.RaftPeerAttribute{
		Id:             id,
		RaftListenAddr: raftListenAddr,
		IsLearner:      isLearner,
	}
	return nil
}

func (c *PeersConfiguration) GetPeer(id uint64) (babuzapb.RaftPeerAttribute, error) {
	raftPeerAttr, ok := c.raftPeersAttr[id]
	if !ok {
		return babuzapb.RaftPeerAttribute{}, fmt.Errorf("peer not found: %d", id)
	}
	return raftPeerAttr, nil
}

func (c *PeersConfiguration) RemovePeer(id uint64) {
	delete(c.raftPeersAttr, id)
}

func (c *PeersConfiguration) RaftPeersAttribute() []babuzapb.RaftPeerAttribute {
	var raftPeersAttr []babuzapb.RaftPeerAttribute
	for _, raftPeerAttr := range c.raftPeersAttr {
		raftPeersAttr = append(raftPeersAttr, raftPeerAttr)
	}
	return raftPeersAttr
}

func (c *PeersConfiguration) PeerIds() []uint64 {
	var raftPeers []uint64
	for _, raftPeerAttr := range c.raftPeersAttr {
		raftPeers = append(raftPeers, raftPeerAttr.Id)
	}
	return raftPeers
}

func (c *PeersConfiguration) ToRaftPeers() ([]raft.Peer, error) {
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

func (c *PeersConfiguration) Clone() *PeersConfiguration {
	conf := NewPeersConfiguration()
	for k, v := range c.raftPeersAttr {
		conf.raftPeersAttr[k] = v
	}
	return conf
}

func (c *PeersConfiguration) Validate() error {
	idSet := make(map[uint64]struct{})
	endpointSet := make(map[string]struct{})
	for _, raftPeerAttr := range c.raftPeersAttr {
		if raftPeerAttr.Id == 0 {
			return fmt.Errorf("PeersConfiguration: empty peer id in config: %v", *c)
		}
		if raftPeerAttr.RaftListenAddr == "" {
			return fmt.Errorf("PeersConfiguration: empty RaftListenAddr in config: %v", *c)
		}
		if _, ok := idSet[raftPeerAttr.Id]; ok {
			return fmt.Errorf("PeersConfiguration: found duplicate ID in config: %v", *c)
		}
		idSet[raftPeerAttr.Id] = struct{}{}
		if _, ok := endpointSet[raftPeerAttr.RaftListenAddr]; ok {
			return fmt.Errorf("PeersConfiguration: found duplicate RaftListenAddr in config: %v", *c)
		}
		endpointSet[raftPeerAttr.RaftListenAddr] = struct{}{}
	}
	return nil
}

func (c *PeersConfiguration) Visit(visitor func(babuzapb.RaftPeerAttribute) error) error {
	for _, raftPeerAttr := range c.raftPeersAttr {
		if err := visitor(raftPeerAttr); err != nil {
			return err
		}
	}
	return nil
}

func (c *PeersConfiguration) MatchRemoteCluster(remoteCtx context.Context, clusterID, fromID uint64,
	groupID ibabuza.RaftGroupID, client ibabuza.TransportClient) error {
	req := babuzapb.GetClusterPeersRequest{
		ClusterID: clusterID,
		GroupID:   uint64(groupID),
		From:      fromID,
	}
	for _, raftPeerAttr := range c.RaftPeersAttribute() {
		if raftPeerAttr.Id == fromID {
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
		}(raftPeerAttr.Id)
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
	for _, peer := range other {
		if _, ok := c.raftPeersAttr[peer.RaftPeerAttr.Id]; !ok {
			return false
		}
	}
	return true
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
	ClusterID         uint64
	LocalPeerID       uint64
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

func DefaultBabuzaConfig(ClusterId, localPeerID uint64, raftListenAddr string) BabuzaConfig {
	return BabuzaConfig{
		ClusterID:                    ClusterId,
		LocalPeerID:                  localPeerID,
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
		ID:                        c.LocalPeerID,
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
