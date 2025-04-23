package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft/shard"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type Node struct {
	config     NodeConfig
	trans      ibabuza.MultiRaftTransport
	storage    BootstrapStorage
	factory    ComponentsFactory
	logger     ibabuza.Logger
	scheduler  ibabuza.MultiRaftSchedulerQueue
	closer     *syncutil.Closer
	replicaSet struct {
		mu      sync.RWMutex
		replica map[ibabuza.RaftGroupID]*replica
	}
}

func (n *Node) Start() error {
	if err := n.trans.SetupTransportRaft(&transportProcessor{
		Node: n,
	}); err != nil {
		return err
	}
	if err := n.trans.Start(); err != nil {
		return errors.Errorf("Node[%d] transport start error: %v", n.config.NodeID, err)
	}
	if err := n.scheduler.Start(); err != nil {
		return errors.Errorf("Node[%d] scheduler start error: %v", n.config.NodeID, err)
	}
	n.closer.Run(func() {
		n.raftTickStart()
	})
	return nil
}

func (n *Node) Stop() {
	n.trans.Stop()
	n.closer.Close()
	n.scheduler.Stop()
	n.replicaSet.mu.Lock()
	defer n.replicaSet.mu.Unlock()
	for _, r := range n.replicaSet.replica {
		r.Stop()
	}
	n.replicaSet.replica = make(map[ibabuza.RaftGroupID]*replica)
}

func (n *Node) CreateRaftGroup(groupID ibabuza.RaftGroupID, peersConfig *babuza.PeersConfiguration, join bool) error {
	n.replicaSet.mu.Lock()
	defer n.replicaSet.mu.Unlock()
	if _, ok := n.replicaSet.replica[groupID]; ok {
		return errors.Errorf("Node[%d] raft group %d already exists", n.config.NodeID, groupID)
	}
	r, err := bootstrapReplicaWithConfiguration(n, groupID, peersConfig, join)
	if err != nil {
		return errors.Errorf("Node[%d] failed to bootstrap new replica(raft group id=%d): %v", n.config.NodeID, groupID, err)
	}
	n.replicaSet.replica[groupID] = r
	return nil
}

func (n *Node) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return babuza.NewErrorResult(errors.Errorf("Node[%d] raft group %d not found", n.config.NodeID, groupID))
	}
	if err := n.scheduler.EnqueueState(groupID, shard.StateProposal); err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.EnqueueProposal(ctx, session, log)
}

func (n *Node) AddVotingPeer(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) babuza.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return babuza.NewErrorResult(errors.Errorf("Node[%d]  raft group %d not found", n.config.NodeID, groupID))
	}
	if n.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrLearnerCanNotVote)
	}
	if err := n.scheduler.EnqueueState(groupID, shard.StateConfigChange); err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, raftPeerAttr, false)
}
