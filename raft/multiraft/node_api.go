package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/status"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft/shard"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type Node struct {
	config      NodeConfig
	trans       ibabuza.MultiRaftTransport
	storage     BootstrapStorage
	factory     ComponentsFactory
	multiStatus *status.MultiRaftStatus
	logger      ibabuza.Logger
	scheduler   ibabuza.MultiRaftSchedulerQueue
	replicaSet  struct {
		mu      sync.RWMutex
		replica map[ibabuza.RaftGroupID]*replica
	}
}

func (n *Node) CreateRaftGroup(groupID ibabuza.RaftGroupID, votingPeers *babuza.PeersConfiguration, joinVoting bool) error {
	n.replicaSet.mu.Lock()
	defer n.replicaSet.mu.Unlock()
	if _, ok := n.replicaSet.replica[groupID]; ok {
		return errors.Errorf("raft group %d already exists", groupID)
	}
	r, err := bootstrapNewReplica(n, groupID, votingPeers, joinVoting)
	if err != nil {
		return errors.Errorf("failed to bootstrap new replica(raft group id=%d): %v", groupID, err)
	}
	n.replicaSet.replica[groupID] = r
	return nil
}

func (n *Node) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return babuza.NewErrorResult(errors.Errorf("raft group %d not found", groupID))
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
		return babuza.NewErrorResult(errors.Errorf("raft group %d not found", groupID))
	}
	if n.config.DisableProposalForwarding && n.multiStatus.Get(groupID).IsLeader() == false {
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
