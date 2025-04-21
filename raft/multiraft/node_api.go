package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft/shard"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type Node struct {
	config        NodeConfig
	trans         ibabuza.MultiRaftTransport
	storage       StorageManager
	factory       ComponentsFactory
	multiStatus   *status.MultiRaftStatus
	logger        ibabuza.Logger
	scheduler     ibabuza.MultiRaftSchedulerQueue
	applyJobQueue ibabuza.MultiRaftReplicaApplyJobQueue
	replicaSet    struct {
		mu      sync.RWMutex
		replica map[ibabuza.RaftGroupID]*replica
	}
}

func (n *Node) CreateRaftGroup(raftGroup ibabuza.RaftGroup, initGroupNodes map[uint64]string, joinVoting bool) error {
	for joinNodeID, raftPeerAttr := range initGroupNodes {
		if joinNodeID != n.config.NodeID {
			n.trans.AddPeer(joinNodeID, raftPeerAttr)
		}
	}
	n.replicaSet.mu.Lock()
	defer n.replicaSet.mu.Unlock()
	if _, ok := n.replicaSet.replica[raftGroup.GroupID]; ok {
		return errors.Errorf("raft group %d already exists", raftGroup.GroupID)
	}
	r := &replica{}
	// BootstrapNewReplica(ReplicaConfig{}, nil, cluster.NewCluster(n.log), n.trans)
	//if err != nil {
	//	return errors.Errorf("failed to bootstrap new replica(raft group id=%d): %v", raftGroup.ID, err)
	//}
	n.replicaSet.replica[raftGroup.GroupID] = r
	return nil
}

func (n *Node) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session raft.ClientSession, log []byte) raft.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return raft.NewErrorResult(errors.Errorf("raft group %d not found", groupID))
	}
	if err := n.scheduler.EnqueueState(groupID, shard.StateProposal); err != nil {
		return raft.NewErrorResult(err)
	}
	return r.EnqueueProposal(ctx, session, log)
}

func (n *Node) AddVotingPeer(ctx context.Context, groupID ibabuza.RaftGroupID, session raft.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) raft.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return raft.NewErrorResult(errors.Errorf("raft group %d not found", groupID))
	}
	if n.config.DisableProposalForwarding && n.multiStatus.Get(groupID).IsLeader() == false {
		return raft.NewErrorResult(raft.ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return raft.NewErrorResult(raft.ErrLearnerCanNotVote)
	}
	if err := n.scheduler.EnqueueState(groupID, shard.StateConfigChange); err != nil {
		return raft.NewErrorResult(err)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, raftPeerAttr, false)
}
