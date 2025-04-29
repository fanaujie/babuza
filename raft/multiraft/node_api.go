package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sort"
	"sync"
)

type Node struct {
	config    NodeConfig
	trans     ibabuza.MultiRaftTransport
	storage   BootstrapStorage
	factory   ComponentsFactory
	logger    ibabuza.Logger
	scheduler Scheduler

	closer     *syncutil.Closer
	replicaSet struct {
		mu      sync.RWMutex
		replica map[ibabuza.RaftGroupID]*replica
	}
}

func (n *Node) Start() error {
	var err error
	if err = n.trans.SetupTransportRaft(&transportProcessor{
		Node: n,
	}); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = n.trans.Stop()
			n.scheduler.Stop()
			for _, r := range n.replicaSet.replica {
				r.Stop()
			}
		}
	}()
	if err = n.trans.Start(); err != nil {
		err = errors.Errorf("Node[%d] transport start error: %v", n.config.NodeID, err)
		return err
	}
	if err = n.scheduler.Start(); err != nil {
		err = errors.Errorf("Node[%d] raftScheduler start error: %v", n.config.NodeID, err)
		return err
	}
	for _, r := range n.replicaSet.replica {
		if err = r.Start(); err != nil {
			err = errors.Errorf("Node[%d] replica start error: %v", n.config.NodeID, err)
			return err
		}
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
	return r.Start()
}

func (n *Node) GetGroupIDs() []ibabuza.RaftGroupID {
	n.replicaSet.mu.RLock()
	defer n.replicaSet.mu.RUnlock()
	groupIDs := make([]ibabuza.RaftGroupID, 0, len(n.replicaSet.replica))
	for groupID := range n.replicaSet.replica {
		groupIDs = append(groupIDs, groupID)
	}
	if len(groupIDs) > 1 {
		sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	}
	return groupIDs
}

func (n *Node) HasGroupID(groupID ibabuza.RaftGroupID) bool {
	n.replicaSet.mu.RLock()
	defer n.replicaSet.mu.RUnlock()
	_, ok := n.replicaSet.replica[groupID]
	return ok
}

func (n *Node) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return babuza.NewErrorResult(errors.Errorf("Node[%d] raft group %d not found", n.config.NodeID, groupID))
	}
	if err := n.scheduler.EnqueueState(stateProposal, groupID); err != nil {
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
	if err := n.scheduler.EnqueueState(stateConfigChange, groupID); err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, raftPeerAttr, false)
}

func (n *Node) Status(groupID ibabuza.RaftGroupID) (babuza.Status, error) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if !ok {
		return babuza.Status{}, errors.Errorf("Node[%d] groupID[%d] not found", n.config.NodeID, groupID)
	}

	lastIndex, lastTerm, snapshot, err := r.storage.EntryStorageInfo()
	if err != nil {
		r.logger.Panic(err)
	}
	softStatus := r.status.CloneSoftState()
	raftState := babuza.RaftState(softStatus.RaftState)
	leaderID := softStatus.Lead
	select {
	case <-r.closer.CloseCh():
		raftState = babuza.StopState
		leaderID = babuza.None
	default:
	}

	return babuza.Status{
		State:              raftState,
		ClusterID:          r.cluster.ClusterID(),
		LocalPeerID:        r.cluster.LocalPeerID(),
		LeaderID:           leaderID,
		RaftTerm:           r.status.GetHardStateTerm(),
		RaftCommittedIndex: r.status.GetCommittedIndex(),
		RaftAppliedIndex:   r.status.GetAppliedIndex(),
		LastLogTerm:        lastTerm,
		LastLogIndex:       lastIndex,
		LastSnapshotTerm:   snapshot.Metadata.Term,
		LastSnapshotIndex:  snapshot.Metadata.Index,
	}, nil
}
