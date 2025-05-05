package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sort"
	"sync"
	"time"
)

type Node struct {
	config         NodeConfig
	trans          ibabuza.MultiRaftTransport
	storage        BootstrapStorage
	factory        ComponentsFactory
	logger         ibabuza.Logger
	scheduler      Scheduler
	replicaEventCh chan replicaEvent
	closer         *syncutil.Closer
	replicaSet     struct {
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
		n.replicaRaftTick()
	})
	n.closer.Run(func() {
		n.replicaListener()
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
	n.storage.Close()
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

func (n *Node) StateMachine(groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error) {
	r, err := n.getReplica(groupID)
	if err != nil {
		return nil, err
	}
	return r.storage.GetStateMachine(), nil
}

func (n *Node) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.EnqueueProposal(ctx, session, log)
}

func (n *Node) AddVotingPeer(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) babuza.ProposedResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if n.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrLearnerCanNotVote)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, raftPeerAttr, false)
}

func (n *Node) RemovePeer(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	peerID uint64) babuza.ProposedResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if n.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeRemoveNode, babuzapb.RaftPeerAttribute{Id: peerID}, false)
}

func (n *Node) AddLearner(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) babuza.ProposedResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if !raftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrNotLearner)
	}
	if n.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddLearnerNode, raftPeerAttr, false)
}

func (n *Node) PromoteLearner(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	peerID uint64) babuza.ProposedResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	result, err := func() (babuza.ProposedResult, error) {
		p, err := r.cluster.Peer(peerID)
		if err != nil {
			return nil, err
		}
		if p.RaftPeerAttr.IsLearner == false {
			return nil, babuza.ErrVotingMemberCanNotPromote
		}
		// only leader can check if the learner is ready
		if err = r.learnerReady(ctx, peerID); err != nil {
			return nil, err
		}
		return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, p.RaftPeerAttr, true), nil
	}()
	if err != nil {
		if !errors.Is(err, babuza.ErrNotLeader) {
			return babuza.NewErrorResult(err)
		}
		// forward request to leader
	} else {
		return result
	}
	//TODO: forward request to leader when current node is not the leader
	return babuza.NewErrorResult(babuza.ErrNotLeader)
}

func (n *Node) TransferLeader(ctx context.Context, groupID ibabuza.RaftGroupID, transferee uint64) babuza.TransferLeaderResult {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	toPeer, err := r.cluster.Peer(transferee)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if toPeer.RaftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrLearnerCanNotSwitchLeadership)
	}
	leaderID := r.status.CloneSoftState().Lead
	if leaderID != raft.None {
		if err = r.EnqueueTransferLeader(transferee); err != nil {
			return babuza.NewErrorResult(err)
		}
		return babuza.NewTransferLeaderResult(ctx, transferee, r.closer, time.Second,
			func() uint64 {
				return r.status.CloneSoftState().Lead
			})
	}
	return babuza.NewErrorResult(babuza.ErrNoLeader)
}

func (n *Node) Configuration(groupID ibabuza.RaftGroupID) (babuza.ClusterConfiguration, error) {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.ClusterConfiguration{}, err
	}
	return r.ClusterConfiguration(), nil
}

func (n *Node) Status(groupID ibabuza.RaftGroupID) (babuza.Status, error) {
	r, err := n.getReplica(groupID)
	if err != nil {
		return babuza.Status{}, err
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

func (n *Node) getReplica(groupID ibabuza.RaftGroupID) (*replica, error) {
	n.replicaSet.mu.RLock()
	defer n.replicaSet.mu.RUnlock()
	r, ok := n.replicaSet.replica[groupID]
	if !ok {
		return nil, errors.Errorf("Node[%d] raft group %d not found", n.config.NodeID, groupID)
	}
	return r, nil
}
