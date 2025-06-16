package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sort"
)

type RaftGroupPeersInfo struct {
	ClusterID   uint64
	GroupID     ibabuza.RaftGroupID
	LeaderID    uint64
	LocalPeerID uint64
	Peers       []babuzapb.RaftPeerAttribute
}

func (info *RaftGroupPeersInfo) IsLeader() bool {
	return info.LeaderID == info.LocalPeerID
}

type Store struct {
	config                  StoreConfig
	trans                   ibabuza.MultiRaftTransport
	storage                 BootstrapStorage
	factory                 ComponentsFactory
	logger                  ibabuza.Logger
	scheduler               Scheduler
	raftListener            ibabuza.MultiRaftListener
	raftEventPublisher      *raftEventPublisher
	closer                  *syncutil.Closer
	replicaSet              *xsync.Map[ibabuza.RaftGroupID, *replica]
	coalescedHeartbeatQueue *coalescedHeartbeatQueue
	idGenerator             babuza.InternalIdGenerator
	leaderTransferChecker   *leaderTransferChecker
}

func (s *Store) Start() error {
	var err error
	tp := &transportProcessor{
		Store: s,
	}
	if err = s.trans.SetupTransportRaft(tp); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.closer.Close()
			_ = s.trans.Stop()
			s.scheduler.Stop()
			s.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
				value.Stop()
				return true
			})
		}
	}()
	if err = s.trans.Start(); err != nil {
		err = errors.Errorf("Store[%d] transport start error: %v", s.config.StoreID, err)
		return err
	}
	if err = s.scheduler.Start(); err != nil {
		err = errors.Errorf("Store[%d] raftScheduler start error: %v", s.config.StoreID, err)
		return err
	}
	s.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
		if err = value.Start(); err != nil {
			err = errors.Errorf("Store[%d] replica start error: %v", s.config.StoreID, err)
			return false
		}
		return true
	})
	s.closer.Run(func() {
		s.replicaRaftTick()
	})
	s.closer.Run(func() {
		s.replicaListener()
	})
	s.closer.Run(func() {
		s.replicaCoalescedHeartbeat()
	})
	return nil
}

func (s *Store) Stop() {
	s.trans.Stop()
	s.closer.Close()
	s.scheduler.Stop()
	s.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
		value.Stop()
		return true
	})
	s.storage.Close()
	s.replicaSet = nil
}

func (s *Store) CreateRaftGroup(peersConfig *PeersConfiguration, join bool) error {
	if _, ok := s.replicaSet.Load(peersConfig.GroupID()); ok {
		return errors.Errorf("Store[%d] raft group %d already exists", s.config.StoreID, peersConfig.GroupID())
	}
	r, err := bootstrapReplicaWithConfiguration(s, peersConfig, join)
	if err != nil {
		return errors.Errorf("Store[%d] failed to bootstrap new replica(raft group id=%d): %v", s.config.StoreID,
			peersConfig.GroupID(), err)
	}
	s.replicaSet.Store(peersConfig.GroupID(), r)
	return r.Start()
}

func (s *Store) CreateBasicRaftGroup(groupID ibabuza.RaftGroupID, localPeerID, leaderID uint64, leaderRaftAddr string) error {
	if _, ok := s.replicaSet.Load(groupID); ok {
		return errors.Errorf("Store[%d] raft group %d already exists", s.config.StoreID, groupID)
	}
	r, err := newReplicaWithoutConfiguration(s, groupID, localPeerID)
	if err != nil {
		return errors.Errorf("Store[%d] failed to create new raw-replica(raft group id=%d) peerID=%d: %v",
			s.config.StoreID, groupID, localPeerID, err)
	}
	s.trans.AddPeer(groupID, leaderID, leaderRaftAddr)
	s.replicaSet.Store(groupID, r)
	return r.Start()
}

func (s *Store) GetGroupIDs() []ibabuza.RaftGroupID {
	groupIDs := make([]ibabuza.RaftGroupID, 0)
	s.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
		groupIDs = append(groupIDs, key)
		return true
	})
	if len(groupIDs) > 1 {
		sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	}
	return groupIDs
}

func (s *Store) HasGroupID(groupID ibabuza.RaftGroupID) bool {
	_, ok := s.replicaSet.Load(groupID)
	return ok
}

func (s *Store) StateMachine(groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error) {
	r, err := s.getReplica(groupID)
	if err != nil {
		return nil, err
	}
	return r.storage.GetStateMachine(), nil
}

func (s *Store) RegisterSession(ctx context.Context, groupID ibabuza.RaftGroupID) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.RegisterSessionRequest(ctx)
}

func (s *Store) UnregisterSession(ctx context.Context, groupID ibabuza.RaftGroupID,
	sessionID uint64) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.UnregisterSessionRequest(ctx, sessionID)
}

func (s *Store) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	return r.EnqueueProposal(ctx, session, log)
}

func (s *Store) AddVotingPeer(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if s.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrLearnerCanNotVote)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddNode, raftPeerAttr, false)
}

func (s *Store) RemovePeer(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	peerID uint64) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if s.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeRemoveNode, babuzapb.RaftPeerAttribute{PeerID: peerID}, false)
}

func (s *Store) AddLearner(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	raftPeerAttr babuzapb.RaftPeerAttribute) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if !raftPeerAttr.IsLearner {
		return babuza.NewErrorResult(babuza.ErrNotLearner)
	}
	if s.config.DisableProposalForwarding && r.Status().IsLeader() == false {
		return babuza.NewErrorResult(babuza.ErrNotLeader)
	}
	return r.EnqueueConfigChange(ctx, session, raftpb.ConfChangeAddLearnerNode, raftPeerAttr, false)
}

func (s *Store) PromoteLearner(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	peerID uint64) babuza.ProposedResult {
	r, err := s.getReplica(groupID)
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
		return babuza.NewErrorResult(err)
	}
	return result
}

func (s *Store) TransferLeader(ctx context.Context, groupID ibabuza.RaftGroupID, transferee uint64) babuza.TransferLeaderResult {
	r, err := s.getReplica(groupID)
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
		r.TransferLeader(transferee)
		transLeaderReq := &transferRequest{
			ctx:              ctx,
			groupID:          groupID,
			status:           r.status,
			expectedLeaderID: transferee,
			resultChan:       make(chan error, 1),
		}
		err = s.leaderTransferChecker.AddTransfer(transLeaderReq)
		if err != nil {
			return babuza.NewErrorResult(err)
		}
		return newLeaderTransferResult(transLeaderReq.resultChan)
	}
	return babuza.NewErrorResult(babuza.ErrNoLeader)
}

func (s *Store) LinearizableRead(ctx context.Context, groupID ibabuza.RaftGroupID) error {
	r, err := s.getReplica(groupID)
	if err != nil {
		return err
	}
	chWithErr := r.linearizeReqNotifier.Current()
	select {
	case <-r.closer.CloseCh():
		return babuza.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	case r.readIndexCh <- struct{}{}:
	default:
	}
	select {
	case <-r.closer.CloseCh():
		return babuza.ErrStopped
	case <-chWithErr.Channel():
		return chWithErr.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) RaftGroupPeersInfo(groupID ibabuza.RaftGroupID) (RaftGroupPeersInfo, error) {
	r, err := s.getReplica(groupID)
	if err != nil {
		return RaftGroupPeersInfo{}, err
	}
	return r.RaftGroupPeersInfo(), nil
}

func (s *Store) Query(groupID ibabuza.RaftGroupID, key any) (any, error) {
	r, err := s.getReplica(groupID)
	if err != nil {
		return nil, err
	}
	return r.storage.GetStateMachine().Query(key)
}

func (s *Store) RaftGroupStatus(groupID ibabuza.RaftGroupID) (babuza.Status, error) {
	r, err := s.getReplica(groupID)
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
	r.cluster.Peers()
	return babuza.Status{
		State:              raftState,
		ClusterID:          r.cluster.ClusterID(),
		LocalPeerID:        r.cluster.LocalPeerID(),
		LeaderID:           leaderID,
		GroupID:            uint64(r.cluster.GroupID()),
		RaftTerm:           r.status.GetHardStateTerm(),
		RaftCommittedIndex: r.status.GetCommittedIndex(),
		RaftAppliedIndex:   r.status.GetAppliedIndex(),
		LastLogTerm:        lastTerm,
		LastLogIndex:       lastIndex,
		LastSnapshotTerm:   snapshot.Metadata.Term,
		LastSnapshotIndex:  snapshot.Metadata.Index,
	}, nil
}

func (s *Store) getReplica(groupID ibabuza.RaftGroupID) (*replica, error) {
	r, ok := s.replicaSet.Load(groupID)
	if !ok {
		return nil, errors.Errorf("Store[%d] raft group %d not found", s.config.StoreID, groupID)
	}
	return r, nil
}
