package raft

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/idgenerator"
	"github.com/fanaujie/babuza/pkg/replier"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
	"time"
)

type ClientSession struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
}

type ClusterInfo struct {
	ClusterID   uint64
	LeaderID    uint64
	LocalPeerID uint64
	Peers       []babuzapb.Peer
}

type RaftState uint32

var raftStateMap = [...]string{
	"FollowerState",
	"CandidateState",
	"LeaderState",
	"PreCandidateState",
	"StopState",
}

func (rs RaftState) String() string {
	return raftStateMap[rs]
}

const (
	FollowerState RaftState = iota
	CandidateState
	LeaderState
	PreCandidateState
	StopState

	None uint64 = 0
)

type Status struct {
	State              RaftState
	ClusterID          uint64
	LocalPeerID        uint64
	GroupID            uint64
	LeaderID           uint64
	RaftTerm           uint64
	RaftCommittedIndex uint64
	RaftAppliedIndex   uint64
	LastLogTerm        uint64
	LastLogIndex       uint64
	LastSnapshotTerm   uint64
	LastSnapshotIndex  uint64
}

func (s Status) IsLeader() bool {
	if s.State == LeaderState {
		return true
	}
	return false
}

type Raft struct {
	config                    BabuzaConfig
	cluster                   ibabuza.Cluster
	walManager                ibabuza.WalManager
	snapshotManager           ibabuza.SnapshotManager
	sessionManager            ibabuza.SessionManager
	idGenerator               InternalIdGenerator
	resultReplier             InternalResultReplier
	completionReplier         InternalCompletionReplier
	raftNode                  raft.Node
	status                    ibabuza.Status
	trans                     ibabuza.Transport
	storage                   RaftStorage
	logger                    ibabuza.Logger
	metricsCollector          ibabuza.MetricsCollector
	raftListener              ibabuza.RaftListener
	appliedFacade             InternalAppliedFacade
	raftEventPublisher        *raftEventPublisher
	applyCh                   chan applyEntryToStateMachine
	manualSnapshotCh          chan manualSnapshot
	readStateCh               chan raft.ReadState
	readIndexCh               chan struct{}
	shutdownCh                chan struct{}
	removeSelfCh              chan struct{}
	closeRaftOnce             sync.Once
	linearizeReqNotifier      *syncutil.SignalManager
	firstCommitInTermNotifier *syncutil.EventSignal
	leaderChangeNotifier      *syncutil.EventSignal
	closer                    *syncutil.Closer
	shutdownMu                sync.Mutex
}

func NewRaft(cfg BabuzaConfig, bootstrap *BootstrapRaftCluster, raftListener ibabuza.RaftListener) (*Raft, error) {
	var err error
	r := &Raft{
		config:                    cfg,
		cluster:                   bootstrap.cluster,
		walManager:                bootstrap.walManager,
		snapshotManager:           bootstrap.snapshotManager,
		sessionManager:            bootstrap.sessionMgr,
		idGenerator:               idgenerator.New(cfg.LocalPeerID, uint64(time.Now().Nanosecond())),
		resultReplier:             replier.NewResult[ibabuza.ApplyResult](),
		completionReplier:         replier.NewCompletion(),
		raftNode:                  bootstrap.node,
		status:                    bootstrap.status,
		trans:                     bootstrap.trans,
		storage:                   bootstrap.storage,
		logger:                    bootstrap.logger,
		metricsCollector:          bootstrap.metricsCollector,
		raftListener:              raftListener,
		applyCh:                   make(chan applyEntryToStateMachine, 8),
		manualSnapshotCh:          make(chan manualSnapshot),
		readStateCh:               make(chan raft.ReadState),
		readIndexCh:               make(chan struct{}),
		shutdownCh:                make(chan struct{}),
		removeSelfCh:              make(chan struct{}),
		linearizeReqNotifier:      syncutil.NewSignalManager(),
		firstCommitInTermNotifier: syncutil.NewEventSignal(),
		leaderChangeNotifier:      syncutil.NewEventSignal(),
		closer:                    syncutil.NewCloser(),
	}

	if err = r.trans.SetupTransportRaft(&transportProcessor{
		Raft: r,
	}); err != nil {
		return nil, err
	}
	if err = r.trans.Start(); err != nil {
		return nil, err
	}
	r.walManager.Purger().Start()
	r.closer.Run(func() {
		r.processRaftReady()
	})
	r.closer.Run(func() {
		r.processStateMachine()
	})
	r.closer.Run(func() {
		r.processRaftLinearizedRead()
	})
	if raftListener != nil {
		r.raftEventPublisher = newRaftEventPublisher()
		r.closer.Run(func() {
			r.handleListenerEvent()
		})
	}
	r.appliedFacade = newAppliedFacadeFromRaft(r)
	return r, nil
}

func (r *Raft) RegisterSession(ctx context.Context) ProposedResult {
	replyID := r.idGenerator.Next()
	proposalData, err := EncodeRegisterSessionRequest(replyID, 0)
	if err != nil {
		return NewErrorResult(err)
	}
	ch, err := r.propose(ctx, replyID, proposalData)
	if err != nil {
		return NewErrorResult(err)
	}
	return NewProposalResult(ctx, r.closer, ch)
}

func (r *Raft) UnregisterSession(ctx context.Context, sessionID uint64) ProposedResult {
	replyID := r.idGenerator.Next()
	proposalData, err := EncodeRegisterSessionRequest(replyID, sessionID)
	if err != nil {
		return NewErrorResult(err)
	}
	ch, err := r.propose(ctx, replyID, proposalData)
	if err != nil {
		return NewErrorResult(err)
	}
	return NewProposalResult(ctx, r.closer, ch)
}

func (r *Raft) AddVotingPeer(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {

	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return NewErrorResult(ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return NewErrorResult(ErrLearnerCanNotVote)
	}
	replyID := r.idGenerator.Next()
	confChange, err := EncodeClusterConfigurationChange(replyID, session, raftpb.ConfChangeAddNode,
		r.cluster.GroupID(), raftPeerAttr, false)
	if err != nil {
		NewErrorResult(err)
	}
	ar, err := r.proposeConfChange(ctx, replyID, confChange)
	if err != nil {
		return NewErrorResult(err)
	}
	return ar
}

func (r *Raft) RemovePeer(ctx context.Context, session ClientSession, peerID uint64) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return NewErrorResult(ErrNotLeader)
	}
	replyID := r.idGenerator.Next()
	confChange, err := EncodeClusterConfigurationChange(replyID, session, raftpb.ConfChangeRemoveNode,
		r.cluster.GroupID(), babuzapb.RaftPeerAttribute{PeerID: peerID}, false)
	if err != nil {
		return NewErrorResult(err)
	}
	ar, err := r.proposeConfChange(ctx, replyID, confChange)
	if err != nil {
		return NewErrorResult(err)
	}
	return ar
}

func (r *Raft) UpdatePeer(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return NewErrorResult(ErrNotLeader)
	}
	replyID := r.idGenerator.Next()
	confChange, err := EncodeClusterConfigurationChange(replyID, session, raftpb.ConfChangeUpdateNode,
		r.cluster.GroupID(), raftPeerAttr, false)
	if err != nil {
		return NewErrorResult(err)
	}
	ar, err := r.proposeConfChange(ctx, replyID, confChange)
	if err != nil {
		return NewErrorResult(err)
	}
	return ar
}

func (r *Raft) AddLearner(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return NewErrorResult(ErrNotLeader)
	}
	if raftPeerAttr.IsLearner == false {
		return NewErrorResult(ErrNotLearner)
	}
	replyID := r.idGenerator.Next()
	confChange, err := EncodeClusterConfigurationChange(replyID, session, raftpb.ConfChangeAddLearnerNode,
		r.cluster.GroupID(), raftPeerAttr, false)
	if err != nil {
		return NewErrorResult(err)
	}
	ar, err := r.proposeConfChange(ctx, replyID, confChange)
	if err != nil {
		return NewErrorResult(err)
	}
	return ar
}

func (r *Raft) PromoteLearner(ctx context.Context, session ClientSession, peerID uint64) ProposedResult {
	//TODO: add test case
	result, err := func() (ProposedResult, error) {
		p, err := r.cluster.Peer(peerID)
		if err != nil {
			return nil, err
		}
		if p.RaftPeerAttr.IsLearner == false {
			return nil, ErrVotingMemberCanNotPromote
		}
		// only leader can check if the learner is ready
		if err = r.learnerReady(peerID); err != nil {
			return nil, err
		}
		replyID := r.idGenerator.Next()
		confChange, err := EncodeClusterConfigurationChange(replyID, session, raftpb.ConfChangeAddNode,
			r.cluster.GroupID(), babuzapb.RaftPeerAttribute{
				PeerID: peerID,
			}, true)
		if err != nil {
			return nil, err
		}
		return r.proposeConfChange(ctx, replyID, confChange)
	}()
	if err != nil {
		if !errors.Is(err, ErrNotLeader) {
			r.metricsCollector.IncrementLearnerPromoteFailed()
			return NewErrorResult(err)
		}
		// forward request to leader
	} else {
		r.metricsCollector.IncrementLearnerPromoteSucceed()
		return result
	}
	//TODO: forward request to leader when current node is not the leader
	return NewErrorResult(ErrNotLeader)
}

func (r *Raft) TransferLeader(ctx context.Context, transferee uint64) TransferLeaderResult {
	toPeer, err := r.cluster.Peer(transferee)
	if err != nil {
		return NewErrorResult(err)
	}
	if toPeer.RaftPeerAttr.IsLearner {
		return NewErrorResult(ErrLearnerCanNotSwitchLeadership)
	}
	leaderID := r.status.CloneSoftState().Lead
	if leaderID != raft.None {
		r.raftNode.TransferLeadership(ctx, leaderID, transferee)
		return NewTransferLeaderResult(ctx, transferee, r.closer, time.Second,
			r.getLeaderId)
	}
	return NewErrorResult(ErrNoLeader)
}

func (r *Raft) Propose(ctx context.Context, session ClientSession, log []byte) ProposedResult {
	replyID := r.idGenerator.Next()
	proposalData, err := EncodeProposedLog(replyID, session, log)
	if err != nil {
		return NewErrorResult(err)
	}
	ch, err := r.propose(ctx, replyID, proposalData)
	if err != nil {
		return NewErrorResult(err)
	}
	return NewProposalResult(ctx, r.closer, ch)
}

func (r *Raft) ManualSnapshot(ctx context.Context) ManualSnapshotResult {
	ch := make(chan SnapshotResult, 1)

	select {
	case <-r.closer.CloseCh():
		return NewErrorResult(ErrStopped)
	case r.manualSnapshotCh <- manualSnapshot{
		resultCh: ch}:
	}
	return &manualSnapshotResult{
		ctx:     ctx,
		closer:  r.closer,
		reader:  r.storage,
		done:    false,
		resulCh: ch,
	}
}

func (r *Raft) LinearizableRead(ctx context.Context) error {
	n := r.linearizeReqNotifier.Current()
	select {
	case <-r.closer.CloseCh():
		return ErrStopped
	case r.readIndexCh <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-r.closer.CloseCh():
		return ErrStopped
	case <-n.Channel():
		return n.Error()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Raft) Shutdown() ShutdownResult {
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	select {
	case <-r.removeSelfCh:
		return NewErrorResult(ErrStopped)
	default:
		select {
		case <-r.shutdownCh:
			return NewErrorResult(ErrStopped)
		default:
			close(r.shutdownCh)
			return NewShutdownResult(r.stop)
		}
	}
}

func (r *Raft) ApplicationServiceStart(ctx context.Context, appServiceAddresses []string) chan error {
	doneCh := make(chan error, 1)
	r.closer.Run(func() {
		r.applicationServiceStart(ctx, time.Millisecond*500, appServiceAddresses, doneCh)
	})
	return doneCh
}

func (r *Raft) ClusterInfo() ClusterInfo {
	return ClusterInfo{
		ClusterID:   r.cluster.ClusterID(),
		LeaderID:    r.getLeaderId(),
		LocalPeerID: r.cluster.LocalPeerID(),
		Peers:       r.cluster.Peers(),
	}
}

func (r *Raft) Status() Status {
	// TODO: add session or other info
	lastIndex, lastTerm, snapshot, err := r.storage.EntryStorageInfo()
	if err != nil {
		r.logger.Panic(err)
	}
	softStatus := r.status.CloneSoftState()
	raftState := RaftState(softStatus.RaftState)
	leaderID := softStatus.Lead
	select {
	case <-r.closer.CloseCh():
		raftState = StopState
		leaderID = None
	default:
	}

	return Status{
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
	}
}

func (r *Raft) GetStateMachine() ibabuza.BaseStateMachine {
	return r.storage.GetStateMachine()

}

func (r *Raft) stop() {
	r.closeRaftOnce.Do(func() {
		r.raftNode.Stop()
		r.trans.Stop()
		r.closer.Close()
		r.walManager.Close()
		r.snapshotManager.Close()
	})
}
