package raft

import (
	"context"
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

const (
	MemberJoinEvent   uint64 = 1
	MemberUpdateEvent uint64 = 2
	MemberLeaveEvent  uint64 = 3
)

type ClusterMemberEvent struct {
	Event uint64
	Peer  babuzapb.RaftPeerAttribute
}

type ClientSession struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
}

type ClusterConfiguration struct {
	ClusterID uint64
	LeaderID  uint64
	Peers     []babuzapb.Peer
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
	ClusterId          uint64
	LocalPeerId        uint64
	LeaderId           uint64
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
	applyIterator             InternalEntriesIterator
	sessionMgr                ibabuza.SessionManager
	idGenerator               InternalIdGenerator
	resultReplier             InternalResultReplier
	completionReplier         InternalCompletionReplier
	raftNode                  raft.Node
	status                    InternalStatus
	trans                     ibabuza.Transport
	storage                   InternalStorage
	logger                    ibabuza.Logger
	appliedFacade             InternalAppliedFacade
	applyCh                   chan applyEntryToStateMachine
	manualSnapshotCh          chan manualSnapshot
	readStateCh               chan raft.ReadState
	readIndexCh               chan struct{}
	shutdownCh                chan struct{}
	removeSelfCh              chan struct{}
	leaderCh                  chan bool
	clusterMemberEventCh      chan ClusterMemberEvent
	closeRaftOnce             sync.Once
	linearizeReqNotifier      *syncutil.ErrNotifier
	firstCommitInTermNotifier *syncutil.Notifier
	leaderChangeNotifier      *syncutil.Notifier
	closer                    *syncutil.Closer
	shutdownMu                sync.Mutex
}

func NewRaft(cfg BabuzaConfig, bootstrap *BootstrapRaftCluster) (*Raft, error) {
	var err error
	r := &Raft{
		config:                    cfg,
		cluster:                   bootstrap.cluster,
		sessionMgr:                bootstrap.sessionMgr,
		idGenerator:               idgenerator.New(cfg.LocalPeerId, uint64(time.Now().Nanosecond())),
		resultReplier:             replier.NewResult[ibabuza.ApplyResult](),
		completionReplier:         replier.NewCompletion(),
		raftNode:                  bootstrap.node,
		status:                    bootstrap.status,
		trans:                     bootstrap.trans,
		storage:                   bootstrap.storage,
		logger:                    bootstrap.logger,
		applyCh:                   make(chan applyEntryToStateMachine),
		manualSnapshotCh:          make(chan manualSnapshot),
		readStateCh:               make(chan raft.ReadState),
		readIndexCh:               make(chan struct{}),
		shutdownCh:                make(chan struct{}),
		removeSelfCh:              make(chan struct{}),
		leaderCh:                  make(chan bool, 1),
		clusterMemberEventCh:      make(chan ClusterMemberEvent, 3),
		linearizeReqNotifier:      syncutil.NewErrNotifier(),
		firstCommitInTermNotifier: syncutil.NewNotifier(),
		leaderChangeNotifier:      syncutil.NewNotifier(),
		closer:                    syncutil.NewCloser(),
	}
	r.appliedFacade = newAppliedFacadeFromRaft(r)
	r.applyIterator = newIterator(r.appliedFacade)

	if err = r.trans.SetupTransportRaft(&transportProcessor{
		Raft: r,
	}); err != nil {
		return nil, err
	}
	if err = r.trans.Start(); err != nil {
		return nil, err
	}
	r.closer.Run(func() {
		r.processRaftReady()
	})
	r.closer.Run(func() {
		r.processStateMachine()
	})
	r.closer.Run(func() {
		r.processRaftLinearizedRead()
	})
	return r, nil
}

func (r *Raft) RegisterSession(ctx context.Context) ProposedResult {
	replyId := r.idGenerator.Next()
	proposalData, err := encodeRegisterSessionRequest(replyId)
	if err != nil {
		return newErrorResult(err)
	}
	ch, err := r.propose(ctx, replyId, proposalData)
	if err != nil {
		return newErrorResult(err)
	}
	return newProposalResult(ctx, r.closer, ch)
}

func (r *Raft) AddVotingPeer(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {

	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return newErrorResult(ErrNotLeader)
	}
	if raftPeerAttr.IsLearner {
		return newErrorResult(ErrLearnerCanNotVote)
	}
	replyId := r.idGenerator.Next()
	confChange, err := encodeClusterConfigurationChange(replyId, session, raftpb.ConfChangeAddNode,
		raftPeerAttr, false)
	if err != nil {
		newErrorResult(err)
	}
	return r.proposeConfChange(ctx, replyId, confChange)
}

func (r *Raft) RemovePeer(ctx context.Context, session ClientSession, peerId uint64) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return newErrorResult(ErrNotLeader)
	}
	replyId := r.idGenerator.Next()
	confChange, err := encodeClusterConfigurationChange(replyId, session, raftpb.ConfChangeRemoveNode,
		babuzapb.RaftPeerAttribute{Id: peerId}, false)
	if err != nil {
		return newErrorResult(err)
	}
	return r.proposeConfChange(ctx, replyId, confChange)
}

func (r *Raft) UpdatePeer(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return newErrorResult(ErrNotLeader)
	}
	replyId := r.idGenerator.Next()
	confChange, err := encodeClusterConfigurationChange(replyId, session, raftpb.ConfChangeUpdateNode, raftPeerAttr, false)
	if err != nil {
		return newErrorResult(err)
	}
	return r.proposeConfChange(ctx, replyId, confChange)
}

func (r *Raft) AddLearner(ctx context.Context, session ClientSession, raftPeerAttr babuzapb.RaftPeerAttribute) ProposedResult {
	if r.config.DisableProposalForwarding && r.status.IsLeader() == false {
		return newErrorResult(ErrNotLeader)
	}
	if raftPeerAttr.IsLearner == false {
		return newErrorResult(ErrVotingMemberCanNotPromote)
	}
	replyId := r.idGenerator.Next()
	confChange, err := encodeClusterConfigurationChange(replyId, session, raftpb.ConfChangeAddLearnerNode, raftPeerAttr, false)
	if err != nil {
		return newErrorResult(err)
	}
	return r.proposeConfChange(ctx, replyId, confChange)
}

func (r *Raft) PromoteLearner(ctx context.Context, session ClientSession, peerId uint64) ProposedResult {
	p, err := r.cluster.Peer(peerId)
	if err != nil {
		return newErrorResult(err)
	}
	if p.RaftPeerAttr.IsLearner == false {
		return newErrorResult(ErrVotingMemberCanNotPromote)
	}
	if err = r.learnerReady(peerId); err != nil {
		return newErrorResult(err)
	}
	replyId := r.idGenerator.Next()
	confChange, err := encodeClusterConfigurationChange(replyId, session, raftpb.ConfChangeAddNode, babuzapb.RaftPeerAttribute{
		Id: peerId,
	}, true)
	if err != nil {
		return newErrorResult(err)
	}
	return r.proposeConfChange(ctx, replyId, confChange)
}

func (r *Raft) TransferLeader(ctx context.Context, transferee uint64) TransferLeaderResult {
	toPeer, err := r.cluster.Peer(transferee)
	if err != nil {
		return newErrorResult(err)
	}
	if toPeer.RaftPeerAttr.IsLearner {
		return newErrorResult(ErrLearnerCanNotSwitchLeadership)
	}
	r.raftNode.TransferLeadership(ctx, r.config.LocalPeerId, transferee)
	res := newTransferLeaderResult(ctx, transferee, r.closer, time.Second,
		r.getLeaderId)
	go res.do()
	return res
}

func (r *Raft) Propose(ctx context.Context, session ClientSession, log []byte) ProposedResult {
	replyId := r.idGenerator.Next()
	proposalData, err := encodeProposedLog(replyId, session, log)
	if err != nil {
		return newErrorResult(err)
	}
	ch, err := r.propose(ctx, replyId, proposalData)
	if err != nil {
		return newErrorResult(err)
	}
	return newProposalResult(ctx, r.closer, ch)
}

func (r *Raft) ManualSnapshot(ctx context.Context) ManualSnapshotResult {
	ch := make(chan snapshotResult, 1)

	select {
	case <-r.closer.CloseCh():
		return newErrorResult(ErrStopped)
	case r.manualSnapshotCh <- manualSnapshot{
		resultCh: ch}:
	}
	return &manualSnapshotResult{
		ctx:     ctx,
		closer:  r.closer,
		storage: r.storage,
		done:    false,
		resulCh: ch,
		babuza:  r,
	}
}

func (r *Raft) LinearizableRead(ctx context.Context) error {
	n := r.linearizeReqNotifier.Get()
	select {
	case <-r.closer.CloseCh():
		return ErrStopped
	case r.readIndexCh <- struct{}{}:
	default:
	}
	select {
	case <-r.closer.CloseCh():
		return ErrStopped
	case <-n.GetCh():
		return n.GetError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Raft) LeaderCh() chan bool {
	return r.leaderCh
}

func (r *Raft) ClusterMemberEventCh() chan ClusterMemberEvent {
	return r.clusterMemberEventCh
}

func (r *Raft) Shutdown() ShutdownResult {
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	select {
	case <-r.removeSelfCh:
		return newErrorResult(ErrStopped)
	default:
		select {
		case <-r.shutdownCh:
			return newErrorResult(ErrStopped)
		default:
			close(r.shutdownCh)
			return newShutdownResult(r.stop)
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

func (r *Raft) ClusterConfiguration() ClusterConfiguration {
	return ClusterConfiguration{
		ClusterID: r.cluster.ClusterId(),
		LeaderID:  r.getLeaderId(),
		Peers:     r.cluster.Peers(),
	}
}

func (r *Raft) LeaderAppServiceAddresses() []string {
	leaderId := r.getLeaderId()
	if leaderId == raft.None {
		return nil
	}
	p, err := r.cluster.Peer(leaderId)
	if err != nil {
		panic(err)
	}
	return p.AppServiceAddresses
}

func (r *Raft) Status() Status {
	// TODO: add session or other info
	lastIndex, lastTerm, snapshot, err := r.storage.EntryStorageInfo()
	if err != nil {
		r.logger.Panic(err)
	}
	softStatus := r.status.CloneSoftState()
	raftState := RaftState(softStatus.RaftState)
	leaderId := softStatus.Lead
	select {
	case <-r.closer.CloseCh():
		raftState = StopState
		leaderId = None
	default:
	}

	return Status{
		State:              raftState,
		ClusterId:          r.cluster.ClusterId(),
		LocalPeerId:        r.cluster.LocalPeerID(),
		LeaderId:           leaderId,
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
		r.storage.Close()
	})
}
