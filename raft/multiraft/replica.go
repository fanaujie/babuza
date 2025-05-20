package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type RaftGroup struct {
	GroupID ibabuza.RaftGroupID
	PeerID  uint64
}

type leaderChange struct {
	RaftGroup
	isLeader bool
}

type replicaRequestQueue struct {
	proposal     *queue.SwapBufferQueue[*proposalRequest]
	configChange *queue.SwapBufferQueue[configChangeRequest]
	step         *queue.SwapBufferQueue[raftpb.Message]
}

type coalescedHeartbeatQueue struct {
	heartbeatMsg               *xsync.Map[uint64, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]]
	heartbeatRespMsg           *xsync.Map[uint64, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]]
	heartbeatLastActiveUnixSec *xsync.Map[uint64, int64]
}

func newReplicaRequestQueue() *replicaRequestQueue {
	return &replicaRequestQueue{
		proposal: queue.NewSwapBufferQueue[*proposalRequest](1024, nil),
		configChange: queue.NewSwapBufferQueue[configChangeRequest](8, func(requests []configChangeRequest) {
			for i := 0; i < len(requests); i++ {
				requests[i].confChange.Context = nil
			}
		}),
		step: queue.NewSwapBufferQueue[raftpb.Message](1024, func(messages []raftpb.Message) {
			for i := 0; i < len(messages); i++ {
				messages[i].Entries = nil
				messages[i].Context = nil
			}
		}),
	}
}

func (pool *replicaRequestQueue) Dispose() {
	pool.proposal.Dispose()
	pool.configChange.Dispose()
	pool.step.Dispose()
}

type replica struct {
	raftGroup                 RaftGroup
	config                    ReplicaRaftConfig
	cluster                   ibabuza.Cluster
	transport                 ibabuza.MultiRaftTransport
	status                    ibabuza.Status
	sessionManager            ibabuza.SessionManager
	storage                   babuza.RaftStorage
	appliedFacade             babuza.InternalAppliedFacade
	idGenerator               babuza.InternalIdGenerator
	resultReplier             babuza.InternalResultReplier
	completionReplier         babuza.InternalCompletionReplier
	firstCommitInTermNotifier *syncutil.Notifier
	leaderChangeNotifier      *syncutil.Notifier
	leaderCh                  chan leaderChange
	replicaEventCh            chan replicaEvent
	receivedSnapshotMsgCh     chan babuzapb.SnapshotMessage
	scheduler                 Scheduler
	applyJobQueue             JobQueue
	requestQueue              *replicaRequestQueue
	coalescedHeartbeat        *coalescedHeartbeatQueue
	mu                        struct {
		lock           sync.Mutex
		rawNode        *raft.RawNode
		unreachable    map[uint64]struct{}
		snapshotStatus map[uint64]raft.SnapshotStatus
	}
	logger ibabuza.Logger
	closer *syncutil.Closer
}

func (r *replica) Status() ibabuza.Status {
	return r.status
}

func (r *replica) Start() error {
	return r.applyJobQueue.Start()
}

func (r *replica) Stop() {
	r.closer.Close()
	r.requestQueue.Dispose()
	r.applyJobQueue.Stop()
	r.storage.GetStateMachine().Close()
}

func (r *replica) EnqueueProposal(ctx context.Context, session babuza.ClientSession, log []byte) babuza.ProposedResult {
	replyID := r.idGenerator.Next()
	data, err := babuza.EncodeProposedLog(replyID, session, log)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	proposal := poolGetProposal()
	proposal.replyID = replyID
	proposal.data = data
	if err = r.requestQueue.proposal.Put(proposal); err != nil {
		r.resultReplier.CancelResult(replyID)
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
	r.scheduler.EnqueueState(stateProposal, r.raftGroup.GroupID)
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) EnqueueConfigChange(ctx context.Context, session babuza.ClientSession, changeType raftpb.ConfChangeType,
	raftPeerAttr babuzapb.RaftPeerAttribute, promoteLearner bool) babuza.ProposedResult {

	replyID := r.idGenerator.Next()
	config, err := babuza.EncodeClusterConfigurationChange(replyID, session, changeType, raftPeerAttr, promoteLearner)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	if err = r.requestQueue.configChange.Put(configChangeRequest{
		replyID:    replyID,
		confChange: config,
	}); err != nil {
		return babuza.NewErrorResult(err)
	}
	r.scheduler.EnqueueState(stateConfigChange, r.raftGroup.GroupID)
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) RegisterSessionRequest(ctx context.Context) babuza.ProposedResult {

	replyID := r.idGenerator.Next()
	data, err := babuza.EncodeRegisterSessionRequest(replyID, 0)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	proposal := poolGetProposal()
	proposal.replyID = replyID
	proposal.data = data
	if err = r.requestQueue.proposal.Put(proposal); err != nil {
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
	r.scheduler.EnqueueState(stateProposal, r.raftGroup.GroupID)
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) UnregisterSessionRequest(ctx context.Context, sessionID uint64) babuza.ProposedResult {
	replyID := r.idGenerator.Next()
	data, err := babuza.EncodeRegisterSessionRequest(replyID, sessionID)
	if err != nil {
		return babuza.NewErrorResult(err)
	}
	proposal := poolGetProposal()
	proposal.replyID = replyID
	proposal.data = data
	if err = r.requestQueue.proposal.Put(proposal); err != nil {
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
	r.scheduler.EnqueueState(stateProposal, r.raftGroup.GroupID)
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) EnqueueStep(msg raftpb.Message) error {
	if err := r.requestQueue.step.Put(msg); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue step error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateStep, r.raftGroup.GroupID)
	return nil
}

func (r *replica) ReportUnreachable(nodeID uint64) error {
	r.mu.lock.Lock()
	defer r.mu.lock.Unlock()
	if _, ok := r.mu.unreachable[nodeID]; ok {
		return nil
	}
	r.mu.unreachable[nodeID] = struct{}{}
	return nil
}

func (r *replica) ReportSnapshot(nodeID uint64, status raft.SnapshotStatus) error {
	r.mu.lock.Lock()
	defer r.mu.lock.Unlock()
	r.mu.snapshotStatus[nodeID] = status
	return nil
}

func (r *replica) RaftStatus() raft.Status {
	r.mu.lock.Lock()
	defer r.mu.lock.Unlock()
	return r.mu.rawNode.Status()
}

func (r *replica) TransferLeader(transfereeID uint64) {
	r.mu.lock.Lock()
	defer r.mu.lock.Unlock()
	r.mu.rawNode.TransferLeader(transfereeID)
}

func (r *replica) ClusterConfiguration() babuza.ClusterConfiguration {
	return babuza.ClusterConfiguration{
		ClusterID: r.cluster.ClusterID(),
		LeaderID:  r.status.CloneSoftState().Lead,
		GroupID:   uint64(r.cluster.GroupID()),
		Peers:     r.cluster.Peers(),
	}
}

func (r *replica) learnerReady(ctx context.Context, learnerId uint64) error {
	rs := r.RaftStatus()
	if rs.Progress == nil {
		return babuza.ErrNotLeader
	}
	var learnerMatch uint64
	found := false
	ClusterID := rs.ID
	for peerID, progress := range rs.Progress {
		if learnerId == peerID {
			learnerMatch = progress.Match
			found = true
			break
		}
	}
	if found {
		leaderMatch := rs.Progress[ClusterID].Match
		if float64(learnerMatch) < float64(leaderMatch)*r.config.LearnerReadyPercent {
			return babuza.ErrLearnerNotReady
		}
	}
	return nil
}
