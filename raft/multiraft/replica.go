package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RaftGroup struct {
	GroupID ibabuza.RaftGroupID
	PeerID  uint64
}

type leaderChange struct {
	RaftGroup
	isLeader bool
}

type reportUnreachable struct {
	NodeID uint64
	Status raft.SnapshotStatus
}

type reportSnapshotStatus struct {
	NodeID uint64
	Status raft.SnapshotStatus
}

type raftStatus struct {
	resultCh chan raft.Status
}

type replicaRequestQueue struct {
	proposal            *queue.SwapBufferQueue[*proposalRequest]
	configChange        *queue.SwapBufferQueue[configChangeRequest]
	step                *queue.SwapBufferQueue[babuzapb.BatchMessage]
	reportUnreachable   *queue.SwapBufferQueue[reportUnreachable]
	reportSnapshotState *queue.SwapBufferQueue[reportSnapshotStatus]
	raftStatus          *queue.SwapBufferQueue[raftStatus]
	transferLeader      *queue.SwapBufferQueue[uint64]
}

func newReplicaRequestQueue() *replicaRequestQueue {
	return &replicaRequestQueue{
		proposal: queue.NewSwapBufferQueue[*proposalRequest](1024, nil),
		configChange: queue.NewSwapBufferQueue[configChangeRequest](8, func(requests []configChangeRequest) {
			for i := 0; i < len(requests); i++ {
				requests[i].confChange.Context = nil
			}
		}),
		step: queue.NewSwapBufferQueue[babuzapb.BatchMessage](1024, func(messages []babuzapb.BatchMessage) {
			for i := 0; i < len(messages); i++ {
				messages[i].Messages = nil
			}
		}),
		reportUnreachable:   queue.NewSwapBufferQueue[reportUnreachable](8, nil),
		reportSnapshotState: queue.NewSwapBufferQueue[reportSnapshotStatus](8, nil),
		raftStatus: queue.NewSwapBufferQueue[raftStatus](8, func(statuses []raftStatus) {
			for i := 0; i < len(statuses); i++ {
				statuses[i].resultCh = nil
			}
		}),
		transferLeader: queue.NewSwapBufferQueue[uint64](8, nil),
	}
}

func (pool *replicaRequestQueue) Dispose() {
	pool.proposal.Dispose()
	pool.configChange.Dispose()
	pool.step.Dispose()
	pool.reportUnreachable.Dispose()
	pool.reportSnapshotState.Dispose()
	pool.raftStatus.Dispose()
	pool.transferLeader.Dispose()
}

type replica struct {
	raftGroup                 RaftGroup
	config                    ReplicaRaftConfig
	cluster                   ibabuza.Cluster
	transport                 ibabuza.MultiRaftTransport
	status                    ibabuza.Status
	session                   ibabuza.SessionManager
	storage                   babuza.Storage
	appliedFacade             babuza.InternalAppliedFacade
	rawNode                   *raft.RawNode
	idGenerator               babuza.InternalIdGenerator
	resultReplier             babuza.InternalResultReplier
	completionReplier         babuza.InternalCompletionReplier
	firstCommitInTermNotifier *syncutil.Notifier
	leaderChangeNotifier      *syncutil.Notifier
	leaderCh                  chan leaderChange
	replicaEventCh            chan replicaEvent
	scheduler                 Scheduler
	applyJobQueue             JobQueue
	requestQueue              *replicaRequestQueue
	logger                    ibabuza.Logger
	closer                    *syncutil.Closer
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
}

func (r *replica) EnqueueProposal(ctx context.Context, session babuza.ClientSession, log []byte) babuza.ProposedResult {

	replyID := r.idGenerator.Next()
	data, err := babuza.EncodeProposedLog(replyID, session, log)
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
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) EnqueueStep(batchMsg babuzapb.BatchMessage) error {
	if err := r.requestQueue.step.Put(batchMsg); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue step error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateStep, r.raftGroup.GroupID)
	return nil
}

func (r *replica) EnqueueReportUnreachable(nodeID uint64) error {
	if err := r.requestQueue.reportUnreachable.Put(reportUnreachable{
		NodeID: nodeID,
	}); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue report unreachable error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateStep, r.raftGroup.GroupID)
	return nil
}

func (r *replica) EnqueueReportSnapshot(nodeID uint64, status raft.SnapshotStatus) error {
	if err := r.requestQueue.reportSnapshotState.Put(reportSnapshotStatus{
		NodeID: nodeID,
		Status: status,
	}); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue report snapshot error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateStep, r.raftGroup.GroupID)
	return nil
}

func (r *replica) EnqueueRaftStatus(resultCh chan raft.Status) error {
	if err := r.requestQueue.raftStatus.Put(raftStatus{
		resultCh: resultCh,
	}); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue raft status error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateRaftStatus, r.raftGroup.GroupID)
	return nil
}

func (r *replica) EnqueueTransferLeader(transfereeID uint64) error {
	if err := r.requestQueue.transferLeader.Put(transfereeID); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue transfer leader error", r.raftGroup.GroupID)
	}
	r.scheduler.EnqueueState(stateRaftTransferLeader, r.raftGroup.GroupID)
	return nil
}

func (r *replica) ClusterConfiguration() babuza.ClusterConfiguration {
	return babuza.ClusterConfiguration{
		ClusterID: r.cluster.ClusterID(),
		LeaderID:  r.status.CloneSoftState().Lead,
		Peers:     r.cluster.Peers(),
	}
}

func (r *replica) learnerReady(ctx context.Context, learnerId uint64) error {
	resultCh := make(chan raft.Status, 1)
	if err := r.EnqueueRaftStatus(resultCh); err != nil {
		return errors.Wrapf(err, "GroupID[%d] enqueue raft status error", r.raftGroup.GroupID)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.closer.CloseCh():
		return babuza.ErrStopped
	case rs := <-resultCh:
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
}
