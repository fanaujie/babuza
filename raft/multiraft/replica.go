package multiraft

import (
	"context"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
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

type replica struct {
	raftGroup                 RaftGroup
	config                    ReplicaRaftConfig
	applyJobQueue             ibabuza.MultiRaftReplicaJobQueue
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
	proposalQueue             *queue.Queue
	configChangeQueue         *queue.Queue
	applyConfChangeQueue      *queue.Queue
	logger                    ibabuza.Logger
	closer                    *syncutil.Closer
}

func (r *replica) Status() ibabuza.Status {
	return r.status
}

func (r *replica) Stop() {
	r.closer.Close()
	r.applyJobQueue.Stop()
	r.proposalQueue.Dispose()
	r.configChangeQueue.Dispose()
	r.applyConfChangeQueue.Dispose()
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
	if err = r.proposalQueue.Put(proposal); err != nil {
		poolReleaseProposal(proposal)
		return babuza.NewErrorResult(err)
	}
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
	configChange := poolGetConfigChange()
	configChange.replyID = replyID
	configChange.confChange = config
	if err = r.configChangeQueue.Put(configChange); err != nil {
		return babuza.NewErrorResult(err)
	}

	ch, err := r.resultReplier.AcquireResultChan(replyID)
	return babuza.NewProposalResult(ctx, r.closer, ch)
}

func (r *replica) EnqueueApplyConfChange(job *confChangeApplyJob) {
	if err := r.applyConfChangeQueue.Put(job); err != nil {
		r.logger.Errorf("failed to enqueue apply conf change job: %v", err)
		return
	}
}
