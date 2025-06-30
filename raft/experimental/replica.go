package experimental

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
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
	heartbeatMsg               *xsync.Map[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]]
	heartbeatRespMsg           *xsync.Map[string, *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]]
	heartbeatLastActiveUnixSec *xsync.Map[string, int64]
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
	firstCommitInTermNotifier *syncutil.EventSignal
	leaderChangeNotifier      *syncutil.EventSignal
	linearizeReqNotifier      *syncutil.SignalManager
	raftEventPublisher        *raftEventPublisher
	receivedSnapshotMsgCh     chan babuzapb.SnapshotMessage
	readStateCh               chan raft.ReadState
	readIndexCh               chan struct{}
	scheduler                 Scheduler
	applyJobQueue             JobQueue
	enqueueStepFunc           func(ibabuza.RaftGroupID, raftpb.Message) error
	coalescedHeartbeat        *coalescedHeartbeatQueue
	mu                        struct {
		lock        sync.Mutex
		rawNode     *raft.RawNode
		unreachable map[uint64]struct{}
	}
	logger ibabuza.Logger
	closer *syncutil.Closer
}

func (r *replica) Status() ibabuza.Status {
	return r.status
}

func (r *replica) Start() error {
	r.closer.Run(func() {
		r.processRaftLinearizedRead()
	})
	return nil
}

func (r *replica) Stop() {
	r.closer.Close()
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
	r.mu.rawNode.ReportSnapshot(nodeID, status)
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

func (r *replica) RaftGroupPeersInfo() RaftGroupPeersInfo {
	info := RaftGroupPeersInfo{
		ClusterID:   r.cluster.ClusterID(),
		GroupID:     r.raftGroup.GroupID,
		LeaderID:    r.status.CloneSoftState().Lead,
		LocalPeerID: r.raftGroup.PeerID,
	}
	for _, peer := range r.cluster.Peers() {
		info.Peers = append(info.Peers, peer.RaftPeerAttr)
	}
	return info
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
