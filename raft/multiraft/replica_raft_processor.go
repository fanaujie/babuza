package multiraft

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (r *replica) ProcessTick() {
	r.rawNode.Tick()
}

func (r *replica) ProcessReady() {
	if r.rawNode.HasReady() {
		rd := r.rawNode.Ready()
		if rd.SoftState != nil {
			r.updateLeadership(*rd.SoftState)
		}
		isLeader := r.status.IsLeader()
		if len(rd.ReadStates) != 0 {
			//TODO: implement read state
		}
		if raft.IsEmptyHardState(rd.HardState) {
			r.status.SetHardStateTerm(rd.HardState.Term)
		}
		r.updateCommittedIndex(rd.CommittedEntries, rd.Snapshot)
		waitWALSync := shouldWaitWALSync(rd)
		if waitWALSync {
			if err := r.storage.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
				r.logger.Panicf("groupID[%d] raft[id=%d] save hard state, entries and snapshot failed: %v",
					r.cluster.ClusterID(), r.cluster.LocalPeerID(), err)
			}
		}

		emptySnapshot := raft.IsEmptySnap(rd.Snapshot)
		if len(rd.CommittedEntries) > 0 || !emptySnapshot {
			applyData := poolGetApplyEntry()
			applyData.entries = rd.CommittedEntries
			applyData.snapshot = rd.Snapshot
			if err := r.applyJobQueue.Put(func() {
				r.doApplyJob(applyData)
			}); err != nil {
				r.logger.Panicf("groupID[%d] raft[id=%d]: error putting apply job: %v",
					r.cluster.ClusterID(), r.cluster.LocalPeerID(), err)
			}
		}
		if isLeader {
			r.sendRaftMessage(rd.Messages)
		}

		if !waitWALSync {
			if err := r.storage.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
				r.logger.Panicf("groupID[%d] raft[id=%d] save hard state, entries and snapshot failed: %v",
					r.cluster.ClusterID(), r.cluster.LocalPeerID(), err)
			}
		}

		if !emptySnapshot {
			if err := r.storage.ApplyAndReleaseSnapshot(rd.Snapshot); err != nil {
				r.logger.Panicf("groupID[%d] raft[id=%d]: apply snapshot failed: %v", r.cluster.ClusterID(),
					r.cluster.LocalPeerID(), err)
			}
		}
		if err := r.storage.EntryStorageAppend(rd.Entries); err != nil {
			r.logger.Panicf("groupID[%d] raft[id=%d]: append entries failed: %v", r.cluster.ClusterID(),
				r.cluster.LocalPeerID(), err)
		}
		// The applyConfChangeEntry must be handled here to ensure the leader sends out messages (e.g., removing a follower) first.
		// This guarantees that the follower receives the message before the configuration change is actually applied.
		if r.applyConfChangeEntry(rd.CommittedEntries) {
			r.replicaEventCh <- replicaEvent{
				groupID: ibabuza.RaftGroupID(r.cluster.ClusterID()),
				event:   eventRemovePeer,
			}
			return
		}
		if !isLeader {
			r.sendRaftMessage(rd.Messages)
		}
		r.rawNode.Advance(rd)
	}
}

func (r *replica) ProcessStep() {
	r.processStepQueue()
	r.processReportUnreachableQueue()
	r.processReportSnapshotStateQueue()
}

func (r *replica) processReportSnapshotStateQueue() {
	if r.reportSnapshotStateQueue.Len() == 0 {
		return
	}
	items, err := r.reportSnapshotStateQueue.Get()
	if err != nil {
		r.logger.Warningf("groupID[%d] raft[id=%d]: error getting report snapshot state: %v", r.cluster.ClusterID(),
			r.cluster.LocalPeerID(), err)
		return
	}
	defer items.Release()
	for _, item := range items.Data {
		r.rawNode.ReportSnapshot(item.NodeID, item.Status)
	}
}

func (r *replica) processReportUnreachableQueue() {
	if r.reportUnreachableQueue.Len() == 0 {
		return
	}
	items, err := r.reportUnreachableQueue.Get()
	if err != nil {
		r.logger.Warningf("groupID[%d] raft[id=%d]: error getting report unreachable: %v", r.cluster.ClusterID(),
			r.cluster.LocalPeerID(), err)
		return
	}
	defer items.Release()
	for _, item := range items.Data {
		r.rawNode.ReportUnreachable(item.NodeID)
	}
}

func (r *replica) processStepQueue() {
	if r.stepQueue.Len() == 0 {
		return
	}
	items, err := r.stepQueue.Get()
	if err != nil {
		r.logger.Warningf("groupID[%d] raft[id=%d]: error getting step: %v", r.cluster.ClusterID(),
			r.cluster.LocalPeerID(), err)
		return
	}
	defer items.Release()
	for _, m := range items.Data {
		for _, msg := range m.Messages {
			if err = r.rawNode.Step(msg); err != nil {
				r.logger.Warningf("groupID[%d] raft[id=%d]: error stepping message: %v", r.cluster.ClusterID(),
					r.cluster.LocalPeerID(), err)
			}
		}
	}
}

func (r *replica) ProcessProposal() {
	if r.proposalQueue.Len() == 0 {
		return
	}
	items, err := r.proposalQueue.Get()
	if err != nil {
		r.logger.Warningf("groupID[%d] raft[id=%d]: error getting proposals: %v", r.cluster.ClusterID(),
			r.cluster.LocalPeerID(), err)
		return
	}
	defer items.Release()
	for _, item := range items.Data {
		if err = r.rawNode.Propose(item.data); err != nil {
			if errors.Is(err, raft.ErrProposalDropped) {
				err = babuza.ErrNotLeader
			} else if errors.Is(err, raft.ErrStopped) {
				err = babuza.ErrStopped
			}
			r.resultReplier.SendResult(item.replyID, ibabuza.ApplyResult{
				Error: err,
			})
			r.logger.Warningf("groupID[%d] raft[%d] propose failed, err: %v", r.cluster.ClusterID(),
				r.cluster.LocalPeerID(), err)
		}
		poolReleaseProposal(item)
	}
}

func (r *replica) ProcessConfigChange() {
	if r.configChangeQueue.Len() == 0 {
		return
	}
	items, err := r.configChangeQueue.Get()
	if err != nil {
		r.logger.Warningf("groupID[%d] raft[id=%d]: error getting config change: %v", r.cluster.ClusterID(),
			r.cluster.LocalPeerID(), err)
		return
	}
	defer items.Release()
	for _, item := range items.Data {
		if err = r.rawNode.ProposeConfChange(item.confChange); err != nil {
			if errors.Is(err, raft.ErrProposalDropped) {
				err = babuza.ErrNotLeader
			} else if errors.Is(err, raft.ErrStopped) {
				err = babuza.ErrStopped
			}
			r.resultReplier.SendResult(item.replyID, ibabuza.ApplyResult{
				Error: err,
			})
			r.logger.Warningf("groupID[%d] raft[%d] propose failed, err: %v", r.cluster.ClusterID(),
				r.cluster.LocalPeerID(), err)
		}
	}
}

func (r *replica) applyConfChangeEntry(committedEntries []raftpb.Entry) bool {
	for _, entry := range committedEntries {
		if entry.Type == raftpb.EntryConfChange {
			reqCtx, ar, removeSelf := r.appliedFacade.ApplyConfChangeEntry(entry)
			if ar.Error != nil {
				r.appliedFacade.SendAppliedResult(reqCtx.ReplyID, ar)
			} else {
				r.status.SetConfState(*ar.Response.(*raftpb.ConfState))
				ar.Response = nil
				r.appliedFacade.SendAppliedResult(reqCtx.ReplyID, ar)
				if removeSelf {
					return true
				}
			}
		}
	}
	return false
}

func (r *replica) sendRaftMessage(msgs []raftpb.Message) {
	appRespIndex := uint64(0)
	lastAppRespMsgIndex := 0
	optimiseAppendEntryResp := false //optimise for MsgAppResp
	for i := 0; i < len(msgs); i++ {
		m := &msgs[i]
		switch m.Type {
		case raftpb.MsgAppResp:
			if !m.Reject && m.Index > appRespIndex {
				appRespIndex = m.Index
				lastAppRespMsgIndex = i
				optimiseAppendEntryResp = true
			} else {
				r.transport.Send(babuzapb.MultiRaftMessage{
					GroupID: r.cluster.ClusterID(),
					Message: *m,
				})
			}
		case raftpb.MsgSnap:
			m.Snapshot.Metadata.ConfState = r.status.CloneConfState()
			r.transport.SendSnapshot(babuzapb.MultiRaftMessage{
				GroupID: r.cluster.ClusterID(),
				Message: *m,
			})
		default:
			r.transport.Send(babuzapb.MultiRaftMessage{
				GroupID: r.cluster.ClusterID(),
				Message: *m,
			})
		}
	}
	if optimiseAppendEntryResp {
		r.transport.Send(
			babuzapb.MultiRaftMessage{
				GroupID: r.cluster.ClusterID(),
				Message: msgs[lastAppRespMsgIndex],
			})
	}
}

func (r *replica) updateCommittedIndex(entries []raftpb.Entry, snap raftpb.Snapshot) {
	var newCommitIndex uint64
	if len(entries) != 0 {
		newCommitIndex = entries[len(entries)-1].Index
	}
	if snap.Metadata.Index > newCommitIndex {
		newCommitIndex = snap.Metadata.Index
	}
	if newCommitIndex != 0 && newCommitIndex > r.status.GetCommittedIndex() {
		r.status.SetCommittedIndex(newCommitIndex)
	}
}

func (r *replica) updateLeadership(currentState raft.SoftState) {
	preState := r.status.CloneSoftState()

	newLeader := currentState.Lead != raft.None && preState.Lead != currentState.Lead
	r.status.SetSoftState(currentState)
	if currentState.Lead == r.cluster.LocalPeerID() {
		r.status.SetLeader(true)
		//r.leaderCh <- leaderChange{
		//	RaftGroup: r.raftGroup,
		//	isLeader:  true,
		//}
	} else {
		if r.status.IsLeader() {
			r.status.SetLeader(false)
			//r.leaderCh <- leaderChange{
			//	RaftGroup: r.raftGroup,
			//	isLeader:  true,
			//}
		}
	}
	if newLeader {
		r.leaderChangeNotifier.CloseAndRenew()
	}

}

// For a cluster with only one member, the raft may send both the
// unstable entries and committed entries to etcdserver, and there
// may have overlapped log entries between them.
//
// etcd responds to the client once it finishes (actually partially)
// the applying workflow. But when the client receives the response,
// it doesn't mean etcd has already successfully saved the data,
// including BoltDB and WAL, because:
//  1. etcd commits the boltDB transaction periodically instead of on each request;
//  2. etcd saves WAL entries in parallel with applying the committed entries.
//
// Accordingly, it might run into a situation of data loss when the etcd crashes
// immediately after responding to the client and before the boltDB and WAL
// successfully save the data to disk.
// Note that this issue can only happen for clusters with only one member.
//
// For clusters with multiple members, it isn't an issue, because etcd will
// not commit & apply the data before it being replicated to majority members.
// When the client receives the response, it means the data must have been applied.
// It further means the data must have been committed.
// Note: for clusters with multiple members, the raft will never send identical
// unstable entries and committed entries to etcdserver.
//
// Refer to https://github.com/etcd-io/etcd/issues/14370.
func shouldWaitWALSync(rd raft.Ready) bool {
	if len(rd.CommittedEntries) == 0 || len(rd.Entries) == 0 {
		return false
	}

	// Check if there is overlap between unstable and committed entries
	// assuming that their index and term are only incrementing.
	lastCommittedEntry := rd.CommittedEntries[len(rd.CommittedEntries)-1]
	firstUnstableEntry := rd.Entries[0]
	return lastCommittedEntry.Term > firstUnstableEntry.Term ||
		(lastCommittedEntry.Term == firstUnstableEntry.Term && lastCommittedEntry.Index >= firstUnstableEntry.Index)
}
