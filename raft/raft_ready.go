package raft

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

func (r *Raft) processRaftReady() {
	isLeader := false
	ticker := time.NewTicker(time.Duration(r.config.LogicalTickMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.raftNode.Tick()
		case <-r.closer.CloseCh():
			return
		case rd := <-r.raftNode.Ready():
			if rd.SoftState != nil {
				r.updateLeadership(*rd.SoftState)
				isLeader = rd.SoftState.RaftState == raft.StateLeader
			}
			if len(rd.ReadStates) != 0 {
				select {
				case r.readStateCh <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(time.Second):
					r.logger.Warningf("raft[id=%d] timed out sending read state. timeout=%d", r.cluster.LocalPeerID(), time.Second)
				case <-r.closer.CloseCh():
					return
				}
			}

			if raft.IsEmptyHardState(rd.HardState) {
				r.status.SetHardStateTerm(rd.HardState.Term)
			}
			r.updateCommittedIndex(rd.CommittedEntries, rd.Snapshot)
			waitWALSync := shouldWaitWALSync(rd)
			if waitWALSync {
				if err := r.storage.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
					r.logger.Panicf("raft[id=%d] save hard state, entries and snapshot failed: %v", r.cluster.LocalPeerID(), err)
				}

			}

			emptySnapshot := raft.IsEmptySnap(rd.Snapshot)
			if len(rd.CommittedEntries) > 0 || !emptySnapshot {
				select {
				case <-r.closer.CloseCh():
					return
				case r.applyCh <- applyEntryToStateMachine{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot}:
				}
			}
			if isLeader {
				r.sendRaftMessage(rd.Messages)
			}

			if !waitWALSync {
				if err := r.storage.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
					r.logger.Panicf("raft[id=%d] save hard state, entries and snapshot failed: %v", r.cluster.LocalPeerID(), err)
				}
			}

			if !emptySnapshot {
				if err := r.storage.ApplyAndReleaseSnapshot(rd.Snapshot); err != nil {
					r.logger.Panicf("raft[id=%d]: apply snapshot failed: %v", r.cluster.LocalPeerID(), err)
				}
			}
			if err := r.storage.EntryStorageAppend(rd.Entries); err != nil {
				r.logger.Panicf("raft[id=%d]: append entries failed: %v", r.cluster.LocalPeerID(), err)
			}
			if !isLeader {

				// Candidate or follower needs to wait for all pending configuration
				// changes to be applied before sending messages.
				// Otherwise we might incorrectly count votes (e.g. votes from removed members).
				// Also slow machine's follower raft-layer could proceed to become the leader
				// on its own single-node cluster, before apply-layer applies the config change.
				// We simply wait for ALL pending entries to be applied for now.
				// We might improve this later on if it causes unnecessary long blocking issues.
				var lastConfChangIndex uint64
				for i := range rd.CommittedEntries {
					e := &rd.CommittedEntries[i]
					if raftpb.EntryConfChange == e.Type {
						lastConfChangIndex = e.Index
					}
				}
				if lastConfChangIndex > 0 {
					select {
					case <-r.completionReplier.AcquireCompletionChan(lastConfChangIndex):
					case <-r.closer.CloseCh():
						return
					}
				}
				r.sendRaftMessage(rd.Messages)
			}
			r.raftNode.Advance()
		}
	}
}

func (r *Raft) sendRaftMessage(msgs []raftpb.Message) {
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
				r.trans.Send(*m)
			}
		case raftpb.MsgSnap:
			m.Snapshot.Metadata.ConfState = r.status.CloneConfState()
			r.trans.SendSnapshot(*m)
		default:
			r.trans.Send(*m)
		}
	}
	if optimiseAppendEntryResp {
		r.trans.Send(msgs[lastAppRespMsgIndex])
	}
}

func (r *Raft) updateCommittedIndex(entries []raftpb.Entry, snap raftpb.Snapshot) {
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

func (r *Raft) updateLeadership(currentState raft.SoftState) {
	preState := r.status.CloneSoftState()

	newLeader := currentState.Lead != raft.None && preState.Lead != currentState.Lead
	r.status.SetSoftState(currentState)
	if currentState.Lead == raft.None {
		r.metricsCollector.SetIsLeader(0)
	} else {
		r.metricsCollector.SetHasLeader(1)
	}
	if currentState.Lead == r.config.LocalPeerID {
		r.status.SetLeader(true)
		r.metricsCollector.SetIsLeader(1)
		r.leaderCh <- true
	} else {
		if r.status.IsLeader() {
			r.status.SetLeader(false)
			r.metricsCollector.SetIsLeader(0)
			r.leaderCh <- false
		}
	}
	if newLeader {
		r.metricsCollector.IncrementLeaderChanges()
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
