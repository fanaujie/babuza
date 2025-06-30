package experimental

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

func (r *replica) doApplyJob(applyData *applyEntry) {
	defer poolReleaseApplyEntry(applyData)

	r.applySnapshot(applyData.snapshot)
	r.applyEntries(applyData.entries)
	if r.status.GetAppliedIndex()-r.status.GetSnapshotIndex() < r.config.SnapshotCount {
		return
	}
	term, index := r.status.GetAppliedTerm(), r.status.GetAppliedIndex()
	ctx, err := r.storage.CreateSnapshotContext(term, index, r.status.CloneConfState(), r.cluster, r.sessionManager)
	if err != nil {
		r.logger.Panicf("groupID[%d] nodeID[%d]: create snapshot context failed: %v", r.cluster.GroupID(),
			r.cluster.LocalPeerID(), err)
	}
	r.triggerSnapshot(ctx, nil)
}

func (r *replica) applySnapshot(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}
	if snap.Metadata.Index <= r.status.GetAppliedIndex() {
		r.logger.Panicf("groupID[%d] nodeID[%d]: apply snapshot index %d <= applied index %d", r.cluster.GroupID(),
			r.cluster.LocalPeerID(), snap.Metadata.Index, r.status.GetAppliedIndex())
	}
	if err := r.storage.RestoreFromSnapshot(snap.Metadata.Index, true, r.cluster, r.sessionManager); err != nil {
		r.logger.Panicf("raft[id=%d]: apply snapshot failed: %v", r.cluster.LocalPeerID(), err)
	}
	r.transport.RemovePeers()
	for _, p := range r.cluster.Peers() {
		if p.RaftPeerAttr.PeerID == r.cluster.LocalPeerID() {
			continue
		}
		r.transport.AddPeer(r.cluster.GroupID(), p.RaftPeerAttr.PeerID, p.RaftPeerAttr.RaftListenAddr)
	}
	r.logger.Infof("raft[id=%d]: applyEntry done for apply snapshot to storage (snapshot index=%d)",
		r.cluster.LocalPeerID(), snap.Metadata.Index)

	r.status.SetAppliedTerm(snap.Metadata.Term)
	r.status.SetAppliedIndex(snap.Metadata.Index)
	r.status.SetSnapshotIndex(snap.Metadata.Index)
	r.status.SetConfState(snap.Metadata.ConfState)
}

func (r *replica) applyEntries(entries []raftpb.Entry) {
	defer func() {
		r.completionReplier.MarkCompleted(r.status.GetAppliedIndex())
	}()
	length := len(entries)
	bsmInfo := r.storage.GetBasedStateMachineInfo()
	for _, entry := range entries {
		if len(entry.Data) == 0 {
			r.appliedFacade.ApplyNilEntryInNewTerm(entry.Index, entry.Term)
		} else {
			switch entry.Type {
			case raftpb.EntryNormal:
				if entry.Index <= bsmInfo.OpenAppliedIndex() {
					// Skip applying if only the disk-based state machine was successfully opened
					if !r.storage.GetBasedStateMachineInfo().IsDiskType() {
						r.logger.Panicf("groupID[%d] nodeID[%d]: apply entry index %d <= opened applied index %d", r.cluster.GroupID(),
							r.cluster.LocalPeerID(), entry.Index, r.status.GetAppliedIndex())
					}
					continue
				}
				normalReq, ar, session := r.appliedFacade.ApplyNormalEntry(entry)
				if ar.IsEmpty() {
					ar = r.storage.Apply(ibabuza.Entry{
						Term:    entry.Term,
						Index:   entry.Index,
						Command: normalReq.StateMachineLog,
					})
					if err := session.AddResult(normalReq.Context.SequenceNum, time.Now().UnixNano(), ar); err != nil {
						r.logger.Panicf("groupID[%d] nodeID[%d]: add result failed: %v", r.cluster.GroupID(),
							r.cluster.LocalPeerID(), err)
					}
				}
				r.appliedFacade.SendAppliedResult(normalReq.Context.ReplyID, ar)
			case raftpb.EntryConfChange:
				// do nothing, just apply conf change entry before
			default:
				r.logger.Panicf("groupID[%d] nodeID[%d]: not support raft toApplyEntry type %d", r.cluster.GroupID(),
					r.cluster.LocalPeerID(), uint64(entry.Type))
			}
		}
	}
	if length > 0 {
		r.status.SetAppliedTerm(entries[length-1].Term)
		r.status.SetAppliedIndex(entries[length-1].Index)
	}
}

func (r *replica) triggerSnapshot(snapCtx babuza.StorageSnapshotContext, snapshotResultCh chan babuza.SnapshotResult) {
	r.status.SetSnapshotIndex(snapCtx.Index())
	doSnapshot := func() {
		metadata, err := r.doSnapshot(snapCtx)
		if err != nil {
			r.logger.Panicf("groupID[%d] nodeID[%d]: do snapshot failed: %v", r.cluster.GroupID(), r.cluster.LocalPeerID(), err)
		}
		r.logger.Infof("raft[id=%d]: do snapshot done (index=%d)", r.cluster.LocalPeerID(), metadata.Snapshot.Metadata.Index)
		if snapshotResultCh != nil {
			snapshotResultCh <- babuza.NewSnapshotResult(metadata, err)
		}
	}
	if !r.storage.GetBasedStateMachineInfo().SupportConcurrentSnapshot() {
		doSnapshot()
	} else {
		r.closer.Run(func() {
			doSnapshot()
		})
	}

}

func (r *replica) doSnapshot(snapCtx babuza.StorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {
	metadata, err := r.storage.SaveStateMachineSnapshot(snapCtx)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	if err = r.storage.Save(raftpb.HardState{}, nil, metadata.Snapshot); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	inflight := r.status.GetInflightSnapshots()
	if inflight > 0 {
		r.logger.Warningf("raft[id=%d]: inflight snapshot counts=%d, skip compaction", r.cluster.LocalPeerID(), inflight)
		return metadata, nil
	} else if inflight < 0 {
		r.logger.Fatalf("raft[id=%d]: inflight snapshot counts=%d is less than zero ", r.cluster.LocalPeerID(), inflight)
	}

	if err = r.storage.CompactAndReleaseSnapshot(snapCtx.Index(), metadata.Snapshot); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	return metadata, nil
}
