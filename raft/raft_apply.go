package raft

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

type applyEntryToStateMachine struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
}

type manualSnapshot struct {
	resultCh chan SnapshotResult
}

func (r *Raft) ApplyConfChange(clusterID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	// raftNode is thread safe, so we can call ApplyConfChange concurrently
	return r.raftNode.ApplyConfChange(cc), nil
}

func (r *Raft) processStateMachine() {
	for {
		select {
		case <-r.closer.CloseCh():
			return
		case s := <-r.manualSnapshotCh:
			term, index := r.status.GetAppliedTerm(), r.status.GetAppliedIndex()
			ctx, err := r.storage.CreateSnapshotContext(term, index, r.status.CloneConfState(), r.cluster, r.sessionMgr)
			if err != nil {
				s.resultCh <- SnapshotResult{
					err: err,
				}
				r.logger.Panicf("raft[id=%d]: create manual snapshot context failed: %v", r.cluster.ClusterID(), err)
				continue
			}
			r.triggerSnapshot(ctx, s.resultCh)
		case ap := <-r.applyCh:
			r.applySnapshot(ap.snapshot)
			if r.applyEntries(ap.entries) {
				close(r.removeSelfCh)
				select {
				case <-r.shutdownCh: //already shutdown
				default:
					time.AfterFunc(time.Second, func() {
						r.stop()
					})
				}
				return
			}
			if r.status.GetAppliedIndex()-r.status.GetSnapshotIndex() < r.config.SnapshotCount {
				continue
			}
			term, index := r.status.GetAppliedTerm(), r.status.GetAppliedIndex()
			ctx, err := r.storage.CreateSnapshotContext(term, index, r.status.CloneConfState(), r.cluster, r.sessionMgr)
			if err != nil {
				r.logger.Panicf("raft[id=%d]: create snapshot context failed: %v", r.cluster.ClusterID(), err)
			}
			r.triggerSnapshot(ctx, nil)
		}
	}
}

func (r *Raft) applyEntries(entries []raftpb.Entry) bool {
	removeSelf := false
	defer func() {
		appliedIndex := r.status.GetAppliedIndex()
		r.completionReplier.MarkCompleted(appliedIndex)
		r.metricsCollector.SetProposalAppliedIndex(appliedIndex)
	}()
	for pos := 0; pos < len(entries); pos++ {
		if removeSelf {
			break
		}
		entry := entries[pos]
		if len(entry.Data) == 0 {
			r.appliedFacade.ApplyNilEntryInNewTerm(entry.Index, entry.Term)
		} else {
			switch entry.Type {
			case raftpb.EntryNormal:
				applyEntry := r.appliedFacade.ApplyNormalEntry(entry)
				if applyEntry != nil {
					now := time.Now()
					r.storage.Apply(applyEntry)
					r.metricsCollector.RecordApplySec(time.Since(now).Seconds())
				}
			case raftpb.EntryConfChange:
				removeSelf = r.appliedFacade.ApplyConfChangeEntry(entry)
				if removeSelf {
					break
				}
			default:
				r.logger.Panicf("raft[id=%d]: not support raft toApplyEntry type %d", r.cluster.LocalPeerID(), uint64(entry.Type))
			}
		}
	}
	return removeSelf
}

func (r *Raft) applySnapshot(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}
	if snap.Metadata.Index <= r.status.GetAppliedIndex() {
		r.logger.Panicf("raft[id=%d]: apply snapshot index %d <= applied index %d", r.cluster.LocalPeerID(), snap.Metadata.Index, r.status.GetAppliedIndex())
	}
	if err := func() error {
		now := time.Now()
		r.metricsCollector.SetSnapshotApplyInProgress(1)
		defer func() {
			r.metricsCollector.SetSnapshotApplyInProgress(0)
			r.metricsCollector.RecordApplySnapshotSec(time.Since(now).Seconds())
		}()
		return r.storage.RestoreFromSnapshot(snap.Metadata.Index, true, r.cluster, r.sessionMgr)
	}(); err != nil {
		r.logger.Panicf("raft[id=%d]: apply snapshot failed: %v", r.cluster.LocalPeerID(), err)
	}
	r.trans.RemovePeers()
	for _, p := range r.cluster.Peers() {
		if p.RaftPeerAttr.Id == r.cluster.ClusterID() {
			continue
		}
		r.trans.AddPeer(p.RaftPeerAttr.Id, p.RaftPeerAttr.RaftListenAddr)
	}
	r.logger.Infof("raft[id=%d]: applyEntry done for apply snapshot to storage (snapshot index=%d)",
		r.cluster.LocalPeerID(), snap.Metadata.Index)

	r.status.SetAppliedTerm(snap.Metadata.Term)
	r.status.SetAppliedIndex(snap.Metadata.Index)
	r.status.SetSnapshotIndex(snap.Metadata.Index)
	r.status.SetConfState(snap.Metadata.ConfState)
	r.storage.SetStateMachineAppliedIndex(snap.Metadata.Index)
}

func (r *Raft) doSnapshot(snapCtx StorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {
	now := time.Now()
	metadata, err := r.storage.SaveStateMachineSnapshot(snapCtx)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	r.metricsCollector.RecordDoSnapshotSec(time.Since(now).Seconds())
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

func (r *Raft) triggerSnapshot(snapCtx StorageSnapshotContext, snapshotResultCh chan SnapshotResult) {
	r.status.SetSnapshotIndex(snapCtx.Index())
	doSnapshot := func() {
		metadata, err := r.doSnapshot(snapCtx)
		if err != nil {
			r.logger.Panicf("raft[id=%d]: do snapshot failed: %v", r.cluster.LocalPeerID(), err)
		}
		r.logger.Infof("raft[id=%d]: do snapshot done (index=%d)", r.cluster.LocalPeerID(), metadata.Snapshot.Metadata.Index)
		if snapshotResultCh != nil {
			snapshotResultCh <- SnapshotResult{
				metadata: metadata,
				err:      err,
			}
		}
	}
	if !r.storage.SupportConcurrentSnapshot() {
		doSnapshot()
	} else {
		r.closer.Run(func() {
			doSnapshot()
		})
	}

}
