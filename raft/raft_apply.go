package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

type applyEntryToStateMachine struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	notifyCh chan struct{}
}

type snapshotResult struct {
	metadata babuzapb.SnapshotMetadata
	err      error
}

type manualSnapshot struct {
	resultCh chan snapshotResult
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
				s.resultCh <- snapshotResult{
					err: err,
				}
				r.logger.Panicf("raft[id=%d]: create manual snapshot context failed: %v", r.config.LocalPeerId, err)
				continue
			}
			r.triggerSnapshot(ctx, s.resultCh)
		case ap := <-r.applyCh:
			if err := r.applySnapshot(ap.snapshot); err != nil {
				r.logger.Panicf("raft[id=%d]: apply snapshot failed: %v", r.config.LocalPeerId, err)
			}
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
			<-ap.notifyCh
			if r.status.GetAppliedIndex()-r.status.GetSnapshotIndex() < r.config.SnapshotCount {
				continue
			}
			term, index := r.status.GetAppliedTerm(), r.status.GetAppliedIndex()
			ctx, err := r.storage.CreateSnapshotContext(term, index, r.status.CloneConfState(), r.cluster, r.sessionMgr)
			if err != nil {
				r.logger.Panicf("raft[id=%d]: create snapshot context failed: %v", r.config.LocalPeerId, err)
			}
			r.triggerSnapshot(ctx, nil)
		}
	}
}

func (r *Raft) applyEntries(entries []raftpb.Entry) bool {
	r.applyIterator.SetEntries(entries)
	defer r.applyIterator.ReleaseEntries()
	r.storage.Apply(r.applyIterator)
	r.completionReplier.MarkCompleted(r.status.GetAppliedIndex())
	return r.applyIterator.HasRemovedSelf()
}

func (r *Raft) applySnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}
	if snap.Metadata.Index <= r.status.GetAppliedIndex() {
		return fmt.Errorf("applied snapshot is already at index %d", snap.Metadata.Index)
	}
	if err := r.storage.RestoreFromSnapshot(snap.Metadata.Index, true, r.cluster, r.sessionMgr); err != nil {
		return err
	}
	r.trans.RemovePeers()
	for _, p := range r.cluster.Peers() {
		if p.RaftPeerAttr.Id == r.config.LocalPeerId {
			continue
		}
		r.trans.AddPeer(p.RaftPeerAttr.Id, p.RaftPeerAttr.RaftListenAddr)
	}
	r.logger.Infof("raft[id=%d]: applyEntry done for apply snapshot to storage (snapshot index=%d)",
		r.config.LocalPeerId, snap.Metadata.Index)

	r.status.SetAppliedTerm(snap.Metadata.Term)
	r.status.SetAppliedIndex(snap.Metadata.Index)
	r.status.SetSnapshotIndex(snap.Metadata.Index)
	r.status.SetConfState(snap.Metadata.ConfState)
	r.storage.SetStateMachineAppliedIndex(snap.Metadata.Index)
	return nil
}

func (r *Raft) doSnapshot(snapCtx InternalStorageSnapshotContext) (babuzapb.SnapshotMetadata, error) {

	metadata, err := r.storage.SaveStateMachineSnapshot(snapCtx)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	if err = r.storage.Save(raftpb.HardState{}, nil, metadata.Snapshot); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	inflight := r.status.GetInflightSnapshots()
	if inflight > 0 {
		r.logger.Warningf("raft[id=%d]: inflight snapshot counts=%d, skip compaction", r.config.LocalPeerId, inflight)
		return metadata, nil
	} else if inflight < 0 {
		r.logger.Fatalf("raft[id=%d]: inflight snapshot counts=%d is less than zero ", r.config.LocalPeerId, inflight)
	}

	if err = r.storage.CompactAndReleaseSnapshot(snapCtx.Index(), metadata.Snapshot); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	return metadata, nil
}

func (r *Raft) triggerSnapshot(snapCtx InternalStorageSnapshotContext, snapshotResultCh chan snapshotResult) {
	r.status.SetSnapshotIndex(snapCtx.Index())
	doSnapshot := func() {
		metadata, err := r.doSnapshot(snapCtx)
		if err != nil {
			r.logger.Panicf("raft[id=%d]: do snapshot failed: %v", r.config.LocalPeerId, err)
		}
		r.logger.Infof("raft[id=%d]: do snapshot done (index=%d)", r.config.LocalPeerId, metadata.Snapshot.Metadata.Index)
		if snapshotResultCh != nil {
			snapshotResultCh <- snapshotResult{
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
