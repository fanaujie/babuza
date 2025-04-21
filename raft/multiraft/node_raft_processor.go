package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/raft/multiraft/shard"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (n *Node) ProcessTick(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessTick()
	}
}

func (n *Node) ProcessReady(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessReady()
	}
}

func (n *Node) ProcessStep(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessStep()
	}
}

func (n *Node) ProcessProposal(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessProposal()
	}
}

func (n *Node) ProcessConfigChange(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessConfigChange()
	}
}

func (n *Node) ProcessApplyConfChange(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessApplyConfChange()
	}
}

func (n *Node) ApplyConfChange(clusterID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	gid := ibabuza.RaftGroupID(clusterID)
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		if err := n.scheduler.EnqueueState(gid, shard.StateApplyConfChange); err != nil {
			return nil, err
		}
		resultCh := make(chan *raftpb.ConfState, 1)
		ccApplyJob := poolGetConfChangeApplyJob()
		ccApplyJob.cc = cc
		ccApplyJob.resultCh = resultCh
		r.EnqueueApplyConfChange(ccApplyJob)
		result := <-resultCh
		return result, nil
	}
	return nil, errors.Errorf("raft group %d not found", gid)
}
