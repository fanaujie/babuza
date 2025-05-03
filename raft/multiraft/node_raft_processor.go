package multiraft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
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

func (n *Node) ProcessRaftStatus(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		r.ProcessRaftStatus()
	}
}

func (n *Node) ApplyConfChange(groupID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	gid := ibabuza.RaftGroupID(groupID)
	n.replicaSet.mu.RLock()
	r, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		return r.rawNode.ApplyConfChange(cc), nil
	}
	return nil, fmt.Errorf("node[%d] groupID[%d] not found", n.config.NodeID, groupID)
}
