package multiraft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (n *Node) ProcessTick(groupID ibabuza.RaftGroupID) {
	r, ok := n.replicaSet.Load(groupID)
	if ok {
		r.(*replica).ProcessTick()
	}
}

func (n *Node) ProcessReady(groupID ibabuza.RaftGroupID) {
	r, ok := n.replicaSet.Load(groupID)
	if ok {
		r.(*replica).ProcessReady()
	}
}

func (n *Node) ProcessStep(groupID ibabuza.RaftGroupID) {
	r, ok := n.replicaSet.Load(groupID)
	if ok {
		r.(*replica).ProcessStep()
	}
}

func (n *Node) ProcessProposal(groupID ibabuza.RaftGroupID) {
	r, ok := n.replicaSet.Load(groupID)
	if ok {
		r.(*replica).ProcessProposal()
	}
}

func (n *Node) ProcessConfigChange(groupID ibabuza.RaftGroupID) {
	r, ok := n.replicaSet.Load(groupID)
	if ok {
		r.(*replica).ProcessConfigChange()
	}
}

func (n *Node) ApplyConfChange(groupID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	r, ok := n.replicaSet.Load(ibabuza.RaftGroupID(groupID))
	// mu already locked
	if ok {
		return r.(*replica).mu.rawNode.ApplyConfChange(cc), nil
	}
	return nil, fmt.Errorf("node[%d] groupID[%d] not found", n.config.NodeID, groupID)
}
