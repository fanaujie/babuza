package multiraft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type callbackProcessor struct {
	*Store
}

func (p *callbackProcessor) ProcessTick(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.ProcessTick()
	}
}

func (p *callbackProcessor) ProcessReady(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.ProcessReady()
	}
}

func (p *callbackProcessor) ProcessStep(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.ProcessStep()
	}
}

func (p *callbackProcessor) ProcessProposal(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.ProcessProposal()
	}
}

func (p *callbackProcessor) ProcessConfigChange(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.ProcessConfigChange()
	}
}

func (p *callbackProcessor) ApplyConfChange(groupID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	r, ok := p.replicaSet.Load(ibabuza.RaftGroupID(groupID))
	// mu already locked
	if ok {
		return r.mu.rawNode.ApplyConfChange(cc), nil
	}
	return nil, fmt.Errorf("store[%d] groupID[%d] not found", p.config.StoreID, groupID)
}
