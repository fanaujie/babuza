package experimental

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
		r.processTick()
	}
}

func (p *callbackProcessor) ProcessReady(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if ok {
		r.processReady()
	}
}

func (p *callbackProcessor) ProcessStep(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if !ok {
		return
	}
	requestQueue, ok := p.requestQueues.Load(groupID)
	if ok {
		r.processStep(requestQueue)
	}
}

func (p *callbackProcessor) ProcessProposal(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if !ok {
		return
	}
	requestQueue, ok := p.requestQueues.Load(groupID)
	if ok {
		r.processProposal(requestQueue)
	}
}

func (p *callbackProcessor) ProcessConfigChange(groupID ibabuza.RaftGroupID) {
	r, ok := p.replicaSet.Load(groupID)
	if !ok {
		return
	}
	requestQueue, ok := p.requestQueues.Load(groupID)
	if ok {
		r.ProcessConfigChange(requestQueue)
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
