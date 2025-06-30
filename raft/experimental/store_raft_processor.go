// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
