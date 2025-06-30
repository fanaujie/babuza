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
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

var (
	proposalPool   sync.Pool
	applyEntryPool sync.Pool
)

type proposalRequest struct {
	replyID uint64
	data    []byte
}

type configChangeRequest struct {
	replyID    uint64
	confChange raftpb.ConfChange
}

type applyEntry struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
}

type confChangeApplyJob struct {
	cc       raftpb.ConfChangeI
	resultCh chan *raftpb.ConfState
}

func poolGetProposal() *proposalRequest {
	v := proposalPool.Get()
	if v == nil {
		return &proposalRequest{}
	}

	return v.(*proposalRequest)
}

func poolReleaseProposal(value *proposalRequest) {
	value.replyID = 0
	value.data = nil
	proposalPool.Put(value)
}

func poolGetApplyEntry() *applyEntry {
	v := applyEntryPool.Get()
	if v == nil {
		return &applyEntry{}
	}

	return v.(*applyEntry)
}

func poolReleaseApplyEntry(value *applyEntry) {
	value.entries = nil
	value.snapshot = raftpb.Snapshot{}
	applyEntryPool.Put(value)
}
