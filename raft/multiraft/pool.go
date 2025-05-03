package multiraft

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
