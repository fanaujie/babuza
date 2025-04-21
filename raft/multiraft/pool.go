package multiraft

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

var (
	applyDataPool sync.Pool
)

type proposalData struct {
	replyID uint64
	data    []byte
}

type configChangeData struct {
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

func poolGetProposal() *proposalData {
	v := applyDataPool.Get()
	if v == nil {
		return &proposalData{}
	}

	return v.(*proposalData)
}

func poolReleaseProposal(value *proposalData) {
	value.replyID = 0
	value.data = nil
	applyDataPool.Put(value)
}

func poolGetConfigChange() *configChangeData {
	v := applyDataPool.Get()
	if v == nil {
		return &configChangeData{}
	}

	return v.(*configChangeData)
}

func poolReleaseConfigChange(value *configChangeData) {
	value.replyID = 0
	value.confChange = raftpb.ConfChange{}
	applyDataPool.Put(value)
}

func poolGetApplyEntry() *applyEntry {
	v := applyDataPool.Get()
	if v == nil {
		return &applyEntry{}
	}

	return v.(*applyEntry)
}

func poolReleaseApplyEntry(value *applyEntry) {
	value.entries = nil
	value.snapshot = raftpb.Snapshot{}
	applyDataPool.Put(value)
}

func poolGetConfChangeApplyJob() *confChangeApplyJob {
	v := applyDataPool.Get()
	if v == nil {
		return &confChangeApplyJob{}
	}

	return v.(*confChangeApplyJob)
}

func poolReleaseConfChangeApplyJob(value *confChangeApplyJob) {
	value.cc = nil
	value.resultCh = nil
	applyDataPool.Put(value)
}
