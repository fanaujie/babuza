package ibabuza

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Status interface {
	SetHardStateTerm(v uint64)
	GetHardStateTerm() uint64
	SetCommittedIndex(v uint64)
	GetCommittedIndex() uint64
	SetAppliedIndex(v uint64)
	GetAppliedIndex() uint64
	SetAppliedTerm(v uint64)
	GetAppliedTerm() uint64
	SetSnapshotIndex(v uint64)
	GetSnapshotIndex() uint64
	AddInflightSnapshots(v int64)
	GetInflightSnapshots() int64
	SetConfState(confState raftpb.ConfState)
	CloneConfState() raftpb.ConfState
	SetSoftState(softState raft.SoftState)
	CloneSoftState() raft.SoftState
	SetLeader(leader bool)
	IsLeader() bool
}
