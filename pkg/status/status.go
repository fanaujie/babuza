package status

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync/atomic"
)

type LeaderInfo struct {
	NodeId   uint64
	RaftTerm uint64
}

type Status struct {
	hardStateTerm      uint64 //must use atomic operations to access; keep 64-bit aligned
	committedIndex     uint64 //must use atomic operations to access; keep 64-bit aligned
	appliedIndex       uint64 //must use atomic operations to access; keep 64-bit aligned
	appliedTerm        uint64 //must use atomic operations to access; keep 64-bit aligned
	snapIndex          uint64 //must use atomic operations to access; keep 64-bit aligned
	inflightSnapshots  int64  //must use atomic operations to access; keep 64-bit aligned
	acquireLeader      uint64 //must use atomic operations to access; keep 64-bit aligned
	publishServiceDone uint64 //must use atomic operations to access; keep 64-bit aligned
	softState          atomic.Value
	confState          atomic.Value
}

func New() *Status {
	s := &Status{}
	s.softState.Store(raft.SoftState{})
	s.confState.Store(raftpb.ConfState{})
	return s
}
func (s *Status) SetHardStateTerm(v uint64) {
	atomic.StoreUint64(&s.hardStateTerm, v)
}
func (s *Status) GetHardStateTerm() uint64 {
	return atomic.LoadUint64(&s.hardStateTerm)
}
func (s *Status) SetCommittedIndex(v uint64) {
	atomic.StoreUint64(&s.committedIndex, v)
}

func (s *Status) GetCommittedIndex() uint64 {
	return atomic.LoadUint64(&s.committedIndex)
}

func (s *Status) SetAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
}

func (s *Status) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *Status) SetAppliedTerm(v uint64) {
	atomic.StoreUint64(&s.appliedTerm, v)
}

func (s *Status) GetAppliedTerm() uint64 {
	return atomic.LoadUint64(&s.appliedTerm)
}

func (s *Status) SetSnapshotIndex(v uint64) {
	atomic.StoreUint64(&s.snapIndex, v)
}

func (s *Status) GetSnapshotIndex() uint64 {
	return atomic.LoadUint64(&s.snapIndex)
}
func (s *Status) AddInflightSnapshots(v int64) {
	atomic.AddInt64(&s.inflightSnapshots, v)
}

func (s *Status) GetInflightSnapshots() int64 {
	return atomic.LoadInt64(&s.inflightSnapshots)
}

func (s *Status) SetConfState(confState raftpb.ConfState) {
	s.confState.Store(confState)
}

func (s *Status) CloneConfState() raftpb.ConfState {
	return s.confState.Load().(raftpb.ConfState)
}
func (s *Status) SetSoftState(softState raft.SoftState) {
	s.softState.Store(softState)
}
func (s *Status) CloneSoftState() raft.SoftState {
	return s.softState.Load().(raft.SoftState)
}

func (s *Status) SetLeader(leader bool) {
	if leader {
		atomic.StoreUint64(&s.acquireLeader, 1)
	} else {
		atomic.StoreUint64(&s.acquireLeader, 0)
	}
}
func (s *Status) IsLeader() bool {
	return atomic.LoadUint64(&s.acquireLeader) == 1
}

func (s *Status) MarkPublishServiceDone() {
	atomic.StoreUint64(&s.publishServiceDone, 1)
}

func (s *Status) IsPublishServiceMarkDone() bool {
	return atomic.LoadUint64(&s.publishServiceDone) == 1
}
