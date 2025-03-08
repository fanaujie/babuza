package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type snapshotContext struct {
	term                uint64
	index               uint64
	snapWr              ibabuza.AtomicSnapshotWriter
	confState           raftpb.ConfState
	stateMachineContext interface{}
}

func (s *snapshotContext) Term() uint64 {
	return s.term
}

func (s *snapshotContext) Index() uint64 {
	return s.index
}

func (s *snapshotContext) StateMachineSnapshotContext() ibabuza.StateMachineSnapshotContext {
	return s.stateMachineContext
}

func (s *snapshotContext) AtomicSnapshotWriter() ibabuza.AtomicSnapshotWriter {
	return s.snapWr
}

func (s *snapshotContext) ConfState() *raftpb.ConfState {
	return &s.confState
}
