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
