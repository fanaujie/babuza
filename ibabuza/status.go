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
