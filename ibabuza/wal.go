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
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type Wal interface {
	SetUnsafeNoFsync()
	Save(raftpb.HardState, []raftpb.Entry) error
	SaveSnapshot(raftpb.Snapshot) error
	Purge(raftpb.Snapshot) error
	Sync() error
	Close() error
}

type ReplayWalResult interface {
	Metadata() []byte
	HardState() raftpb.HardState
	ForEachConfChangeEntries(func(raftpb.Entry) error) error
}

type WalPurger interface {
	Start()
}

type WalManager interface {
	FindSnapshot() ([]walpb.Snapshot, error)
	CreateWal(metadata babuzapb.WalMetadata) (EntryStorage, Wal, error)
	ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (ReplayWalResult, EntryStorage, Wal, error)
	HasExistingWals() (bool, error)
	Purger() WalPurger
	Close() error
}

type EntryStorage interface {
	raft.Storage
	SetHardState(raftpb.HardState) error
	Append([]raftpb.Entry) error
	ApplySnapshot(raftpb.Snapshot) error
	CreateSnapshot(snapshotIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(compactIndex uint64) error
}
