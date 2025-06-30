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


package entrycollection

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	ErrNotImplementedOp = errors.New("not implemented operation")
)

type NopEntryStore struct {
}

func NewNopEntry() *NopEntryStore {
	return &NopEntryStore{}
}

func (e *NopEntryStore) Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte,
	entryDataCapacity int64, r iwal.ReplayWalResult) error {
	return ErrNotImplementedOp
}

func (e *NopEntryStore) Entries() (interface{}, error) {
	return nil, ErrNotImplementedOp

}
func (e *NopEntryStore) ClearEntries() error {
	return ErrNotImplementedOp
}

func (e *NopEntryStore) VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error {
	return ErrNotImplementedOp
}
func (e *NopEntryStore) DeleteUncommittedEntry(commitIndex uint64) error {
	return ErrNotImplementedOp
}
