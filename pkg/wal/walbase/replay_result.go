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


package walbase

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type ReplayResult struct {
	metadata  []byte
	hardState raftpb.HardState
	entries   []raftpb.Entry
}

func NewReplayResult(metadata []byte, hardState raftpb.HardState, entries []raftpb.Entry) *ReplayResult {
	return &ReplayResult{
		metadata:  metadata,
		hardState: hardState,
		entries:   entries,
	}
}

func (r *ReplayResult) Metadata() []byte {
	return r.metadata
}
func (r *ReplayResult) HardState() raftpb.HardState {
	return r.hardState
}

func (r *ReplayResult) GetEntries() []raftpb.Entry {
	return r.entries
}

func (r *ReplayResult) ForEachConfChangeEntries(visitor func(raftpb.Entry) error) error {
	var confEntries []raftpb.Entry
	for i := range r.entries {
		e := &r.entries[i]
		if e.Type == raftpb.EntryConfChange {
			confEntries = append(confEntries, *e)
		}
	}
	for i := range confEntries {
		if err := visitor(confEntries[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReplayResult) DeleteUncommittedEntry(commitIndex uint64) error {
	var deleteFrom int
	entsLen := len(r.entries)
	for i := 0; i < entsLen; i++ {
		e := &r.entries[i]
		if e.Index > commitIndex {
			deleteFrom = i
			break
		}
	}
	if deleteFrom != 0 {
		r.entries = r.entries[:deleteFrom]
	}
	return nil
}
