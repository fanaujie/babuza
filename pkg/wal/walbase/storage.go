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
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type EntryIndex[T any] struct {
	Term     uint64
	Index    uint64
	Type     raftpb.EntryType
	Metadata T
}

type EntryDataReader[T any] interface {
	ReadEntriesData(readMetadata []EntryIndex[T], ents []raftpb.Entry) error
}

type EntryStorage[T any] struct {
	hardState raftpb.HardState
	snapshot  raftpb.Snapshot
	cache     *Cache
	ents      []EntryIndex[T]
	reader    EntryDataReader[T]
	mu        sync.Mutex
}

func NewEntryStorage[T any](reader EntryDataReader[T]) *EntryStorage[T] {
	return &EntryStorage[T]{
		cache:  NewCache(),
		ents:   make([]EntryIndex[T], 1),
		reader: reader,
	}
}

func (es *EntryStorage[T]) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return es.hardState, es.snapshot.Metadata.ConfState, nil
}

func (es *EntryStorage[T]) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	firstEntryIndex, loOffset, hiOffset, err := es.validateGetEntriesRange(lo, hi)
	if err != nil {
		return nil, err
	}
	copyEnts := make([]raftpb.Entry, hiOffset-loOffset)
	copyIndex := 0
	for i := loOffset; i < hiOffset; i++ {
		e := &es.ents[i]
		ce := &copyEnts[copyIndex]
		ce.Term = e.Term
		ce.Index = e.Index
		ce.Type = e.Type
		copyIndex++
	}
	cacheRange := es.cache.IndexRange()
	includedHiEntryIndex := firstEntryIndex + hiOffset - 1
	loEntryIndex := firstEntryIndex + loOffset

	if cacheRange.Empty || includedHiEntryIndex < cacheRange.First {
		// cache miss
		fmt.Printf("entryindex storage: cache miss low:%d hi:%d\n", lo, hi)
		if err = es.reader.ReadEntriesData(es.ents[loOffset:hiOffset], copyEnts); err != nil {
			return nil, err
		}
	} else {
		if loEntryIndex >= cacheRange.First && includedHiEntryIndex <= cacheRange.Last {
			//cache hit
			if false == es.cache.ReadEntriesData(copyEnts[:]) {
				return nil, errors.New("")
			}
		} else if loEntryIndex < cacheRange.First && includedHiEntryIndex >= cacheRange.First {
			//partial cache hit
			cacheFirstOffset, _ := cacheRange.ToOffsetIndex(firstEntryIndex)
			untilOffset := cacheFirstOffset - loOffset
			fmt.Printf("entryindex storage: partial cache hit low:%d hi:%d\n", lo, hi)
			if err = es.reader.ReadEntriesData(es.ents[loOffset:cacheFirstOffset], copyEnts[0:untilOffset]); err != nil {
				return nil, err
			}
			if false == es.cache.ReadEntriesData(copyEnts[untilOffset:]) {
				return nil, errors.New("")
			}
		} else {
			panic("")
		}
	}
	return limitSize(copyEnts, maxSize), nil
}

func (es *EntryStorage[T]) Term(term uint64) (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	offset := es.ents[0].Index
	if term < offset {
		return 0, raft.ErrCompacted
	}
	if int(term-offset) >= len(es.ents) {
		return 0, raft.ErrUnavailable
	}
	return es.ents[term-offset].Term, nil
}

func (es *EntryStorage[T]) LastIndex() (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.lastIndex(), nil
}

func (es *EntryStorage[T]) lastIndex() uint64 {
	return es.ents[0].Index + uint64(len(es.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (es *EntryStorage[T]) FirstIndex() (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.firstIndex(), nil
}

func (es *EntryStorage[T]) firstIndex() uint64 {
	return es.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (es *EntryStorage[T]) Snapshot() (raftpb.Snapshot, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.snapshot, nil
}

func (es *EntryStorage[T]) ApplySnapshot(snap raftpb.Snapshot) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	//handle check for old snapshot being applied
	currentSnapshotIndex := es.snapshot.Metadata.Index
	newSnapshotIndex := snap.Metadata.Index
	if currentSnapshotIndex >= newSnapshotIndex {
		return raft.ErrSnapOutOfDate
	}

	es.snapshot = snap
	es.cache.Clear()
	es.ents = []EntryIndex[T]{
		{
			Term:  snap.Metadata.Term,
			Index: snap.Metadata.Index,
		},
	}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the appendPos compaction,
// the result of the appendPos ApplyConfChange must be passed in.
func (es *EntryStorage[T]) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	if i <= es.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}

	offset := es.ents[0].Index
	if i > es.lastIndex() {
		panic(fmt.Sprintf("snapshot %d is out of bound lastindex(%d)", i, es.lastIndex()))
	}

	es.snapshot.Metadata.Index = i
	es.snapshot.Metadata.Term = es.ents[i-offset].Term
	if cs != nil {
		es.snapshot.Metadata.ConfState = *cs
	}
	es.snapshot.Data = data
	return es.snapshot, nil
}

// Compact discards all log Entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an Index
// greater than raftLog.applied.
func (es *EntryStorage[T]) Compact(compactIndex uint64) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	offset := es.ents[0].Index
	if compactIndex <= offset {
		return raft.ErrCompacted
	}
	if compactIndex > es.lastIndex() {
		//getLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, s.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]EntryIndex[T], 1, 1+uint64(len(es.ents))-i)
	ents[0].Index = es.ents[i].Index
	ents[0].Term = es.ents[i].Term
	ents = append(ents, es.ents[i+1:]...)
	es.ents = ents

	return nil
}

func (es *EntryStorage[T]) AppendEntryIndex(entriesIndex []EntryIndex[T]) error {
	if len(entriesIndex) == 0 {
		return nil
	}
	es.mu.Lock()
	defer es.mu.Unlock()

	first := es.firstIndex()
	last := entriesIndex[0].Index + uint64(len(entriesIndex)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted Entries
	if first > entriesIndex[0].Index {
		truncateIndex := first - entriesIndex[0].Index
		entriesIndex = entriesIndex[truncateIndex:]
	}
	offset := entriesIndex[0].Index - es.ents[0].Index
	switch {
	case uint64(len(es.ents)) > offset:
		es.ents = append([]EntryIndex[T]{}, es.ents[:offset]...)
		es.ents = append(es.ents, entriesIndex...)
	case uint64(len(es.ents)) == offset:
		es.ents = append(es.ents, entriesIndex...)
	default:
		return errors.New(fmt.Sprintf("missing log entry [appendPos: %d, append at: %d]",
			es.lastIndex(), entriesIndex[0].Index))
	}
	return nil
}

func (es *EntryStorage[T]) SetHardState(st raftpb.HardState) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.hardState = st
	return nil
}

func (es *EntryStorage[T]) AppendCache(ents []raftpb.Entry) {
	es.mu.Lock()
	defer es.mu.Unlock()
	//TODO: how to process if ents does not continue entry index for cache?
	es.cache.Append(ents)
}

func (es *EntryStorage[T]) DeleteCache(commitIndex uint64) {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.cache.Delete(commitIndex)
}

func (es *EntryStorage[T]) EntryIndex() []EntryIndex[T] {
	es.mu.Lock()
	defer es.mu.Unlock()
	r := make([]EntryIndex[T], len(es.ents)-1)
	copy(r, es.ents[1:])
	return r
}

func (es *EntryStorage[T]) validateGetEntriesRange(lo, hi uint64) (uint64, uint64, uint64, error) {
	firstEntIndex := es.ents[0].Index
	if lo <= firstEntIndex {
		return 0, 0, 0, raft.ErrCompacted
	}
	if hi > es.lastIndex()+1 {
		return 0, 0, 0, errors.New(fmt.Sprintf("entries' hi(%d) is out of bound lastindex(%d)", hi, es.lastIndex()))
	}
	// only contains dummy Entries.
	if len(es.ents) == 1 {
		return 0, 0, 0, raft.ErrUnavailable
	}
	return firstEntIndex, lo - firstEntIndex, hi - firstEntIndex, nil
}

func limitSize(ents []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}
