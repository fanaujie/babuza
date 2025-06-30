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


/*
Storage is compatible with interface of etcdwal memory storage.
So it must pass the test case of etcdwal memory storage.
The following test cases are from https://github.com/etcd-io/raft/blob/main/storage_test.go
TestStorageTerm
TestStorageEntries
TestStorageLastIndex
TestStorageFirstIndex
TestStorageCompact
TestStorageCreateSnapshot
TestStorageAppendIndex
*/

package walbase

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"math"
	"reflect"
	"testing"
)

type EntryMetadata struct {
	FileId       uint64
	Offset       int64
	DataLen      int64
	DataCapacity int64 // for boundary alignment
}

type mockReader struct {
}

func (m *mockReader) ReadEntriesData(readMetadata []EntryIndex[EntryMetadata], ents []raftpb.Entry) error {
	for i := 0; i < len(readMetadata); i++ {
		ents[i].Data = make([]byte, readMetadata[i].Metadata.DataLen)
		for j := int64(0); j < readMetadata[i].Metadata.DataLen; j++ {
			ents[i].Data[j] = 1
		}
	}
	return nil
}

func TestStorageTerm(t *testing.T) {
	ents := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	for i, tt := range tests {
		s := &EntryStorage[EntryMetadata]{ents: ents}

		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()

			term, err := s.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
	}
}

func TestStorageEntries(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	entsIndex := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5},
		{Index: 6, Term: 6}}
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []raftpb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		////limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}

	for i, tt := range tests {
		s := &EntryStorage[EntryMetadata]{
			cache:  NewCache(),
			ents:   entsIndex,
			reader: &mockReader{},
		}
		s.AppendCache(ents) //cache always hit
		entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}
	s := &EntryStorage[EntryMetadata]{ents: ents}

	last, err := s.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 5 {
		t.Errorf("last = %d, want %d", last, 5)
	}

	s.AppendEntryIndex([]EntryIndex[EntryMetadata]{{Index: 6, Term: 5}})
	last, err = s.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 6)
	}
}

func TestStorageFirstIndex(t *testing.T) {
	ents := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}
	s := &EntryStorage[EntryMetadata]{ents: ents}

	first, err := s.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}

	s.Compact(4)
	first, err = s.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}

func TestStorageCompact(t *testing.T) {
	ents := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, raft.ErrCompacted, 3, 3, 3},
		{3, raft.ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}

	for i, tt := range tests {
		s := &EntryStorage[EntryMetadata]{ents: ents}
		err := s.Compact(tt.i)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if s.ents[0].Index != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, s.ents[0].Index, tt.windex)
		}
		if s.ents[0].Term != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, s.ents[0].Term, tt.wterm)
		}
		if len(s.ents) != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(s.ents), tt.wlen)
		}
	}
}

func TestStorageCreateSnapshot(t *testing.T) {
	ents := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	data := []byte("data")

	tests := []struct {
		i uint64

		werr  error
		wsnap raftpb.Snapshot
	}{
		{4, nil, raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
		{5, nil, raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
	}

	for i, tt := range tests {
		s := &EntryStorage[EntryMetadata]{ents: ents}
		snap, err := s.CreateSnapshot(tt.i, cs, data)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(snap, tt.wsnap) {
			t.Errorf("#%d: snap = %+v, want %+v", i, snap, tt.wsnap)
		}
	}
}

func TestStorageAppendIndex(t *testing.T) {
	entsIndex := []EntryIndex[EntryMetadata]{
		{Index: 3, Term: 3},
		{Index: 4, Term: 4},
		{Index: 5, Term: 5}}

	tests := []struct {
		entries []EntryIndex[EntryMetadata]

		werr     error
		wentries []EntryIndex[EntryMetadata]
	}{
		{
			[]EntryIndex[EntryMetadata]{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]EntryIndex[EntryMetadata]{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// truncate the existing entries and append
		{
			[]EntryIndex[EntryMetadata]{{Index: 4, Term: 5}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			[]EntryIndex[EntryMetadata]{{Index: 6, Term: 5}},
			nil,
			[]EntryIndex[EntryMetadata]{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}

	for i, tt := range tests {
		s := &EntryStorage[EntryMetadata]{ents: entsIndex}
		err := s.AppendEntryIndex(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(s.ents, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, s.ents, tt.wentries)
		}
	}
}

func TestStorageEntriesCacheHit(t *testing.T) {
	ents := []raftpb.Entry{{Index: 7, Term: 7}, {Index: 8, Term: 8}, {Index: 9, Term: 9}, {Index: 10, Term: 10}}
	for i := 0; i < len(ents); i++ {
		ents[i].Data = make([]byte, ents[i].Index)
		for j := uint64(0); j < ents[i].Index; j++ {
			ents[i].Data[j] = 1
		}
	}
	var entsIndex []EntryIndex[EntryMetadata]
	for i := uint64(1); i <= 10; i++ {
		e := EntryIndex[EntryMetadata]{
			Index: i, Term: i, Metadata: EntryMetadata{DataLen: int64(i)}}
		entsIndex = append(entsIndex, e)
	}
	s := NewEntryStorage[EntryMetadata](&mockReader{})
	s.AppendEntryIndex(entsIndex)
	type testRange struct {
		start    uint64
		end      uint64
		readSize int
	}
	tc := []testRange{
		{start: 2, end: 3, readSize: 1},  //miss cache
		{start: 4, end: 7, readSize: 3},  //miss cache
		{start: 4, end: 8, readSize: 4},  //partial hit cache
		{start: 4, end: 9, readSize: 5},  //partial hit cache
		{start: 5, end: 7, readSize: 2},  //miss cache miss
		{start: 7, end: 11, readSize: 4}, //hit cache
	}
	for k, c := range tc {
		entries, err := s.Entries(c.start, c.end, math.MaxUint64)
		if err != nil {
			t.Error(err)
		}
		if len(entries) != c.readSize {
			t.Errorf("#case%d: expect size fo entries is 4, but real (%d)", k, len(entries))
		}
		for i, e := range entries {
			for j := uint64(0); j < e.Index; j++ {
				if e.Data[j] != 1 {
					t.Errorf("#case%d-%d-%d: entry data  expected 1 but real %v", k, i, j, e.Data[j])
				}
			}
		}
	}

}
