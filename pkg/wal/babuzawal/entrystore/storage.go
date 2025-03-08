package entrystore

import (
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type EntryDataReader interface {
	ReadEntriesData(readMetadata []EntryIndex, ents []raftpb.Entry) error
}

type EntryDataMetadata struct {
	FileId            uint64
	EntryOffset       int64
	EntryDataLen      int64
	EntryDataCapacity int64 // for boundary alignment
}

type EntryIndex struct {
	Term  uint64
	Index uint64
	Type  raftpb.EntryType
	EntryDataMetadata
}

type Storage struct {
	hardState      raftpb.HardState
	snapshot       raftpb.Snapshot
	entryDataCache *Cache
	ents           []EntryIndex
	reader         EntryDataReader
	mu             sync.Mutex
}

func NewStorage(reader EntryDataReader) *Storage {
	return &Storage{
		entryDataCache: NewCache(),
		ents:           make([]EntryIndex, 1),
		reader:         reader,
	}
}

func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	firstEntryIndex, loOffset, hiOffset, err := s.validateGetEntriesRange(lo, hi)
	if err != nil {
		return nil, err
	}
	////TODO: avoid copy?
	copyEnts := make([]raftpb.Entry, hiOffset-loOffset)
	copyIndex := 0
	for i := loOffset; i < hiOffset; i++ {
		e := &s.ents[i]
		ce := &copyEnts[copyIndex]
		ce.Term = e.Term
		ce.Index = e.Index
		ce.Type = e.Type
		copyIndex++
	}
	cacheRange := s.entryDataCache.IndexRange()
	includedHiEntryIndex := firstEntryIndex + hiOffset - 1
	loEntryIndex := firstEntryIndex + loOffset

	if cacheRange.Empty || includedHiEntryIndex < cacheRange.First {
		// cache miss
		fmt.Printf("entryindex storage: cache miss low:%d hi:%d\n", lo, hi)
		if err = s.reader.ReadEntriesData(s.ents[loOffset:hiOffset], copyEnts); err != nil {
			return nil, err
		}
	} else {
		if loEntryIndex >= cacheRange.First && includedHiEntryIndex <= cacheRange.Last {
			//cache hit
			if false == s.entryDataCache.ReadEntriesData(copyEnts[:]) {
				return nil, errors.New("")
			}
		} else if loEntryIndex < cacheRange.First && includedHiEntryIndex >= cacheRange.First {
			//partial cache hit
			cacheFirstOffset, _ := cacheRange.ToOffsetIndex(firstEntryIndex)
			untilOffset := cacheFirstOffset - loOffset
			fmt.Printf("entryindex storage: partial cache hit low:%d hi:%d\n", lo, hi)
			if err = s.reader.ReadEntriesData(s.ents[loOffset:cacheFirstOffset], copyEnts[0:untilOffset]); err != nil {
				return nil, err
			}
			if false == s.entryDataCache.ReadEntriesData(copyEnts[untilOffset:]) {
				return nil, errors.New("")
			}
		} else {
			panic("")
		}
	}
	return limitSize(copyEnts, maxSize), nil
}

func (s *Storage) Term(term uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.ents[0].Index
	if term < offset {
		return 0, raft.ErrCompacted
	}
	if int(term-offset) >= len(s.ents) {
		return 0, raft.ErrUnavailable
	}
	return s.ents[term-offset].Term, nil
}

func (s *Storage) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndex(), nil
}

func (s *Storage) lastIndex() uint64 {
	return s.ents[0].Index + uint64(len(s.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstIndex(), nil
}

func (s *Storage) firstIndex() uint64 {
	return s.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, nil
}

func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	//handle check for old snapshot being applied
	msIndex := s.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return raft.ErrSnapOutOfDate
	}

	s.snapshot = snap
	s.entryDataCache.Clear()
	//TODO: GC?
	s.ents = []EntryIndex{
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
func (s *Storage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i <= s.snapshot.Metadata.Index {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}

	offset := s.ents[0].Index
	if i > s.lastIndex() {
		panic(fmt.Sprintf("snapshot %d is out of bound lastindex(%d)", i, s.lastIndex()))
	}

	s.snapshot.Metadata.Index = i
	s.snapshot.Metadata.Term = s.ents[i-offset].Term
	if cs != nil {
		s.snapshot.Metadata.ConfState = *cs
	}
	s.snapshot.Data = data
	return s.snapshot, nil
}

// Compact discards all log Entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an Index
// greater than raftLog.applied.
func (s *Storage) Compact(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.ents[0].Index
	if compactIndex <= offset {
		return raft.ErrCompacted
	}
	if compactIndex > s.lastIndex() {
		//getLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, s.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]EntryIndex, 1, 1+uint64(len(s.ents))-i)
	ents[0].Index = s.ents[i].Index
	ents[0].Term = s.ents[i].Term
	ents = append(ents, s.ents[i+1:]...)
	s.ents = ents

	return nil
}

func (s *Storage) Append(ents []raftpb.Entry) error {
	//not implemented
	return nil
}

func (s *Storage) AppendEntryIndex(entriesIndex []EntryIndex) error {
	if len(entriesIndex) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	first := s.firstIndex()
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
	offset := entriesIndex[0].Index - s.ents[0].Index
	switch {
	case uint64(len(s.ents)) > offset:
		s.ents = append([]EntryIndex{}, s.ents[:offset]...)
		s.ents = append(s.ents, entriesIndex...)
	case uint64(len(s.ents)) == offset:
		s.ents = append(s.ents, entriesIndex...)
	default:
		return errors.New(fmt.Sprintf("missing log entry [appendPos: %d, append at: %d]",
			s.lastIndex(), entriesIndex[0].Index))
	}
	return nil
}

func (s *Storage) SetHardState(st raftpb.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = st
	return nil
}

func (s *Storage) AppendCache(ents []raftpb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//TODO: how to process if ents does not continue entry index for cache? 只有保存還沒commit的entry
	s.entryDataCache.Append(ents)
}

func (s *Storage) DeleteCache(commitIndex uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entryDataCache.Delete(commitIndex)
}

func (s *Storage) EntryIndex() []EntryIndex {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]EntryIndex, len(s.ents)-1)
	copy(r, s.ents[1:])
	return r
}

func (s *Storage) validateGetEntriesRange(lo, hi uint64) (uint64, uint64, uint64, error) {
	firstEntIndex := s.ents[0].Index
	if lo <= firstEntIndex {
		return 0, 0, 0, raft.ErrCompacted
	}
	if hi > s.lastIndex()+1 {
		return 0, 0, 0, errors.New(fmt.Sprintf("entries' hi(%d) is out of bound lastindex(%d)", hi, s.lastIndex()))
	}
	// only contains dummy Entries.
	if len(s.ents) == 1 {
		return 0, 0, 0, raft.ErrUnavailable
	}
	return firstEntIndex, lo - firstEntIndex, hi - firstEntIndex, nil
}

//func (s *Storage) copyEntries(lo, hi uint64, maxSize int64) ([]raftpb.Entry, uint64, uint64, error) {
//	offset := s.ents[0].Index
//	if lo <= offset {
//		return nil, 0, 0, raft.ErrCompacted
//	}
//	if hi > s.lastIndex()+1 {
//		return nil, 0, 0, errors.New(fmt.Sprintf("entries' hi(%d) is out of bound lastindex(%d)", hi, s.lastIndex()))
//	}
//	// only contains dummy Entries.
//	if len(s.ents) == 1 {
//		return nil, 0, 0, raft.ErrUnavailable
//	}
//	startIndex := lo - offset
//	endIndex := hi - offset
//	size := int64(0)
//	limitIndex := startIndex
//	for ; limitIndex < endIndex; limitIndex++ {
//		e := &s.ents[limitIndex]
//		size += e.EntryDataLen
//		if size > maxSize {
//			break
//		}
//	}
//	if limitIndex == startIndex {
//		return nil, 0, 0, errors.New("")
//	}
//	outEntries := make([]raftpb.Entry, 0, limitIndex-startIndex)
//	for i := startIndex; i < limitIndex; i++ {
//		e := &s.ents[i]
//		outEntries = append(outEntries, raftpb.Entry{
//			Term:  e.Term,
//			Index: e.Index,
//			Type:  e.Type,
//		})
//	}
//	return outEntries, startIndex, limitIndex, nil
//}

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
