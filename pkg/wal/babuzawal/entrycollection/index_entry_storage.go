package entrycollection

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type EntryIndexReader interface {
	ReadEntriesData(readMetadata []walbase.EntryIndex[storage.EntryIndexMetadata], ents []raftpb.Entry) error
}

type IndexedEntryStore struct {
	entriesIndex []walbase.EntryIndex[storage.EntryIndexMetadata]
	reader       EntryIndexReader
}

func NewIndexedEntryStore() *IndexedEntryStore {
	return &IndexedEntryStore{}
}

func (ei *IndexedEntryStore) Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte,
	entryDataCapacity int64, r iwal.ReplayWalResult) error {

	nextEntry := r.NextEntry()
	entry := walbase.EntryIndex[storage.EntryIndexMetadata]{
		Term:  nextEntry.NextTerm,
		Index: nextEntry.NextIndex,
		Type:  raftpb.EntryType(logType),
		Metadata: storage.EntryIndexMetadata{
			FileId:       fileId,
			Offset:       r.LastValidLogOffset() + codec.HeaderSize,
			DataLen:      int64(len(logBuf)),
			DataCapacity: entryDataCapacity,
		},
	}

	if entry.Index > snapshotIndex {
		// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
		up := entry.Index - snapshotIndex - 1
		if up > uint64(len(ei.entriesIndex)) {
			// return error before append call causes runtime panic
			return errors.New("")
		}
		// The line below is potentially overriding some 'uncommitted' termEntriesIndex.
		ei.entriesIndex = append(ei.entriesIndex[:up], entry)
	}
	r.SetNextIndex(entry.Index + 1)
	return nil
}

func (ei *IndexedEntryStore) Entries() (interface{}, error) {
	return ei.entriesIndex, nil
}
func (ei *IndexedEntryStore) ClearEntries() error {
	ei.entriesIndex = nil
	return nil
}

func (ei *IndexedEntryStore) VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error {
	if ei.reader == nil {
		return errors.New("reader is nil")
	}
	var confEntries []raftpb.Entry
	var entriesIndex []walbase.EntryIndex[storage.EntryIndexMetadata]

	for i := range ei.entriesIndex {
		e := &ei.entriesIndex[i]
		if e.Type == entryType {
			confEntries = append(confEntries, raftpb.Entry{
				Term:  e.Term,
				Index: e.Index,
				Type:  e.Type,
			})
			entriesIndex = append(entriesIndex, *e)
		}
	}

	if err := ei.reader.ReadEntriesData(entriesIndex, confEntries); err != nil {
		return err
	}
	for i := range confEntries {
		if err := visitor(confEntries[i]); err != nil {
			return err
		}
	}
	return nil
}
func (ei *IndexedEntryStore) DeleteUncommittedEntry(commitIndex uint64) error {
	var deleteFrom int
	entsLen := len(ei.entriesIndex)
	for i := 0; i < entsLen; i++ {
		e := &ei.entriesIndex[i]
		if e.Index > commitIndex {
			deleteFrom = i
			break
		}
	}
	if deleteFrom != 0 {
		ei.entriesIndex = ei.entriesIndex[:deleteFrom]
	}
	return nil
}

func (ei *IndexedEntryStore) SetReader(r EntryIndexReader) {
	ei.reader = r
}
