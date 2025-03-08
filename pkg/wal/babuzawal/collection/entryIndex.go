package collection

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/entrystore"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type EntryIndexReader interface {
	ReadEntriesData(readMetadata []entrystore.EntryIndex, ents []raftpb.Entry) error
}

type EntryIndex struct {
	entriesIndex []entrystore.EntryIndex
	reader       EntryIndexReader
}

func NewEntryIndex() *EntryIndex {
	return &EntryIndex{}
}

func (e *EntryIndex) Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte,
	entryDataCapacity int64, r iwal.ReplayWalResult) error {

	nextEntry := r.NextEntry()
	entry := entrystore.EntryIndex{
		Term:  nextEntry.NextTerm,
		Index: nextEntry.NextIndex,
		Type:  raftpb.EntryType(logType),
		EntryDataMetadata: entrystore.EntryDataMetadata{
			FileId:            fileId,
			EntryOffset:       r.LastValidLogOffset() + codec.HeaderSize,
			EntryDataLen:      int64(len(logBuf)),
			EntryDataCapacity: entryDataCapacity,
		},
	}
	r.IncreaseNextIndex()
	if entry.Index > snapshotIndex {
		// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
		up := entry.Index - snapshotIndex - 1
		if up > uint64(len(e.entriesIndex)) {
			// return error before append call causes runtime panic
			return errors.New("")
		}
		// The line below is potentially overriding some 'uncommitted' termEntriesIndex.
		e.entriesIndex = append(e.entriesIndex[:up], entry)
	}
	return nil
}

func (e *EntryIndex) Entries() (interface{}, error) {
	return e.entriesIndex, nil
}
func (e *EntryIndex) ClearEntries() error {
	e.entriesIndex = nil
	return nil
}

func (e *EntryIndex) VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error {
	if e.reader == nil {
		return errors.New("reader is nil")
	}
	var confEntries []raftpb.Entry
	var entriesIndex []entrystore.EntryIndex

	for i := range e.entriesIndex {
		ei := &e.entriesIndex[i]
		if ei.Type == entryType {
			confEntries = append(confEntries, raftpb.Entry{
				Term:  ei.Term,
				Index: ei.Index,
				Type:  ei.Type,
			})
			entriesIndex = append(entriesIndex, *ei)
		}
	}

	if err := e.reader.ReadEntriesData(entriesIndex, confEntries); err != nil {
		return err
	}
	for i := range confEntries {
		if err := visitor(confEntries[i]); err != nil {
			return err
		}
	}
	return nil
}
func (e *EntryIndex) DeleteUncommittedEntry(commitIndex uint64) error {
	var deleteFrom int
	entsLen := len(e.entriesIndex)
	for i := 0; i < entsLen; i++ {
		e := &e.entriesIndex[i]
		if e.Index > commitIndex {
			deleteFrom = i
			break
		}
	}
	if deleteFrom != 0 {
		e.entriesIndex = e.entriesIndex[:deleteFrom]
	}
	return nil
}

func (e *EntryIndex) SetReader(r EntryIndexReader) {
	e.reader = r
}
