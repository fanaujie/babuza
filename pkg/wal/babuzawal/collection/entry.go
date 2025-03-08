package collection

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Entry struct {
	entries []raftpb.Entry
}

func NewEntry() *Entry {
	return &Entry{}
}

func (e *Entry) Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte,
	entryDataCapacity int64, r iwal.ReplayWalResult) error {

	nextEntry := r.NextEntry()
	entry := raftpb.Entry{
		Term:  nextEntry.NextTerm,
		Index: nextEntry.NextIndex,
		Type:  raftpb.EntryType(logType),
	}
	if len(logBuf) > 0 {
		entry.Data = make([]byte, len(logBuf), entryDataCapacity)
		copy(entry.Data, logBuf)
	}
	r.IncreaseNextIndex()
	if entry.Index > snapshotIndex {
		// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
		up := entry.Index - snapshotIndex - 1
		if up > uint64(len(e.entries)) {
			// return error before append call causes runtime panic
			return errors.New("")
		}
		// The line below is potentially overriding some 'uncommitted' termEntriesIndex.
		e.entries = append(e.entries[:up], entry)
	}
	return nil
}

func (e *Entry) Entries() (interface{}, error) {
	return e.entries, nil
}
func (e *Entry) ClearEntries() error {
	e.entries = nil
	return nil
}

func (e *Entry) VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error {
	var confEntries []raftpb.Entry
	for i := range e.entries {
		e := &e.entries[i]
		if e.Type == entryType {
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
func (e *Entry) DeleteUncommittedEntry(commitIndex uint64) error {
	var deleteFrom int
	entsLen := len(e.entries)
	for i := 0; i < entsLen; i++ {
		e := &e.entries[i]
		if e.Index > commitIndex {
			deleteFrom = i
			break
		}
	}
	if deleteFrom != 0 {
		e.entries = e.entries[:deleteFrom]
	}
	return nil
}
