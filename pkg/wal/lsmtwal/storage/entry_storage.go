package storage

import (
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type EntryMetadata struct {
}

type EntryStorage struct {
	*walbase.EntryStorage[EntryMetadata]
}

func (es *EntryStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return es.EntryStorage.InitialState()
}

func (es *EntryStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return es.EntryStorage.Entries(lo, hi, maxSize)
}

func (es *EntryStorage) Term(i uint64) (uint64, error) {
	return es.EntryStorage.Term(i)
}

func (es *EntryStorage) LastIndex() (uint64, error) {
	return es.EntryStorage.LastIndex()
}

func (es *EntryStorage) FirstIndex() (uint64, error) {
	return es.EntryStorage.FirstIndex()
}

func (es *EntryStorage) Snapshot() (raftpb.Snapshot, error) {
	return es.EntryStorage.Snapshot()
}

func (es *EntryStorage) SetHardState(state raftpb.HardState) error {
	return es.EntryStorage.SetHardState(state)
}

func (es *EntryStorage) Append(entries []raftpb.Entry) error {
	indexEntries := make([]walbase.EntryIndex[EntryMetadata], len(entries))
	for i, entry := range entries {
		indexEntries[i] = walbase.EntryIndex[EntryMetadata]{
			Term:     entry.Term,
			Index:    entry.Index,
			Type:     entry.Type,
			Metadata: EntryMetadata{},
		}
	}
	return es.EntryStorage.AppendEntryIndex(indexEntries)
}

func (es *EntryStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	return es.EntryStorage.ApplySnapshot(snapshot)
}

func (es *EntryStorage) CreateSnapshot(snapshotIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	return es.EntryStorage.CreateSnapshot(snapshotIndex, cs, data)
}

func (es *EntryStorage) Compact(compactIndex uint64) error {
	return es.EntryStorage.Compact(compactIndex)
}
