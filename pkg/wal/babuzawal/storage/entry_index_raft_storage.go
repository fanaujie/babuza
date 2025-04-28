package storage

import (
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type EntryIndexMetadata struct {
	FileId       uint64
	Offset       int64
	DataLen      int64
	DataCapacity int64 // for boundary alignment
}

type EntryIndexRaftStorage struct {
	*walbase.EntryStorage[EntryIndexMetadata]
}

func (es *EntryIndexRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return es.EntryStorage.InitialState()
}

func (es *EntryIndexRaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return es.EntryStorage.Entries(lo, hi, maxSize)
}

func (es *EntryIndexRaftStorage) Term(i uint64) (uint64, error) {
	return es.EntryStorage.Term(i)
}

func (es *EntryIndexRaftStorage) LastIndex() (uint64, error) {
	return es.EntryStorage.LastIndex()
}

func (es *EntryIndexRaftStorage) FirstIndex() (uint64, error) {
	return es.EntryStorage.FirstIndex()
}

func (es *EntryIndexRaftStorage) Snapshot() (raftpb.Snapshot, error) {
	return es.EntryStorage.Snapshot()
}

func (es *EntryIndexRaftStorage) SetHardState(state raftpb.HardState) error {
	return es.EntryStorage.SetHardState(state)
}

func (es *EntryIndexRaftStorage) Append(entries []raftpb.Entry) error {
	// not implemented
	return nil
}

func (es *EntryIndexRaftStorage) AppendEntryIndex(entries []walbase.EntryIndex[EntryIndexMetadata]) error {
	return es.EntryStorage.AppendEntryIndex(entries)
}

func (es *EntryIndexRaftStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	return es.EntryStorage.ApplySnapshot(snapshot)
}

func (es *EntryIndexRaftStorage) CreateSnapshot(snapshotIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	return es.EntryStorage.CreateSnapshot(snapshotIndex, cs, data)
}

func (es *EntryIndexRaftStorage) Compact(compactIndex uint64) error {
	return es.EntryStorage.Compact(compactIndex)
}
