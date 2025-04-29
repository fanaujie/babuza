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
