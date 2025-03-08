package collection

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	ErrNotImplementedOp = errors.New("")
)

type NopEntry struct {
}

func NewNopEntry() *NopEntry {
	return &NopEntry{}
}

func (e *NopEntry) Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte,
	entryDataCapacity int64, r iwal.ReplayWalResult) error {
	return ErrNotImplementedOp
}

func (e *NopEntry) Entries() (interface{}, error) {
	return nil, ErrNotImplementedOp

}
func (e *NopEntry) ClearEntries() error {
	return ErrNotImplementedOp
}

func (e *NopEntry) VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error {
	return ErrNotImplementedOp
}
func (e *NopEntry) DeleteUncommittedEntry(commitIndex uint64) error {
	return ErrNotImplementedOp
}
