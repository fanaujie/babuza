package etcdwal

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type WalWrapper struct {
	*wal.WAL
}

func NewWalWrapper(w *wal.WAL) *WalWrapper {
	return &WalWrapper{
		WAL: w,
	}
}

func (w *WalWrapper) SaveSnapshot(snap raftpb.Snapshot) error {
	return w.WAL.SaveSnapshot(walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	})
}
func (w *WalWrapper) Purge(snap raftpb.Snapshot) error {
	return w.WAL.ReleaseLockTo(snap.Metadata.Index)
}
