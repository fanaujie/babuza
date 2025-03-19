package lsmtwal

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"time"
)

const (
	keyHardState = "hard" // Hard state
	keySnapshot  = "snap" // Snapshot
	keyEntry     = "entry"
	keyMetadata  = "metadata" // Entry
)

type BadgerWal struct {
	db      *badger.DB
	noFsync bool
	stopCh  chan struct{}
}

func NewBadgerWal(db *badger.DB) *BadgerWal {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
			again:
				err := db.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
			}
		}
	}()
	return &BadgerWal{
		db:     db,
		stopCh: stopCh,
	}
}

func (w *BadgerWal) SetUnsafeNoFsync() {
	w.noFsync = true
}

func (w *BadgerWal) Save(hardState raftpb.HardState, entries []raftpb.Entry) error {
	wb := w.db.NewWriteBatch()
	defer wb.Cancel()

	if !isEmptyHardState(hardState) {
		data, err := hardState.Marshal()
		if err != nil {
			return err
		}
		if err = wb.Set([]byte(keyHardState), data); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		key := make([]byte, 16)
		copy(key, keyEntry)
		binary.BigEndian.PutUint64(key[8:], entry.Index)
		if err = wb.Set(key, data); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (w *BadgerWal) SaveSnapshot(snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}
	walsnap := walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
	data, err := walsnap.Marshal()
	if err != nil {
		return err
	}

	return w.db.Update(func(txn *badger.Txn) error {
		key := make([]byte, 16)
		copy(key, keySnapshot)
		binary.BigEndian.PutUint64(key[8:], snapshot.Metadata.Index)
		return txn.Set(key, data)
	})
}

func (w *BadgerWal) Purge(snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}

	// Delete entries that are included in the snapshot
	return w.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Collect keys to delete
		var keysToDelete [][]byte
		snapshotIndex := make([]byte, 16)
		copy(snapshotIndex, keyEntry)
		binary.BigEndian.PutUint64(snapshotIndex[8:], snapshot.Metadata.Index)
		for it.Seek(snapshotIndex); it.ValidForPrefix(snapshotIndex[:8]); it.Next() {
			item := it.Item()
			key := item.Key()
			keysToDelete = append(keysToDelete, append([]byte{}, key...))
		}
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (w *BadgerWal) Sync() error {
	if !w.noFsync {
		if w.db.Opts().InMemory {
			return nil
		}
		return w.db.Sync()
	}
	return nil
}

func (w *BadgerWal) Close() error {
	close(w.stopCh)
	return w.db.Close()
}

func isEmptyHardState(st raftpb.HardState) bool {
	return st.Term == 0 && st.Vote == 0 && st.Commit == 0
}

func isEmptySnapshot(snap raftpb.Snapshot) bool {
	return snap.Metadata.Index == 0
}
