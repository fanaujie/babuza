package lsmtwal

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type BadgerWal struct {
	db        *badger.DB
	es        *storage.EntryStorage
	noFsync   bool
	keyPrefix *keyPrefix
}

func NewBadgerWal(db *badger.DB, es *storage.EntryStorage, keyPrefix *keyPrefix) *BadgerWal {

	return &BadgerWal{
		db:        db,
		es:        es,
		keyPrefix: keyPrefix,
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
		if err = wb.Set(w.keyPrefix.hardState, data); err != nil {
			return err
		}
	}

	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return err
		}
		var key [24]byte
		copy(key[:16], w.keyPrefix.entry)
		binary.BigEndian.PutUint64(key[16:], entry.Index)
		if err = wb.Set(key[:24], data); err != nil {
			return err
		}
	}

	if err := wb.Flush(); err != nil {
		return err
	}

	if len(entries) > 0 && w.es != nil {
		w.es.AppendCache(entries)
	}

	return nil
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
		var key [24]byte
		copy(key[:], w.keyPrefix.snapshot)
		binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)
		return txn.Set(key[:24], data)
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

		// Prepare seek key with the snapshot index
		var snapshotIndex [24]byte
		copy(snapshotIndex[:16], w.keyPrefix.entry)
		binary.BigEndian.PutUint64(snapshotIndex[16:], snapshot.Metadata.Index)
		for it.Seek(snapshotIndex[:24]); it.ValidForPrefix(w.keyPrefix.entry); it.Next() {
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
	return nil
}

func isEmptyHardState(st raftpb.HardState) bool {
	return st.Term == 0 && st.Vote == 0 && st.Commit == 0
}

func isEmptySnapshot(snap raftpb.Snapshot) bool {
	return snap.Metadata.Index == 0
}
