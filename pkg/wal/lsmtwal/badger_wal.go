package lsmtwal

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type BadgerWal struct {
	db               *badger.DB
	es               *storage.EntryStorage
	noFsync          bool
	keyPrefix        *keyPrefix
	hardState        raftpb.HardState
	purgerSnapCh     chan purgeRequest
	multiRaftGroupID ibabuza.RaftGroupID
}

func NewBadgerWal(db *badger.DB, es *storage.EntryStorage, keyPrefix *keyPrefix, purgerSnapCh chan purgeRequest) *BadgerWal {

	return &BadgerWal{
		db:           db,
		es:           es,
		keyPrefix:    keyPrefix,
		purgerSnapCh: purgerSnapCh,
	}
}

func (w *BadgerWal) SetUnsafeNoFsync() {
	w.noFsync = true
}

func (w *BadgerWal) SetMultiRaftPurger(groupID ibabuza.RaftGroupID) {
	w.multiRaftGroupID = groupID
}

func (w *BadgerWal) Save(hardState raftpb.HardState, entries []raftpb.Entry) error {
	wb := w.db.NewWriteBatch()
	defer wb.Cancel()

	if !isEmptyHardState(hardState) {
		w.hardState = hardState
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

	if raft.MustSync(hardState, w.hardState, len(entries)) == true {
		if err := w.Sync(); err != nil {
			return err
		}
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
	if w.purgerSnapCh != nil {
		w.purgerSnapCh <- purgeRequest{
			groupID:  w.multiRaftGroupID,
			snapshot: snapshot,
		}
	}
	return nil
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
