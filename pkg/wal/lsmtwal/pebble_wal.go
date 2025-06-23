package lsmtwal

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble/v2"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type PebbleWal struct {
	db               *pebble.DB
	es               *storage.EntryStorage
	noFsync          bool
	keyPrefix        *keyPrefix
	hardState        raftpb.HardState
	purgerSnapCh     chan purgeRequest
	multiRaftGroupID ibabuza.RaftGroupID
}

func NewPebbleWal(db *pebble.DB, es *storage.EntryStorage, keyPrefix *keyPrefix, purgerSnapCh chan purgeRequest) *PebbleWal {
	return &PebbleWal{
		db:           db,
		es:           es,
		keyPrefix:    keyPrefix,
		purgerSnapCh: purgerSnapCh,
	}
}

func (w *PebbleWal) SetUnsafeNoFsync() {
	w.noFsync = true
}

func (w *PebbleWal) SetMultiRaftPurger(groupID ibabuza.RaftGroupID) {
	w.multiRaftGroupID = groupID
}

func (w *PebbleWal) Save(hardState raftpb.HardState, entries []raftpb.Entry) error {
	batch := w.db.NewBatch()
	defer batch.Close()

	if !isEmptyHardState(hardState) {
		w.hardState = hardState
		data, err := hardState.Marshal()
		if err != nil {
			return err
		}
		if err = batch.Set(w.keyPrefix.hardState, data, pebble.Sync); err != nil {
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
		if err = batch.Set(key[:24], data, pebble.Sync); err != nil {
			return err
		}
	}

	syncMode := pebble.NoSync
	if !w.noFsync && raft.MustSync(hardState, w.hardState, len(entries)) {
		syncMode = pebble.Sync
	}

	if err := batch.Commit(syncMode); err != nil {
		return err
	}

	if len(entries) > 0 && w.es != nil {
		w.es.AppendCache(entries)
	}
	return nil
}

func (w *PebbleWal) SaveSnapshot(snapshot raftpb.Snapshot) error {
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

	var key [24]byte
	copy(key[:], w.keyPrefix.snapshot)
	binary.BigEndian.PutUint64(key[16:], snapshot.Metadata.Index)
	return w.db.Set(key[:24], data, pebble.Sync)
}

func (w *PebbleWal) Purge(snapshot raftpb.Snapshot) error {
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

func (w *PebbleWal) Sync() error {
	if !w.noFsync {
		// Pebble handles syncing internally based on the write options
		// We don't need to explicitly sync here as it's handled by the commit options
		return nil
	}
	return nil
}

func (w *PebbleWal) Close() error {
	return nil
}
