package lsmtwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type MultiRaftPebbleWalManager struct {
	logger      ibabuza.Logger
	db          *pebble.DB
	prefixCache *keyPrefixCache
}

type PebbleGroupEntryDataReader struct {
	manager *MultiRaftPebbleWalManager
	groupID ibabuza.RaftGroupID
}

func (r *PebbleGroupEntryDataReader) ReadEntriesData(readEntryIndex []walbase.EntryIndex[storage.EntryMetadata], destEnts []raftpb.Entry) error {
	return r.manager.ReadEntriesData(r.groupID, readEntryIndex, destEnts)
}

func NewMultiRaftPebbleWalManager(config MultiRaftConfig, logger ibabuza.Logger) ibabuza.MultiRaftWalManager {
	opts := &pebble.Options{}
	if config.InMemory {
		opts.FS = vfs.NewMem()
		config.WalDir = ""
	}

	db, err := pebble.Open(config.WalDir, opts)
	if err != nil {
		logger.Panicf("failed to open pebble database: %v", err)
	}
	manager := &MultiRaftPebbleWalManager{
		logger:      logger,
		db:          db,
		prefixCache: newKeyPrefixCache(config.KeyPrefixCacheSize),
	}
	return manager
}

func (m *MultiRaftPebbleWalManager) FindSnapshot(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	snapshots := make([]walpb.Snapshot, 0)
	groupPrefix := m.prefixCache.get(groupID)

	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: groupPrefix.snapshot,
		UpperBound: append(groupPrefix.snapshot, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		ws := walpb.Snapshot{}
		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		if err = ws.Unmarshal(data); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, ws)
	}

	if err = iter.Error(); err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (m *MultiRaftPebbleWalManager) CreateWal(groupID ibabuza.RaftGroupID, metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	groupPrefix := m.prefixCache.get(groupID)

	reader := &PebbleGroupEntryDataReader{
		manager: m,
		groupID: groupID,
	}
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](reader),
	}

	w := NewPebbleWal(m.db, es, m.prefixCache.get(groupID))

	// write empty snapshot and metadata to the database
	batch := m.db.NewBatch()
	defer batch.Close()

	snapshot := walpb.Snapshot{}
	data, err := snapshot.Marshal()
	if err != nil {
		return nil, nil, err
	}
	if err = batch.Set(groupPrefix.snapshot, data, pebble.Sync); err != nil {
		return nil, nil, err
	}

	data, err = metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}
	if err = batch.Set(groupPrefix.metadata, data, pebble.Sync); err != nil {
		return nil, nil, err
	}

	if err = batch.Commit(pebble.Sync); err != nil {
		return nil, nil, err
	}

	return es, w, nil
}

func (m *MultiRaftPebbleWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {

	groupPrefix := m.prefixCache.get(groupID)

	var hardState raftpb.HardState
	var metadata []byte
	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap = walpb.Snapshot{
			Index:     snapshot.Metadata.Index,
			Term:      snapshot.Metadata.Term,
			ConfState: &snapshot.Metadata.ConfState,
		}
	}

	entries := make([]raftpb.Entry, 0)

	// Read hard state
	hardStateBytes, closer, err := m.db.Get(groupPrefix.hardState)
	if err == nil {
		if err = hardState.Unmarshal(hardStateBytes); err != nil {
			closer.Close()
			return nil, nil, nil, err
		}
		closer.Close()
	}

	// Read metadata
	walMetadata, closer, err := m.db.Get(groupPrefix.metadata)
	if err != nil {
		return nil, nil, nil, err
	}
	metadata = make([]byte, len(walMetadata))
	copy(metadata, walMetadata)
	closer.Close()

	// Read entries
	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: groupPrefix.entry,
		UpperBound: append(groupPrefix.entry, 0xff),
	})
	if err != nil {
		return nil, nil, nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		entry := raftpb.Entry{}
		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, nil, err
		}
		if err = entry.Unmarshal(data); err != nil {
			return nil, nil, nil, err
		}
		if entry.Index > walSnap.Index {
			// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
			up := entry.Index - walSnap.Index - 1
			if up > uint64(len(entries)) {
				// return error before append call causes runtime panic
				return nil, nil, nil, errors.New("up is out of range")
			}
			// The line below is potentially overriding some 'uncommitted' termEntriesIndex.
			entries = append(entries[:up], entry)
		}
	}

	if err = iter.Error(); err != nil {
		return nil, nil, nil, err
	}

	result := walbase.NewReplayResult(metadata, hardState, entries)
	if deleteUncommitted {
		if err = result.DeleteUncommittedEntry(result.HardState().Commit); err != nil {
			return nil, nil, nil, err
		}
	}

	reader := &PebbleGroupEntryDataReader{
		manager: m,
		groupID: groupID,
	}
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](reader),
	}
	if snapshot != nil {
		es.ApplySnapshot(*snapshot)
	}
	es.SetHardState(result.HardState())
	if err = es.Append(entries); err != nil {
		return nil, nil, nil, err
	}

	return es, NewPebbleWal(m.db, es, m.prefixCache.get(groupID)), result, nil
}

func (m *MultiRaftPebbleWalManager) HasExistingWals() ([]ibabuza.RaftGroupID, error) {
	var groupIDs []ibabuza.RaftGroupID

	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:8], keyMetadata)

	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix[:8],
		UpperBound: append(prefix[:8], 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) >= 16 {
			groupID := ibabuza.RaftGroupID(binary.BigEndian.Uint64(iter.Key()[8:16]))
			groupIDs = append(groupIDs, groupID)
		}
	}

	if err = iter.Error(); err != nil {
		return nil, err
	}

	return groupIDs, nil
}

func (m *MultiRaftPebbleWalManager) PurgeWals(config ibabuza.WalPurgeConfig) {
	// TODO: implement this
}

func (m *MultiRaftPebbleWalManager) ReadEntriesData(groupID ibabuza.RaftGroupID, readEntryIndex []walbase.EntryIndex[storage.EntryMetadata],
	destEnts []raftpb.Entry) error {

	groupPrefix := m.prefixCache.get(groupID)

	if len(readEntryIndex) != len(destEnts) || len(readEntryIndex) == 0 {
		return errors.New("invalid the size of entryIndex and raftpb.Entry")
	}

	for i, entryIndex := range readEntryIndex {
		var key [24]byte
		copy(key[:16], groupPrefix.entry)
		binary.BigEndian.PutUint64(key[16:], entryIndex.Index)

		value, closer, err := m.db.Get(key[:24])
		if err != nil {
			return fmt.Errorf("entry not found for index %d: %v", entryIndex.Index, err)
		}

		var ent raftpb.Entry
		if err := ent.Unmarshal(value); err != nil {
			closer.Close()
			return err
		}
		destEnts[i] = ent
		closer.Close()
	}

	return nil
}

func (m *MultiRaftPebbleWalManager) Close() error {
	return m.db.Close()
}
