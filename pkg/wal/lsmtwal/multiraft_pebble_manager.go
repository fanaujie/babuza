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
	"sync"
)

type MultiRaftPebbleWalManager struct {
	logger       ibabuza.Logger
	db           *pebble.DB
	prefixCache  *keyPrefixCache
	purgerSnapCh chan purgeRequest
	purgerStopCh chan struct{}
	once         sync.Once
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
		logger:       logger,
		db:           db,
		prefixCache:  newKeyPrefixCache(config.KeyPrefixCacheSize),
		purgerSnapCh: make(chan purgeRequest, 1),
		purgerStopCh: make(chan struct{}),
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

	w := NewPebbleWal(m.db, es, m.prefixCache.get(groupID), m.purgerSnapCh)
	w.SetMultiRaftPurger(groupID)

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

func (m *MultiRaftPebbleWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (ibabuza.ReplayWalResult, ibabuza.EntryStorage, ibabuza.Wal, error) {

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

	w := NewPebbleWal(m.db, es, m.prefixCache.get(groupID), m.purgerSnapCh)
	w.SetMultiRaftPurger(groupID)
	return result, es, w, nil
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
		if err = ent.Unmarshal(value); err != nil {
			closer.Close()
			return err
		}
		destEnts[i] = ent
		closer.Close()
	}

	return nil
}

func (m *MultiRaftPebbleWalManager) Purger() ibabuza.WalPurger {
	return &multiRaftPebblePurger{
		MultiRaftPebbleWalManager: m,
	}
}

func (m *MultiRaftPebbleWalManager) RemoveData(groupID ibabuza.RaftGroupID) error {
	groupPrefix := m.prefixCache.get(groupID)

	// Check if the group exists by looking for metadata
	_, closer, err := m.db.Get(groupPrefix.metadata)
	if err != nil && err != pebble.ErrNotFound {
		return fmt.Errorf("failed to check if group %d exists: %v", groupID, err)
	}
	if err == pebble.ErrNotFound {
		// Group doesn't exist, return success (idempotent operation)
		m.logger.Infof("Group %d does not exist, RemoveData is a no-op", groupID)
		return nil
	}
	closer.Close()

	batch := m.db.NewBatch()
	defer batch.Close()

	// Delete all keys associated with this group using DeleteRange for better performance
	keyTypes := [][]byte{
		groupPrefix.hardState,
		groupPrefix.snapshot,
		groupPrefix.metadata,
		groupPrefix.entry,
	}

	for _, keyType := range keyTypes {
		upperBound := append(keyType, 0xff)
		if err := batch.DeleteRange(keyType, upperBound, pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete range for group %d: %v", groupID, err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit deletion batch for group %d: %v", groupID, err)
	}

	m.logger.Infof("Successfully removed WAL data for group %d", groupID)
	return nil
}

type multiRaftPebblePurger struct {
	*MultiRaftPebbleWalManager
}

func (p *multiRaftPebblePurger) Start() {
	p.once.Do(func() {
		go func() {
			for {
				select {
				case req := <-p.purgerSnapCh:
					if err := p.purgeSnapshot(req.groupID, req.snapshot); err != nil {
						p.logger.Errorf("failed to purge snapshot groupID=%d index=%d: %v", req.groupID, req.snapshot.Metadata.Index, err)
					}
				case <-p.purgerStopCh:
					return
				}
			}
		}()
	})
}

func (p *multiRaftPebblePurger) Stop() {
	select {
	case <-p.purgerStopCh:
	default:
		close(p.purgerStopCh)
	}
}

func (p *multiRaftPebblePurger) purgeSnapshot(groupID ibabuza.RaftGroupID, snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}

	groupPrefix := p.prefixCache.get(groupID)

	// Delete entries that are included in the snapshot
	batch := p.db.NewBatch()
	defer batch.Close()

	// Prepare seek key with the snapshot index + 1 (to include the snapshot index in deletion)
	var snapshotIndexPlusOne [24]byte
	copy(snapshotIndexPlusOne[:16], groupPrefix.entry)
	binary.BigEndian.PutUint64(snapshotIndexPlusOne[16:], snapshot.Metadata.Index+1)

	if err := batch.DeleteRange(groupPrefix.entry, snapshotIndexPlusOne[:24], pebble.Sync); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

func (m *MultiRaftPebbleWalManager) Close() error {
	select {
	case <-m.purgerStopCh:
	default:
		close(m.purgerStopCh)
	}
	return m.db.Close()
}
