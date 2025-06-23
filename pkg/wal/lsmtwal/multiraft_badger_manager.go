package lsmtwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"time"
)

func NewMultiRaftBadgerWalManager(config MultiRaftConfig, logger ibabuza.Logger) ibabuza.MultiRaftWalManager {
	db, err := badger.Open(badger.DefaultOptions(config.WalDir).WithInMemory(config.InMemory))
	if err != nil {
		logger.Panicf("failed to open badger database: %v", err)
	}
	stopCh := make(chan struct{})
	manager := &MultiRaftBadgerWalManager{
		logger:       logger,
		db:           db,
		prefixCache:  newKeyPrefixCache(config.KeyPrefixCacheSize),
		stopCh:       stopCh,
		purgerSnapCh: make(chan purgeRequest, 1),
		purgerStopCh: make(chan struct{}),
	}

	// Start a background goroutine to run value log GC periodically

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

	return manager
}

func (m *MultiRaftBadgerWalManager) FindSnapshot(groupID ibabuza.RaftGroupID) ([]walpb.Snapshot, error) {
	snapshots := make([]walpb.Snapshot, 0)

	groupPrefix := m.prefixCache.get(groupID)

	if err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		var prefix [16]byte
		copy(prefix[:16], groupPrefix.snapshot)
		for it.Seek(prefix[:16]); it.ValidForPrefix(prefix[:16]); it.Next() {
			ws := walpb.Snapshot{}
			if err := it.Item().Value(func(value []byte) error {
				if err := ws.Unmarshal(value); err != nil {
					return err
				}
				snapshots = append(snapshots, ws)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (m *MultiRaftBadgerWalManager) CreateWal(groupID ibabuza.RaftGroupID, metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	groupPrefix := m.prefixCache.get(groupID)

	reader := &GroupEntryDataReader{
		manager: m,
		groupID: groupID,
	}
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](reader),
	}

	w := NewBadgerWal(m.db, es, m.prefixCache.get(groupID), m.purgerSnapCh)
	w.SetMultiRaftPurger(groupID)

	// write empty snapshot and metadata to the database
	if err := m.db.Update(func(txn *badger.Txn) error {
		snapshot := walpb.Snapshot{}
		data, err := snapshot.Marshal()
		if err != nil {
			return err
		}
		if err = txn.Set(groupPrefix.snapshot, data); err != nil {
			return err
		}
		data, err = metadata.Marshal()
		if err != nil {
			return err
		}
		return txn.Set(groupPrefix.metadata, data)
	}); err != nil {
		return nil, nil, err
	}

	return es, w, nil
}

func (m *MultiRaftBadgerWalManager) ReplayWal(groupID ibabuza.RaftGroupID, snapshot *raftpb.Snapshot, deleteUncommitted bool) (
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
	err := m.db.View(func(txn *badger.Txn) error {
		hardStateBytes, err := txn.Get(groupPrefix.hardState)
		if err == nil {
			if err = hardStateBytes.Value(func(value []byte) error {
				if err = hardState.Unmarshal(value); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}

		walMetadata, err := txn.Get(groupPrefix.metadata)
		if err != nil {
			return err
		}
		if err = walMetadata.Value(func(value []byte) error {
			metadata = make([]byte, len(value))
			copy(metadata, value)
			return nil
		}); err != nil {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := groupPrefix.entry
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entry := raftpb.Entry{}
			if err = it.Item().Value(func(value []byte) error {
				return entry.Unmarshal(value)
			}); err != nil {
				return err
			}
			if entry.Index > walSnap.Index {
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := entry.Index - walSnap.Index - 1
				if up > uint64(len(entries)) {
					// return error before append call causes runtime panic
					return errors.New("up is out of range")
				}
				// The line below is potentially overriding some 'uncommitted' termEntriesIndex.
				entries = append(entries[:up], entry)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	result := walbase.NewReplayResult(metadata, hardState, entries)
	if deleteUncommitted {
		if err = result.DeleteUncommittedEntry(result.HardState().Commit); err != nil {
			return nil, nil, nil, err
		}
	}

	reader := &GroupEntryDataReader{
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

	w := NewBadgerWal(m.db, es, m.prefixCache.get(groupID), m.purgerSnapCh)
	w.SetMultiRaftPurger(groupID)
	return es, w, result, nil
}

func (m *MultiRaftBadgerWalManager) HasExistingWals() ([]ibabuza.RaftGroupID, error) {
	var groupIDs []ibabuza.RaftGroupID

	if err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var prefix [8]byte
		binary.BigEndian.PutUint64(prefix[:8], keyMetadata)
		for it.Seek(prefix[:8]); it.ValidForPrefix(prefix[:8]); it.Next() {
			groupID := ibabuza.RaftGroupID(binary.BigEndian.Uint64(it.Item().Key()[8:16]))
			groupIDs = append(groupIDs, groupID)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return groupIDs, nil
}

func (m *MultiRaftBadgerWalManager) ReadEntriesData(groupID ibabuza.RaftGroupID, readEntryIndex []walbase.EntryIndex[storage.EntryMetadata],
	destEnts []raftpb.Entry) error {

	groupPrefix := m.prefixCache.get(groupID)

	if len(readEntryIndex) != len(destEnts) || len(readEntryIndex) == 0 {
		return errors.New("invalid the size of entryIndex and raftpb.Entry")
	}

	return m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var startKey [24]byte
		copy(startKey[:16], groupPrefix.entry)
		startIndex := 0
		binary.BigEndian.PutUint64(startKey[16:], readEntryIndex[startIndex].Index)

		for it.Seek(startKey[:24]); startIndex < len(readEntryIndex) && it.ValidForPrefix(startKey[:16]); it.Next() {
			keyBytes := it.Item().Key()
			entryIndex := binary.BigEndian.Uint64(keyBytes[16:])
			if entryIndex != readEntryIndex[startIndex].Index {
				return fmt.Errorf("invalid entry index %d expected %d",
					entryIndex, readEntryIndex[startIndex].Index)
			}

			if err := it.Item().Value(func(value []byte) error {
				var ent raftpb.Entry
				if err := ent.Unmarshal(value); err != nil {
					return err
				}
				destEnts[startIndex] = ent
				return nil
			}); err != nil {
				return err
			}

			startIndex++
		}

		if startIndex != len(readEntryIndex) {
			return fmt.Errorf("only found %d of %d requested entries for readEntryIndex %v", startIndex, len(readEntryIndex), readEntryIndex)
		}

		return nil
	})
}

func (m *MultiRaftBadgerWalManager) Purger() ibabuza.WalPurger {
	return &multiRaftBadgerPurger{
		MultiRaftBadgerWalManager: m,
	}
}

func (m *MultiRaftBadgerWalManager) RemoveData(groupID ibabuza.RaftGroupID) error {
	groupPrefix := m.prefixCache.get(groupID)

	// Use DropPrefix for more efficient batch deletion
	prefixes := [][]byte{
		groupPrefix.hardState,
		groupPrefix.snapshot,
		groupPrefix.metadata,
		groupPrefix.entry,
	}

	if err := m.db.DropPrefix(prefixes...); err != nil {
		return fmt.Errorf("failed to drop prefixes for group %d: %v", groupID, err)
	}

	m.logger.Infof("Successfully removed WAL data for group %d", groupID)
	return nil
}

type multiRaftBadgerPurger struct {
	*MultiRaftBadgerWalManager
}

func (p *multiRaftBadgerPurger) Start() {
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
}

func (p *multiRaftBadgerPurger) Stop() {
	select {
	case <-p.purgerStopCh:
	default:
		close(p.purgerStopCh)
	}
}

func (p *multiRaftBadgerPurger) purgeSnapshot(groupID ibabuza.RaftGroupID, snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}

	groupPrefix := p.prefixCache.get(groupID)

	// Delete entries that are included in the snapshot
	return p.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate from the beginning of entries up to (but not including) snapshot.Index + 1
		for it.Seek(groupPrefix.entry); it.ValidForPrefix(groupPrefix.entry); it.Next() {
			entryIndex := binary.BigEndian.Uint64(it.Item().Key()[16:])
			// Check if we've reached the upper bound (snapshot.Index + 1)
			if entryIndex >= snapshot.Metadata.Index+1 {
				break
			}
			copyKey := it.Item().KeyCopy(nil)
			if err := txn.Delete(copyKey); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *MultiRaftBadgerWalManager) Close() error {
	select {
	case <-m.purgerStopCh:
	default:
		close(m.purgerStopCh)
	}
	close(m.stopCh)
	return m.db.Close()
}
