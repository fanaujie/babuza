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

type MultiRaftBadgerWalManager struct {
	logger      ibabuza.Logger
	db          *badger.DB
	prefixCache *keyPrefixCache
	stopCh      chan struct{}
}

type GroupEntryDataReader struct {
	manager *MultiRaftBadgerWalManager
	groupID ibabuza.RaftGroupID
}

func (r *GroupEntryDataReader) ReadEntriesData(readEntryIndex []walbase.EntryIndex[storage.EntryMetadata], destEnts []raftpb.Entry) error {
	return r.manager.ReadEntriesData(r.groupID, readEntryIndex, destEnts)
}

type MultiRaftConfig struct {
	InMemory           bool
	WalDir             string
	KeyPrefixCacheSize int
}

func NewMultiRaftBadgerWalManager(config MultiRaftConfig, logger ibabuza.Logger) ibabuza.MultiRaftWalManager {
	db, err := badger.Open(badger.DefaultOptions(config.WalDir).WithInMemory(config.InMemory))
	if err != nil {
		logger.Panicf("failed to open badger database: %v", err)
	}
	stopCh := make(chan struct{})
	manager := &MultiRaftBadgerWalManager{
		logger:      logger,
		db:          db,
		prefixCache: newKeyPrefixCache(config.KeyPrefixCacheSize),
		stopCh:      stopCh,
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

	w := NewBadgerWal(m.db, es, m.prefixCache.get(groupID))

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

	return es, NewBadgerWal(m.db, es, m.prefixCache.get(groupID)), result, nil
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

func (m *MultiRaftBadgerWalManager) PurgeWals(config ibabuza.WalPurgeConfig) {
	// TODO: implement this
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

		if startIndex == 0 {
			return fmt.Errorf("no entries found for readEntryIndex %v", readEntryIndex)
		}

		return nil
	})
}

func (m *MultiRaftBadgerWalManager) Close() error {
	close(m.stopCh)
	return m.db.Close()
}
