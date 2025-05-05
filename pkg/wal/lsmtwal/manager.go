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

const (
	keyHardState = uint64(1)
	keySnapshot  = uint64(2)
	keyEntry     = uint64(3)
	keyMetadata  = uint64(4)
)

type keyPrefix struct {
	hardState       []byte
	snapshot        []byte
	entry           []byte
	metadata        []byte
	reverseMetadata []byte
}

type BadgerWalManager struct {
	logger    ibabuza.Logger
	db        *badger.DB
	keyPrefix *keyPrefix
	stopCh    chan struct{}
}

type Config struct {
	InMemory bool
	WalDir   string
}

func newKeyPrefix(groupID ibabuza.RaftGroupID) *keyPrefix {
	createKey := func(typeID uint64) []byte {
		if groupID == 0 {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, typeID)
			return key
		}
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[:8], uint64(groupID))
		binary.BigEndian.PutUint64(key[8:], typeID)
		return key
	}
	createReverseKey := func(typeID uint64) []byte {
		if groupID == 0 {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, typeID)
			return key
		}
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[:8], typeID)
		binary.BigEndian.PutUint64(key[8:], uint64(groupID))
		return key
	}

	return &keyPrefix{
		hardState:       createKey(keyHardState),
		snapshot:        createKey(keySnapshot),
		entry:           createKey(keyEntry),
		metadata:        createKey(keyMetadata),
		reverseMetadata: createReverseKey(keyMetadata),
	}
}

var _ ibabuza.WalManager = (*BadgerWalManager)(nil)

func NewBadgerWalManager(config Config, logger ibabuza.Logger) *BadgerWalManager {
	stopCh := make(chan struct{})
	db, err := badger.Open(badger.DefaultOptions(config.WalDir).WithInMemory(config.InMemory))
	if err != nil {
		logger.Panicf("failed to open badger database: %v", err)
	}
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
	return &BadgerWalManager{
		logger:    logger,
		db:        db,
		keyPrefix: newKeyPrefix(0),
		stopCh:    stopCh,
	}
}

func (m *BadgerWalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	snapshots := make([]walpb.Snapshot, 0)
	if err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := m.keyPrefix.snapshot
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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

func (m *BadgerWalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {

	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](m),
	}
	w := NewBadgerWal(m.db, es, m.keyPrefix)
	// write empty snapshot and metadata to the database
	if err := m.db.Update(func(txn *badger.Txn) error {
		snapshot := walpb.Snapshot{}
		data, err := snapshot.Marshal()
		if err != nil {
			return err
		}
		if err = txn.Set(m.keyPrefix.snapshot, data); err != nil {
			return err
		}
		data, err = metadata.Marshal()
		if err != nil {
			return err
		}
		return txn.Set(m.keyPrefix.metadata, data)
	}); err != nil {
		return nil, nil, err
	}
	return es, w, nil
}

func (m *BadgerWalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {

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
		hardStateBytes, err := txn.Get(m.keyPrefix.hardState)
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
		walMetadata, err := txn.Get(m.keyPrefix.metadata)
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
		prefix := m.keyPrefix.entry
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
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](m),
	}
	if snapshot != nil {
		es.ApplySnapshot(*snapshot)
	}
	es.SetHardState(result.HardState())
	if err = es.Append(entries); err != nil {
		return nil, nil, nil, err
	}
	return es, NewBadgerWal(m.db, es, m.keyPrefix), result, nil
}

func (m *BadgerWalManager) HasExistingWals() (bool, error) {
	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := m.keyPrefix.entry
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return nil
		}
		return errors.New("no existing WALs found")
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (m *BadgerWalManager) PurgeWals(config ibabuza.WalPurgeConfig) {
	//TODO: implement this
}

func (m *BadgerWalManager) ReadEntriesData(readEntryIndex []walbase.EntryIndex[storage.EntryMetadata],
	destEnts []raftpb.Entry) error {

	if len(readEntryIndex) != len(destEnts) || len(readEntryIndex) == 0 {
		return errors.New("invalid the size of entryIndex and raftpb.Entry")
	}
	return m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var startKey [24]byte
		copy(startKey[:16], m.keyPrefix.entry)
		startIndex := 0
		binary.BigEndian.PutUint64(startKey[16:], readEntryIndex[startIndex].Index)
		for it.Seek(startKey[:24]); startIndex < len(readEntryIndex) && it.ValidForPrefix(startKey[:16]); it.Next() {
			if binary.BigEndian.Uint64(it.Item().Key()[16:]) != readEntryIndex[startIndex].Index {
				return fmt.Errorf("invalid entry index %d expected %d",
					binary.BigEndian.Uint64(it.Item().Key()), readEntryIndex[startIndex].Index)
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

func (m *BadgerWalManager) Close() error {
	close(m.stopCh)
	return m.db.Close()
}
