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
)

type BadgerWalManager struct {
	logger ibabuza.Logger
	db     *badger.DB
}

type Config struct {
	InMemory bool
	WalDir   string
}

func NewBadgerWalManager(config Config, logger ibabuza.Logger) ibabuza.WalManager {
	db, err := badger.Open(badger.DefaultOptions(config.WalDir).WithInMemory(config.InMemory))
	if err != nil {
		logger.Panicf("failed to open badger database: %v", err)
	}
	return &BadgerWalManager{
		logger: logger,
		db:     db,
	}
}

func (m *BadgerWalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	snapshots := make([]walpb.Snapshot, 0)
	if err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(keySnapshot)
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

	mData, err := metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}
	err = m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyMetadata), mData)
	})
	if err != nil {
		return nil, nil, err
	}
	w := NewBadgerWal(m.db)
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](m),
	}
	return es, w, nil
}

func (m *BadgerWalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
	ibabuza.EntryStorage, ibabuza.Wal, ibabuza.ReplayWalResult, error) {

	var hardState raftpb.HardState
	var metadata []byte
	entries := make([]raftpb.Entry, 0)
	err := m.db.View(func(txn *badger.Txn) error {
		hardStateBytes, err := txn.Get([]byte(keyHardState))
		if err != nil {
			return err
		}
		if err = hardStateBytes.Value(func(value []byte) error {
			if err = hardState.Unmarshal(value); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		walMetadata, err := txn.Get([]byte(keyMetadata))
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
		prefix := []byte(keyEntry)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entry := raftpb.Entry{}
			if err = it.Item().Value(func(value []byte) error {
				return entry.Unmarshal(value)
			}); err != nil {
				return err
			}
			if entry.Index > snapshot.Metadata.Index {
				// prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
				up := entry.Index - snapshot.Metadata.Index - 1
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
	w := NewBadgerWal(m.db)
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](m),
	}
	return es, w, result, nil
}

func (m *BadgerWalManager) HasExistingWals() (bool, error) {
	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(keyEntry)
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
		startKey := make([]byte, 16)
		copy(startKey, keyEntry)
		startIndex := 0
		binary.BigEndian.PutUint64(startKey[8:], readEntryIndex[startIndex].Index)
		for it.Seek(startKey); it.ValidForPrefix(startKey[:8]); it.Next() {
			if binary.BigEndian.Uint64(it.Item().Key()[8:]) != readEntryIndex[startIndex].Index {
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
		if startIndex != len(readEntryIndex) {
			return fmt.Errorf("start index %d is not equal to the length of readEntryIndex %d",
				startIndex, len(readEntryIndex))
		}
		return nil
	})
}
