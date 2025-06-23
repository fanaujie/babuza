package lsmtwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"time"
)

type BadgerWalManager struct {
	logger       ibabuza.Logger
	db           *badger.DB
	keyPrefix    *keyPrefix
	stopCh       chan struct{}
	purgerSnapCh chan purgeRequest
	purgerStopCh chan struct{}
}

var _ ibabuza.WalManager = (*BadgerWalManager)(nil)

func NewBadgerWalManager(config Config, logger ibabuza.Logger) *BadgerWalManager {
	stopCh := make(chan struct{})
	if !fileutil.Exist(config.WalDir) {
		err := fileutil.CreateDirAndTouch(config.WalDir)
		if err != nil {
			logger.Panicf("failed to create wal dir %s: %v", config.WalDir, err)
		}
	}
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
		logger:       logger,
		db:           db,
		keyPrefix:    newKeyPrefix(0),
		stopCh:       stopCh,
		purgerSnapCh: make(chan purgeRequest, 1),
		purgerStopCh: make(chan struct{}),
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
	w := NewBadgerWal(m.db, es, m.keyPrefix, m.purgerSnapCh)
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
	w := NewBadgerWal(m.db, es, m.keyPrefix, m.purgerSnapCh)
	return es, w, result, nil
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

func (m *BadgerWalManager) Purger() ibabuza.WalPurger {
	return &badgerPurger{
		BadgerWalManager: m,
	}
}

type badgerPurger struct {
	*BadgerWalManager
}

func (p *badgerPurger) Start() {
	go func() {
		for {
			select {
			case req := <-p.purgerSnapCh:
				if err := p.purgeSnapshot(req.snapshot); err != nil {
					p.logger.Errorf("failed to purge snapshot index=%d: %v", req.snapshot.Metadata.Index, err)
				}
			case <-p.purgerStopCh:
				return
			}
		}
	}()
}

func (p *badgerPurger) purgeSnapshot(snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}

	// Delete entries that are included in the snapshot
	return p.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate from the beginning of entries up to (but not including) snapshot.Index + 1
		for it.Seek(p.keyPrefix.entry); it.ValidForPrefix(p.keyPrefix.entry); it.Next() {
			entryIndex := binary.BigEndian.Uint64(it.Item().Key()[16:])
			// Check if we've reached the upper bound (snapshot.Index + 1)
			if entryIndex >= snapshot.Metadata.Index+1 {
				break
			}

			// Make a copy of the key for deletion
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
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
			foundIndex := binary.BigEndian.Uint64(it.Item().Key()[16:])
			if foundIndex != readEntryIndex[startIndex].Index {
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
			return fmt.Errorf("only found %d of %d requested entries for readEntryIndex %v", startIndex, len(readEntryIndex), readEntryIndex)
		}
		return nil
	})
}

func (m *BadgerWalManager) Close() error {
	select {
	case <-m.purgerStopCh:
	default:
		close(m.purgerStopCh)
	}
	close(m.stopCh)
	return m.db.Close()
}
