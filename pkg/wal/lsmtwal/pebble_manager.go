package lsmtwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type PebbleWalManager struct {
	logger       ibabuza.Logger
	db           *pebble.DB
	keyPrefix    *keyPrefix
	purgerSnapCh chan purgeRequest
	purgerStopCh chan struct{}
}

var _ ibabuza.WalManager = (*PebbleWalManager)(nil)

func NewPebbleWalManager(config Config, logger ibabuza.Logger) *PebbleWalManager {
	if !config.InMemory && !fileutil.Exist(config.WalDir) {
		err := fileutil.CreateDirAndTouch(config.WalDir)
		if err != nil {
			logger.Panicf("failed to create wal dir %s: %v", config.WalDir, err)
		}
	}

	opts := &pebble.Options{}
	if config.InMemory {
		opts.FS = vfs.NewMem()
		config.WalDir = ""
	}

	db, err := pebble.Open(config.WalDir, opts)
	if err != nil {
		logger.Panicf("failed to open pebble database: %v", err)
	}

	return &PebbleWalManager{
		logger:       logger,
		db:           db,
		keyPrefix:    newKeyPrefix(0),
		purgerSnapCh: make(chan purgeRequest, 1),
		purgerStopCh: make(chan struct{}),
	}
}

func (m *PebbleWalManager) FindSnapshot() ([]walpb.Snapshot, error) {
	snapshots := make([]walpb.Snapshot, 0)

	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: m.keyPrefix.snapshot,
		UpperBound: append(m.keyPrefix.snapshot, 0xff),
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

func (m *PebbleWalManager) CreateWal(metadata babuzapb.WalMetadata) (ibabuza.EntryStorage, ibabuza.Wal, error) {
	es := &storage.EntryStorage{
		EntryStorage: walbase.NewEntryStorage[storage.EntryMetadata](m),
	}
	w := NewPebbleWal(m.db, es, m.keyPrefix, m.purgerSnapCh)

	// write empty snapshot and metadata to the database
	batch := m.db.NewBatch()
	defer batch.Close()

	snapshot := walpb.Snapshot{}
	data, err := snapshot.Marshal()
	if err != nil {
		return nil, nil, err
	}
	if err = batch.Set(m.keyPrefix.snapshot, data, pebble.Sync); err != nil {
		return nil, nil, err
	}

	data, err = metadata.Marshal()
	if err != nil {
		return nil, nil, err
	}
	if err = batch.Set(m.keyPrefix.metadata, data, pebble.Sync); err != nil {
		return nil, nil, err
	}

	if err = batch.Commit(pebble.Sync); err != nil {
		return nil, nil, err
	}

	return es, w, nil
}

func (m *PebbleWalManager) ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (
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

	// Read hard state
	hardStateBytes, closer, err := m.db.Get(m.keyPrefix.hardState)
	if err == nil {
		if err = hardState.Unmarshal(hardStateBytes); err != nil {
			closer.Close()
			return nil, nil, nil, err
		}
		closer.Close()
	}

	// Read metadata
	walMetadata, closer, err := m.db.Get(m.keyPrefix.metadata)
	if err != nil {
		return nil, nil, nil, err
	}
	metadata = make([]byte, len(walMetadata))
	copy(metadata, walMetadata)
	closer.Close()

	// Read entries
	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: m.keyPrefix.entry,
		UpperBound: append(m.keyPrefix.entry, 0xff),
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
	w := NewPebbleWal(m.db, es, m.keyPrefix, m.purgerSnapCh)
	return es, w, result, nil
}

func (m *PebbleWalManager) HasExistingWals() (bool, error) {
	iter, err := m.db.NewIter(&pebble.IterOptions{
		LowerBound: m.keyPrefix.entry,
		UpperBound: append(m.keyPrefix.entry, 0xff),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	if iter.First() {
		return true, nil
	}

	if err = iter.Error(); err != nil {
		return false, err
	}

	return false, nil
}

func (m *PebbleWalManager) Purger() ibabuza.WalPurger {
	return &pebblePurger{
		PebbleWalManager: m,
	}
}

type pebblePurger struct {
	*PebbleWalManager
}

func (p *pebblePurger) Start() {
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

func (p *pebblePurger) purgeSnapshot(snapshot raftpb.Snapshot) error {
	if isEmptySnapshot(snapshot) {
		return nil
	}

	// Delete entries that are included in the snapshot using DeleteRange for better performance
	batch := p.db.NewBatch()
	defer batch.Close()

	// Prepare seek key with the snapshot index + 1 (to include the snapshot index in deletion)
	var snapshotIndexPlusOne [24]byte
	copy(snapshotIndexPlusOne[:16], p.keyPrefix.entry)
	binary.BigEndian.PutUint64(snapshotIndexPlusOne[16:], snapshot.Metadata.Index+1)

	if err := batch.DeleteRange(p.keyPrefix.entry, snapshotIndexPlusOne[:24], pebble.Sync); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

func (m *PebbleWalManager) ReadEntriesData(readEntryIndex []walbase.EntryIndex[storage.EntryMetadata],
	destEnts []raftpb.Entry) error {

	if len(readEntryIndex) != len(destEnts) || len(readEntryIndex) == 0 {
		return errors.New("invalid the size of entryIndex and raftpb.Entry")
	}

	for i, entryIndex := range readEntryIndex {
		var key [24]byte
		copy(key[:16], m.keyPrefix.entry)
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

func (m *PebbleWalManager) Close() error {
	select {
	case <-m.purgerStopCh:
	default:
		close(m.purgerStopCh)
	}
	return m.db.Close()
}
