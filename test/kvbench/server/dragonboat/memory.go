package dragonboat

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"io"
	"sync"
)

// Errors
var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrUnknownCommand = errors.New("unknown command")
)

type KVPair struct {
	Key   []byte
	Value []byte
}

type BatchKVPair []KVPair

const (
	batchKvCount      = 16
	MemorySnapshotTag = "kv-memory"
)

// MemoryStore is a simple in-memory key-value store
type MemoryStore struct {
	store *kvstore.KvOperationOrderMap
	mu    sync.RWMutex
}

// NewMemoryStore creates a new memory-based key-value store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		store: kvstore.NewKvOperationOrderMap(),
	}
}

// Close closes the state machine
func (m *MemoryStore) Close() error {
	return nil
}

// implement the sm.IStateMachine

func (m *MemoryStore) Lookup(query interface{}) (interface{}, error) {
	return nil, nil
}

func (m *MemoryStore) Update(e sm.Entry) (sm.Result, error) {
	var req kvbenchpb.KvOP

	if err := req.Unmarshal(e.Cmd); err != nil {
		panic(err)
	}
	switch req.Command {
	case kvbenchpb.KvCommand_PUT:
		m.store.Set(string(req.Key), string(req.Value))
		return sm.Result{
			Value: e.Index}, nil
	}
	return sm.Result{}, ErrUnknownCommand
}

func (m *MemoryStore) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	buf := make([]byte, 8)
	var batchKv BatchKVPair
	batchWrite := func(kv []KVPair) error {
		data, err := json.Marshal(kv)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(buf, uint64(len(data)))
		if _, err = w.Write(buf); err != nil {
			return err
		}
		if _, err = w.Write(data); err != nil {
			return err
		}
		return nil
	}
	it := m.store.Iterator()
	for strPair := it.First(); strPair != nil; strPair = it.Next() {
		pair := KVPair{}
		pair.Key = []byte(strPair.Key)
		pair.Value = []byte(strPair.Value)
		batchKv = append(batchKv, pair)
		if len(batchKv) == batchKvCount {
			if err := batchWrite(batchKv); err != nil {
				return err
			}
			batchKv = batchKv[:0]
		}
	}
	if err := batchWrite(batchKv); err != nil {
		return err
	}
	return nil
}

func (m *MemoryStore) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf := make([]byte, 8)
	m.store = kvstore.NewKvOperationOrderMap()
	var batchKv BatchKVPair
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		batchKvSize := binary.LittleEndian.Uint64(buf)
		data := make([]byte, batchKvSize)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		if err := json.Unmarshal(data, &batchKv); err != nil {
			return err
		}
		for _, pair := range batchKv {
			m.store.Set(string(pair.Key), string(pair.Value))
		}
		batchKv = batchKv[:0]
	}
	return nil
}
