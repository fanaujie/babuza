package kvstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"io"
	"sync"
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

// Apply applies a command to the state machine
func (m *MemoryStore) Apply(e ibabuza.Entry) ibabuza.ApplyResult {
	var req kvbenchpb.KvOP

	if err := req.Unmarshal(e.Command); err != nil {
		panic(err)
	}
	switch req.Command {
	case kvbenchpb.KvCommand_PUT:
		m.store.Set(string(req.Key), string(req.Value))
		return ibabuza.ApplyResult{
			LogIndex: e.Index,
		}
	}
	return ibabuza.ApplyResult{
		LogIndex: e.Index,
		Error:    ErrUnknownCommand,
	}
}

// SaveSnapshot saves the state machine to a snapshot
func (m *MemoryStore) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	wc, err := writer.CreateStateMachineFile(MemorySnapshotTag, babuzapb.SnapshotFileCompression_Snappy)
	if err != nil {
		return err
	}
	defer wc.Close()
	buf := make([]byte, 8)
	var batchKv BatchKVPair
	batchWrite := func(kv []KVPair) error {
		data, err := json.Marshal(kv)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(buf, uint64(len(data)))
		if _, err = wc.Write(buf); err != nil {
			return err
		}
		if _, err = wc.Write(data); err != nil {
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
			if err = batchWrite(batchKv); err != nil {
				return err
			}
			batchKv = batchKv[:0]
		}
	}
	if err = batchWrite(batchKv); err != nil {
		return err
	}
	return nil
}

// RestoreFromSnapshot restores the state machine from a snapshot
func (m *MemoryStore) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, _, err := reader.Open(MemorySnapshotTag)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	m.store = kvstore.NewKvOperationOrderMap()
	var batchKv BatchKVPair
	for {
		if _, err = io.ReadFull(r, buf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		batchKvSize := binary.LittleEndian.Uint64(buf)
		data := make([]byte, batchKvSize)
		if _, err = io.ReadFull(r, data); err != nil {
			return err
		}
		if err = json.Unmarshal(data, &batchKv); err != nil {
			return err
		}
		for _, pair := range batchKv {
			m.store.Set(string(pair.Key), string(pair.Value))
		}
		batchKv = batchKv[:0]
	}
	return nil
}

// Close closes the state machine
func (m *MemoryStore) Close() error {
	return nil
}

// Errors
var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrUnknownCommand = errors.New("unknown command")
)
