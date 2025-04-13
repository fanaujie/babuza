package kvstore

import (
	"encoding/binary"
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"hash/crc32"
	"io"
	"sync"
)

type MemoryStore struct {
	store *KvOperationOrderMap
	mu    *sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		store: NewKvOperationOrderMap(),
		mu:    &sync.RWMutex{},
	}
}

func (m *MemoryStore) Apply(e ibabuza.Entry) {
	var req KvCommand

	if err := req.Unmarshal(e.Command()); err != nil {
		panic(err)
	}
	switch req.Command {
	case Set:
		m.mu.Lock()
		m.store.Set(req.Key, req.Value)
		m.mu.Unlock()
		res := KvResult{
			Command: Set,
			Key:     req.Key,
			Value:   req.Value,
		}
		e.SendResponse(&res, nil)
	case Append:
		m.mu.Lock()
		result := m.store.Append(string(req.Key), req.Value)
		m.mu.Unlock()
		res := KvResult{
			Command: Append,
			Key:     req.Key,
			Value:   result,
		}
		e.SendResponse(&res, nil)
	case Delete:
		m.mu.Lock()
		ok := m.store.Delete(string(req.Key))
		m.mu.Unlock()
		if ok {
			res := KvResult{
				Command: Delete,
				Key:     req.Key,
			}
			e.SendResponse(&res, nil)
		} else {
			e.SendResponse(nil, kverror.ErrKeyNotFound)
		}
	}
}
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

func (m *MemoryStore) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, _, err := reader.Open(MemorySnapshotTag)
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	m.store = NewKvOperationOrderMap()
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
func (m *MemoryStore) Hash() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	it := m.store.Iterator()
	if it == nil {
		return 0
	}
	for pair := it.First(); pair != nil; pair = it.Next() {
		h.Write([]byte(pair.Key))
		h.Write([]byte(pair.Value))
	}
	return h.Sum32()
}
func (m *MemoryStore) Close() error {
	return nil
}

func (m *MemoryStore) Load(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.store.Get(string(key))
	if !ok {
		return "", kverror.ErrKeyNotFound
	}
	return r, nil
}
