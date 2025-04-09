package kvstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"hash/crc32"
	"io"
)

var (
	applyIndexPrefix = []byte("applyIndex")
)

type BadgerStore struct {
	db  *badger.DB
	buf []byte
}

func NewBadgerStore(db *badger.DB) *BadgerStore {
	return &BadgerStore{
		db:  db,
		buf: make([]byte, 8),
	}
}

func (s *BadgerStore) Apply(e ibabuza.Entry) {
	var req KvCommand

	if err := req.Unmarshal(e.Command()); err != nil {
		panic(err)
	}
	binary.LittleEndian.PutUint64(s.buf, e.Index())
	switch req.Command {
	case Set:
		if err := s.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set([]byte(req.Key), []byte(req.Value)); err != nil {
				return err
			}
			if err := txn.Set(applyIndexPrefix, s.buf); err != nil {
				return err
			}
			return nil
		}); err != nil {
			panic(err)
		} else {
			res := KvResult{
				Command: Set,
				Key:     req.Key,
				Value:   req.Value,
			}
			e.SendResponse(&res)
		}
	case Append:
		var result []byte
		if err := s.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(req.Key))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					result = []byte(req.Value)
					return txn.Set([]byte(req.Key), result)
				}
				return err
			}
			return item.Value(func(val []byte) error {
				result = append(result, val...)
				result = append(result, req.Value...)
				if err = txn.Set([]byte(req.Key), result); err != nil {
					return err
				}
				return txn.Set(applyIndexPrefix, s.buf)
			})
		}); err != nil {
			panic(err)
		} else {
			res := KvResult{
				Command: Append,
				Key:     req.Key,
				Value:   string(result),
			}
			e.SendResponse(&res)
		}

	case Delete:
		key := []byte(req.Key)
		if err := s.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			if err != nil {
				return err
			}
			if err = txn.Delete(key); err != nil {
				return err
			}
			return txn.Set(applyIndexPrefix, s.buf)
		}); err != nil {
			if err != badger.ErrKeyNotFound {
				panic(err)
			}
			e.SendResponse(kverror.ErrKeyNotFound)
		} else {
			res := KvResult{
				Command: Delete,
				Key:     req.Key,
			}
			e.SendResponse(&res)
		}
	}
}

func (s *BadgerStore) PrepareSnapshotContext() (ibabuza.StateMachineSnapshotContext, error) {
	return s.db.NewTransaction(false), nil
}
func (s *BadgerStore) ReleaseSnapshotContext(ctx ibabuza.StateMachineSnapshotContext) error {
	txn, ok := ctx.(*badger.Txn)
	if !ok {
		return errors.New("can not cast ctx to point to badger.Txn")
	}
	txn.Discard()
	return nil
}

func (s *BadgerStore) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	wc, err := writer.CreateStateMachineFile(BadgerDBSnapshotTag, babuzapb.SnapshotFileCompression_Snappy)
	if err != nil {
		return err
	}
	defer wc.Close()
	txn, ok := ctx.(*badger.Txn)
	if !ok {
		return errors.New("can not cast ctx to point to badger.Txn")
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
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

	for it.Rewind(); it.Valid(); it.Next() {
		pair := KVPair{}
		item := it.Item()
		pair.Key = item.KeyCopy(pair.Key)
		pair.Value, err = item.ValueCopy(pair.Value)
		if err != nil {
			return err
		}
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

func (s *BadgerStore) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	r, _, err := reader.Open(BadgerDBSnapshotTag)
	if err != nil {
		return err
	}
	if err = s.db.DropAll(); err != nil {
		return err
	}
	buf := make([]byte, 8)
	wb := s.db.NewWriteBatch()
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
			if err = wb.Set(pair.Key, pair.Value); err != nil {
				return err
			}
		}
		batchKv = batchKv[:0]
	}
	return wb.Flush()
}
func (s *BadgerStore) Hash() uint32 {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		h.Write(item.Key())
		_ = item.Value(func(val []byte) error {
			h.Write(val)
			return nil
		})
	}
	return h.Sum32()
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

func (s *BadgerStore) Load(key string) (value string, err error) {
	var v []byte
	if err = s.db.View(func(txn *badger.Txn) error {
		item, gErr := txn.Get([]byte(key))
		if gErr != nil {
			if gErr == badger.ErrKeyNotFound {
				return kverror.ErrKeyNotFound
			}
			return err
		}
		v, err = item.ValueCopy(v)
		return err
	}); err != nil {
		return "", err
	}
	value = string(v)
	return
}
