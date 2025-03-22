package kvstore

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
)

type MemoryStoreWithConcurrentSnapshotAndSession struct {
	*BadgerStore
}

func NewMemoryStoreWithConcurrentSnapshotAndSession() *MemoryStoreWithConcurrentSnapshotAndSession {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(err)
	}
	return &MemoryStoreWithConcurrentSnapshotAndSession{
		BadgerStore: NewBadgerStore(db),
	}
}

func (m *MemoryStoreWithConcurrentSnapshotAndSession) GetResponseSerializer() ibabuza.ResponseSerializer {
	return NewResultSerializer()
}
