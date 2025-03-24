package kvstore

import (
	"github.com/dgraph-io/badger/v4"
)

type MemoryStoreWithConcurrentSnapshot struct {
	*BadgerStore
}

func NewMemoryStoreWithConcurrentSnapshot() *MemoryStoreWithConcurrentSnapshot {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(err)
	}
	return &MemoryStoreWithConcurrentSnapshot{
		BadgerStore: NewBadgerStore(db),
	}
}
