package kvstore

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v3"
)

type Disk struct {
	*BadgerStore
}

func NewDisk(dataDir string) *Disk {
	db, err := badger.Open(badger.DefaultOptions(dataDir))
	if err != nil {
		panic(err)
	}
	return &Disk{
		BadgerStore: NewBadgerStore(db),
	}
}

func (d *Disk) Open() (lastApplyIndex uint64, rebuild bool, err error) {
	err = d.db.View(func(txn *badger.Txn) error {
		item, gErr := txn.Get(applyIndexPrefix)
		if gErr != nil {
			return gErr
		}
		return item.Value(func(val []byte) error {
			lastApplyIndex = binary.LittleEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		rebuild = true
	}
	return
}
