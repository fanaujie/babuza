// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package kvstore

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
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

func (d *Disk) Open() (lastApplyIndex uint64, rebuild bool) {
	err := d.db.View(func(txn *badger.Txn) error {
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
