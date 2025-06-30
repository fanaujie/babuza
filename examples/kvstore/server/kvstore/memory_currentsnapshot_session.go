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
