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


package lsmtwal

import (
	"encoding/binary"
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	keyHardState = uint64(1)
	keySnapshot  = uint64(2)
	keyEntry     = uint64(3)
	keyMetadata  = uint64(4)
)

type keyPrefix struct {
	hardState []byte
	snapshot  []byte
	entry     []byte
	metadata  []byte
}

type WalManagerType string

const (
	WalManagerTypeBadger WalManagerType = "badger"
	WalManagerTypePebble WalManagerType = "pebble"
)

type Config struct {
	InMemory    bool
	WalDir      string
	ManagerType WalManagerType
}

func newKeyPrefix(groupID ibabuza.RaftGroupID) *keyPrefix {
	createKey := func(typeID uint64) []byte {
		if groupID == 0 {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, typeID)
			return key
		}
		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key[:8], typeID)
		binary.BigEndian.PutUint64(key[8:], uint64(groupID))
		return key
	}
	return &keyPrefix{
		hardState: createKey(keyHardState),
		snapshot:  createKey(keySnapshot),
		entry:     createKey(keyEntry),
		metadata:  createKey(keyMetadata),
	}
}

func NewWalManager(config Config, logger ibabuza.Logger) ibabuza.WalManager {
	switch config.ManagerType {
	case WalManagerTypePebble:
		return NewPebbleWalManager(config, logger)
	case WalManagerTypeBadger:
		fallthrough
	default:
		return NewBadgerWalManager(config, logger)
	}
}

func isEmptyHardState(st raftpb.HardState) bool {
	return st.Term == 0 && st.Vote == 0 && st.Commit == 0
}

func isEmptySnapshot(snap raftpb.Snapshot) bool {
	return snap.Metadata.Index == 0
}
