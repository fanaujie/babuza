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

package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

const snapshotTag = "simple-kv"

var ErrKeyNotFound = errors.New("key not found")

type SimpleKVStore struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewSimpleKVStore() *SimpleKVStore {
	return &SimpleKVStore{
		data: make(map[string]string),
	}
}

func (s *SimpleKVStore) Apply(e ibabuza.Entry) ibabuza.ApplyResult {
	cmd, err := DecodeCommand(e.Command)
	if err != nil {
		return ibabuza.ApplyResult{
			LogIndex: e.Index,
			Error:    err,
		}
	}

	switch cmd.Type {
	case CmdSet:
		s.mu.Lock()
		s.data[cmd.Key] = cmd.Value
		s.mu.Unlock()
		return ibabuza.ApplyResult{
			LogIndex: e.Index,
			Response: cmd.Value,
		}
	default:
		return ibabuza.ApplyResult{
			LogIndex: e.Index,
			Error:    errors.New("unknown command"),
		}
	}
}

func (s *SimpleKVStore) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	wc, err := writer.CreateStateMachineFile(snapshotTag, babuzapb.SnapshotFileCompression_None)
	if err != nil {
		return err
	}
	defer wc.Close()

	data, err := json.Marshal(s.data)
	if err != nil {
		return err
	}

	// Write length prefix
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(data)))
	if _, err = wc.Write(buf); err != nil {
		return err
	}

	// Write data
	if _, err = wc.Write(data); err != nil {
		return err
	}

	return nil
}

func (s *SimpleKVStore) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, _, err := reader.Open(snapshotTag)
	if err != nil {
		return err
	}

	// Read length prefix
	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return err
	}
	dataLen := binary.LittleEndian.Uint64(buf)

	// Read data
	data := make([]byte, dataLen)
	if _, err = io.ReadFull(r, data); err != nil {
		return err
	}

	s.data = make(map[string]string)
	if err = json.Unmarshal(data, &s.data); err != nil {
		return err
	}

	return nil
}

func (s *SimpleKVStore) Close() error {
	return nil
}

func (s *SimpleKVStore) Query(key any) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k, ok := key.(string)
	if !ok {
		return nil, errors.New("key must be a string")
	}

	v, ok := s.data[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}
