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


package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"hash/crc32"
)

type EncodeLog interface {
	Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error)
}

type CrcLog uint32

func (l CrcLog) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	binary.LittleEndian.PutUint32(buf, uint32(l))
	return uint32(l), nil
}

type SliceBytes []byte

//TODO: panic? if encode was failure.

func (l SliceBytes) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	if n := copy(buf, l); n != logSize {
		return 0, errors.New(fmt.Sprintf("copy size (%d) mismatched with data size (%d)", n, logSize))
	}
	return crc32.Update(lastCrc, iwal.Crc32Table, buf[:logSize]), nil
}

type HardStateLog raftpb.HardState

func (l HardStateLog) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	if _, err := (*raftpb.HardState)(&l).MarshalTo(buf); err != nil {
		return 0, err
	}
	return crc32.Update(lastCrc, iwal.Crc32Table, buf[:logSize]), nil
}

type WalSnapshotLog walpb.Snapshot

func (l WalSnapshotLog) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	if _, err := (*walpb.Snapshot)(&l).MarshalTo(buf); err != nil {
		return 0, err
	}
	return crc32.Update(lastCrc, iwal.Crc32Table, buf[:logSize]), nil
}

type WalNextEntryLog pb.WalNextEntry

func (l WalNextEntryLog) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	if _, err := (*pb.WalNextEntry)(&l).MarshalTo(buf); err != nil {
		return 0, err
	}
	return crc32.Update(lastCrc, iwal.Crc32Table, buf[:logSize]), nil
}
