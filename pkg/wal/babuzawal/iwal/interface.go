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


package iwal

import (
	"fmt"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"hash/crc32"
	"io"
)

var (
	Crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

type LogFileDesc struct {
	Id            uint64
	StartLogIndex uint64
	IsTempFile    bool
}

func (desc LogFileDesc) GetLogFileName() string {
	return fmt.Sprintf("%016x-%016x.wal", desc.Id, desc.StartLogIndex)
}
func (desc LogFileDesc) GetTempLogFileName() string {
	return fmt.Sprintf("%016x-%016x.wal.tmp", desc.Id, desc.StartLogIndex)
}

type LogFileReader interface {
	io.ReadCloser
	CurrentLogFileDesc() LogFileDesc
}

type WalPlayer interface {
}

type LogFileManager interface {
	OpenLogFile(id uint64, seekTo int64, initCrc uint32) (LogFile, error)
	CreateNextTempLogFile(id uint64, startLogIndex uint64) (LogFile, error)
	FinalizeTempLogFile(id uint64) error
	Purge(snapshotIndex uint64) error
	LastLogFileDesc() (LogFileDesc, error)
	ReadEntriesData(readMetadata []walbase.EntryIndex[storage.EntryIndexMetadata], ents []raftpb.Entry) error
	SyncWalFolder() error
	Close() error
}

type LogFile interface {
	NextEntry(nextEntry pb.WalNextEntry) error
	Entry(entryType pb.LogType, entryData []byte) error
	Crc(crc uint32) error
	Metadata(metadata []byte) error
	Snapshot(snap walpb.Snapshot) error
	Sync(enableSync bool) error
	Offset() int64
	LastCrc() uint32
	HardState(state raftpb.HardState) error
	Truncate() error
	DoCycle() bool
	Close() error
}

type EntryCollection interface {
	Decode(fileId, snapshotIndex uint64, logType pb.LogType, logBuf []byte, entryDataCapacity int64, r ReplayWalResult) error
	Entries() (interface{}, error)
	VisitEntry(entryType raftpb.EntryType, visitor func(raftpb.Entry) error) error
	DeleteUncommittedEntry(commitIndex uint64) error
	ClearEntries() error
}

type ReplayLastLogFileResult interface {
	Metadata() []byte
	LastLogFileDesc() LogFileDesc
	HardState() raftpb.HardState
	NextEntry() pb.WalNextEntry
	LastValidLogOffset() int64
	LastValidLogCrc() uint32
}

type ReplayWalResult interface {
	Metadata() []byte
	LastLogFileDesc() LogFileDesc
	HardState() raftpb.HardState
	NextEntry() pb.WalNextEntry
	WalSnapshots() []walpb.Snapshot
	LastValidLogOffset() int64
	LastValidLogCrc() uint32
	EntryCollection() EntryCollection
	Reset()
	SetMetadata([]byte)
	SetLastLogFileDesc(LogFileDesc)
	UnmarshalHardState([]byte) error
	UnmarshalNextEntry([]byte) error
	SetNextIndex(nextIndex uint64)
	AppendWalSnapshots(walpb.Snapshot)
	SetLastValidLogFileOffset(int64)
	IncreaseLastValidLogFileOffset(int64)
	SetLastValidLogCrc(uint32)
}
