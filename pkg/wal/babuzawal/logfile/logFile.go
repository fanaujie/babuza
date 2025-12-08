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

package logfile

import (
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile/page"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type LogFile struct {
	pw  *page.Writer
	enc *codec.Encoder
}

func New(pw *page.Writer, enc *codec.Encoder) *LogFile {
	return &LogFile{
		pw:  pw,
		enc: enc,
	}
}

func (l *LogFile) Truncate() error {
	return l.pw.Truncate()
}

func (l *LogFile) Sync(enableSync bool) error {
	return l.pw.Sync(enableSync)
}

func (l *LogFile) Close() error {
	return l.pw.Close()
}

func (l *LogFile) Offset() int64 {
	return int64(l.pw.CurrentOffset())
}

func (l *LogFile) LastCrc() uint32 {
	return l.enc.LastCrc()
}

func (l *LogFile) DoCycle() bool {
	return l.pw.CheckCycle()
}

func (l *LogFile) Crc(crc uint32) error {
	return codec.Encode(l.enc, pb.LogTypeCrc, 4, codec.CrcLog(crc))
}

func (l *LogFile) Metadata(metadata []byte) error {
	return codec.Encode(l.enc, pb.LogTypeMetadata, len(metadata), codec.SliceBytes(metadata))
}

func (l *LogFile) HardState(state raftpb.HardState) error {
	return codec.Encode(l.enc, pb.LogTypeHardState, state.Size(), (codec.HardStateLog)(state))
}

func (l *LogFile) Snapshot(snap walpb.Snapshot) error {
	if err := walpb.ValidateSnapshotForWrite(&snap); err != nil {
		return err
	}
	return codec.Encode(l.enc, pb.LogTypeSnapshot, snap.Size(), (codec.WalSnapshotLog)(snap))
}

func (l *LogFile) NextEntry(nextEntry pb.WalNextEntry) error {
	return codec.Encode(l.enc, pb.LogTypeNextEntry, nextEntry.Size(), (codec.WalNextEntryLog)(nextEntry))
}

func (l *LogFile) Entry(entryType pb.LogType, entryData []byte) error {
	return codec.Encode(l.enc, entryType, len(entryData), codec.SliceBytes(entryData))
}
