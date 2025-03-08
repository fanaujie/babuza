package logfile

import (
	codec2 "github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile/page"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type LogFile struct {
	pw  *page.Writer
	enc *codec2.Encoder
}

func New(pw *page.Writer, enc *codec2.Encoder) *LogFile {
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
	return codec2.Encode(l.enc, pb.LogTypeCrc, 4, codec2.CrcLog(crc))
}

func (l *LogFile) Metadata(metadata []byte) error {
	return codec2.Encode(l.enc, pb.LogTypeMetadata, len(metadata), codec2.SliceBytes(metadata))
}

func (l *LogFile) HardState(state raftpb.HardState) error {
	return codec2.Encode(l.enc, pb.LogTypeHardState, state.Size(), (codec2.HardStateLog)(state))
}

func (l *LogFile) Snapshot(snap walpb.Snapshot) error {
	if err := walpb.ValidateSnapshotForWrite(&snap); err != nil {
		return err
	}
	return codec2.Encode(l.enc, pb.LogTypeSnapshot, snap.Size(), (codec2.WalSnapshotLog)(snap))
}

func (l *LogFile) NextEntry(nextEntry pb.WalNextEntry) error {
	return codec2.Encode(l.enc, pb.LogTypeNextEntry, nextEntry.Size(), (codec2.WalNextEntryLog)(nextEntry))
}

func (l *LogFile) Entry(entryType pb.LogType, entryData []byte) error {
	return codec2.Encode(l.enc, entryType, len(entryData), codec2.SliceBytes(entryData))
}
