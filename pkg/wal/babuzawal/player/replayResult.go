package player

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type ParseEntryFormat int

type ReplayResult struct {
	metadata           []byte
	hardState          raftpb.HardState
	nextEntry          pb.WalNextEntry
	walSnapshots       []walpb.Snapshot
	lastLogFileDesc    iwal.LogFileDesc
	lastValidLogOffset int64
	lastValidLogCrc    uint32
	entryCollection    iwal.EntryCollection
}

func NewReplayResult(entryCollection iwal.EntryCollection) *ReplayResult {
	return &ReplayResult{
		entryCollection: entryCollection,
	}
}

func (r *ReplayResult) MatchSnapshot(startSnapshot walpb.Snapshot) error {
	for _, s := range r.walSnapshots {
		if startSnapshot.Index == s.Index {
			if startSnapshot.Term != s.Term {
				return errors.New(fmt.Sprintf("snapshot mismatch. expect(%v) real(%v)", startSnapshot, s))
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("not found snapshot. snapshot(%v)", startSnapshot))
}

func (r *ReplayResult) ForEachConfChangeEntries(visitor func(raftpb.Entry) error) error {
	return r.entryCollection.VisitEntry(raftpb.EntryConfChange, visitor)
}

func (r *ReplayResult) Metadata() []byte {
	return r.metadata
}
func (r *ReplayResult) LastLogFileDesc() iwal.LogFileDesc {
	return r.lastLogFileDesc
}

func (r *ReplayResult) HardState() raftpb.HardState {
	return r.hardState
}
func (r *ReplayResult) NextEntry() pb.WalNextEntry {
	return r.nextEntry
}
func (r *ReplayResult) WalSnapshots() []walpb.Snapshot {
	return r.walSnapshots
}
func (r *ReplayResult) LastValidLogOffset() int64 {
	return r.lastValidLogOffset
}
func (r *ReplayResult) LastValidLogCrc() uint32 {
	return r.lastValidLogCrc
}
func (r *ReplayResult) EntryCollection() iwal.EntryCollection {
	return r.entryCollection
}
func (r *ReplayResult) Reset() {
	r.entryCollection.ClearEntries()
	*r = ReplayResult{entryCollection: r.entryCollection}
}

func (r *ReplayResult) SetMetadata(metadata []byte) {
	r.metadata = make([]byte, len(metadata))
	copy(r.metadata, metadata)
}

func (r *ReplayResult) SetLastLogFileDesc(fileDesc iwal.LogFileDesc) {
	r.lastLogFileDesc = fileDesc
}

func (r *ReplayResult) UnmarshalHardState(buf []byte) error {
	var hs raftpb.HardState
	if err := hs.Unmarshal(buf); err != nil {
		return err
	}
	r.hardState = hs
	return nil
}

func (r *ReplayResult) UnmarshalNextEntry(buf []byte) error {
	var ne pb.WalNextEntry
	if err := ne.Unmarshal(buf); err != nil {
		return err
	}
	r.nextEntry = ne
	return nil
}

func (r *ReplayResult) SetNextIndex(nextIndex uint64) {
	r.nextEntry.NextIndex = nextIndex
}

func (r *ReplayResult) AppendWalSnapshots(snap walpb.Snapshot) {
	r.walSnapshots = append(r.walSnapshots, snap)
}
func (r *ReplayResult) SetLastValidLogFileOffset(offset int64) {
	r.lastValidLogOffset = offset
}
func (r *ReplayResult) IncreaseLastValidLogFileOffset(offset int64) {
	r.lastValidLogOffset += offset
}

func (r *ReplayResult) SetLastValidLogCrc(crc uint32) {
	r.lastValidLogCrc = crc
}
