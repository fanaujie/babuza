package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type Wal interface {
	SetUnsafeNoFsync()
	Save(raftpb.HardState, []raftpb.Entry) error
	SaveSnapshot(raftpb.Snapshot) error
	Purge(raftpb.Snapshot) error
	Sync() error
	Close() error
}

type ReplayWalResult interface {
	Metadata() []byte
	HardState() raftpb.HardState
	ForEachConfChangeEntries(func(raftpb.Entry) error) error
}

type WalPurger interface {
	Start()
}

type WalManager interface {
	FindSnapshot() ([]walpb.Snapshot, error)
	CreateWal(metadata babuzapb.WalMetadata) (EntryStorage, Wal, error)
	ReplayWal(snapshot *raftpb.Snapshot, deleteUncommitted bool) (EntryStorage,
		Wal, ReplayWalResult, error)
	HasExistingWals() (bool, error)
	Purger() WalPurger
	Close() error
}

type EntryStorage interface {
	raft.Storage
	SetHardState(raftpb.HardState) error
	Append([]raftpb.Entry) error
	ApplySnapshot(raftpb.Snapshot) error
	CreateSnapshot(snapshotIndex uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(compactIndex uint64) error
}
