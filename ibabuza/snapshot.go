package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
)

type StateMachineFileDesc struct {
	Tag      string
	Metadata []byte
	FilePath string
}

type SnapshotReader interface {
	Open(fileTag string) (io.Reader, StateMachineFileDesc, error)
	Close() error
	ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error
	Metadata() babuzapb.SnapshotMetadata
	Cluster() (io.Reader, error)
	Session() (io.Reader, error)
	CreateTarArchiveReader() (io.ReadCloser, error)
}

type AtomicSnapshotWriter interface {
	CreateStateMachineFile(fileTag string, compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
	AddStateMachineFileMetadata(fileTag string, metadata []byte) error
	CreateClusterFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
	CreateSessionFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
	Commit(raftpb.Snapshot) (babuzapb.SnapshotMetadata, error)
}
type AtomicSnapshotReceiver interface {
	SaveChunk(snapshotIndex uint64, msg babuzapb.SnapshotChunkMessage) error
	DeleteDir() error
	Commit(snapshotIndex uint64) error
}

type SnapshotPurger interface {
	Start()
}

type SnapshotManager interface {
	ScanInstalledSnapshots(removeUnfinishedSnapshotDir bool) error
	LoadLastValidSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error)
	CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex uint64) (AtomicSnapshotWriter, error)
	CreateInstalledSnapshotReader(snapshotIndex uint64, validateFsmFiles bool) (SnapshotReader, error)
	CreateAtomicSnapshotReceiver(metadata babuzapb.SnapshotMetadata) (AtomicSnapshotReceiver, error)
	Purger() SnapshotPurger
	Purge(snapshot raftpb.Snapshot) error
	Close() error
}
