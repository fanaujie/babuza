package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"io"
)

type Entry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type ApplyResult struct {
	LogIndex uint64
	Response any
	Error    error
}

func (ar ApplyResult) IsEmpty() bool {
	return ar.LogIndex == 0 && ar.Response == nil && ar.Error == nil
}

type StateMachineSnapshotWriter interface {
	CreateStateMachineFile(fileTag string, compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error)
	AddStateMachineFileMetadata(fileTag string, metadata []byte) error
}

type StateMachineSnapshotReader interface {
	Open(fileTag string) (io.Reader, StateMachineFileDesc, error)
	Metadata() babuzapb.SnapshotMetadata
}

type BaseStateMachine interface {
	Apply(Entry) ApplyResult
	SaveSnapshot(StateMachineSnapshotContext, StateMachineSnapshotWriter) error
	RestoreFromSnapshot(StateMachineSnapshotReader) error
	Close() error
}

type MemoryStateMachine BaseStateMachine

type DiskStateMachine interface {
	Open() (uint64, bool, error)
	BaseStateMachine
	ConcurrentSnapshotStateMachine
}

type StateMachineSnapshotContext any

type ConcurrentSnapshotStateMachine interface {
	PrepareSnapshotContext() (StateMachineSnapshotContext, error)
	ReleaseSnapshotContext(StateMachineSnapshotContext) error
}

type ResponseSerializer interface {
	Serialize(io.Writer, any) error
	Deserialize(io.Reader) (any, error)
}

type SessionEnabledStateMachine interface {
	GetResponseSerializer() ResponseSerializer
}
