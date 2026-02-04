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

package ibabuza

import (
	"io"

	"github.com/fanaujie/babuza/ibabuza/babuzapb"
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
	AddExternalFile(descriptor ExternalFileDescriptor) error
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
	Query(key any) (any, error)
}

type MemoryStateMachine BaseStateMachine

type DiskStateMachine interface {
	Open() (uint64, bool)
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
