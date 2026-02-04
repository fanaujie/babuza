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
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
)

type ExternalFileDescriptor struct {
	FileTag     string
	LocationUri string
	Metadata    []byte
}

type ExternalFileHandler interface {
	OnSnapshotReceived(snapshotIndex uint64, files []ExternalFileDescriptor) error
}

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
	AddExternalFile(descriptor ExternalFileDescriptor) error
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
	SetExternalFileHandler(handler ExternalFileHandler)
	GetExternalFileMetadata(snapshotIndex uint64, fileTag string) (babuzapb.SnapshotFileDesc, error)
}
