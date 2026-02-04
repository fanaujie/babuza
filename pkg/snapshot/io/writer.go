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

package io

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Installer interface {
	CommitSnapshot(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error
	SnapshotVersion() uint64
}

type MetadataEncoder interface {
	Encode(destW io.Writer, m babuzapb.SnapshotMetadata) error
}

type snapshotFileMetadata struct {
	filePath  string
	fileDesc  *babuzapb.SnapshotFileDesc
	crcWriter api.CrcFileWriter
}

type Writer struct {
	fs            api.SnapshotFileSystem
	snapshotFiles map[string]snapshotFileMetadata
	dir           string
	metadataEn    MetadataEncoder
	installer     Installer
	mu            sync.Mutex
	snapshotIndex uint64
}

func NewWriter(fs api.SnapshotFileSystem, dir string, metadataEn MetadataEncoder, installer Installer, snapshotIndex uint64) *Writer {
	return &Writer{
		fs:            fs,
		snapshotFiles: make(map[string]snapshotFileMetadata),
		dir:           dir,
		metadataEn:    metadataEn,
		installer:     installer,
		snapshotIndex: snapshotIndex,
	}
}

func (w *Writer) CreateStateMachineFile(fileTag string, compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error) {
	filename, err := w.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, w.snapshotIndex, fileTag)
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(w.dir, filename)
	return w.create(fileTag, fp, compression, babuzapb.SnapshotFileType_StateMachine)
}

func (w *Writer) AddStateMachineFileMetadata(fileTag string, metadata []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.snapshotFiles[fileTag]
	if !ok {
		return fmt.Errorf("snapshotor[index=%d]: not found tag(%s)", w.snapshotIndex, fileTag)
	}
	w.snapshotFiles[fileTag].fileDesc.Metadata = metadata
	return nil
}

func (w *Writer) CreateClusterFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error) {
	filename, err := w.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Cluster, w.snapshotIndex, "")
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(w.dir, filename)
	return w.create(filename, fp, compression, babuzapb.SnapshotFileType_Cluster)
}

func (w *Writer) CreateSessionFile(compression babuzapb.SnapshotFileCompressionType) (io.WriteCloser, error) {
	filename, err := w.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Session, w.snapshotIndex, "")
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(w.dir, filename)
	return w.create(filename, fp, compression, babuzapb.SnapshotFileType_Session)
}

func (w *Writer) Commit(snap raftpb.Snapshot) (babuzapb.SnapshotMetadata, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	sm := babuzapb.SnapshotMetadata{
		Version:  w.installer.SnapshotVersion(),
		Snapshot: snap,
		Files:    make(map[string]babuzapb.SnapshotFileDesc),
	}

	for _, ff := range w.snapshotFiles {
		// External files have no crcWriter; their integrity is the application's responsibility
		if ff.crcWriter != nil {
			ff.fileDesc.FileSize = int64(ff.crcWriter.FileSize())
			ff.fileDesc.FileCrc64 = ff.crcWriter.Crc()
		}
		sm.Files[ff.fileDesc.Tag] = *ff.fileDesc
	}

	filename, err := w.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, w.snapshotIndex, "")
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	fp := filepath.Join(w.dir, filename)

	fw, err := w.fs.FileWrite(fp)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	if err = w.metadataEn.Encode(fw, sm); err != nil {
		fw.Close()
		return babuzapb.SnapshotMetadata{}, err
	}
	if err = fw.Close(); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	return sm, w.installer.CommitSnapshot(babuzapb.SnapshotFolderType_TempWrite, w.snapshotIndex)
}

func (w *Writer) Dir() string {
	return w.dir
}

func (w *Writer) AddExternalFile(descriptor ibabuza.ExternalFileDescriptor) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	fileTag := descriptor.FileTag
	_, ok := w.snapshotFiles[fileTag]
	if ok {
		return fmt.Errorf("snapshotor[index=%d]: duplicated tag(%s)", w.snapshotIndex, fileTag)
	}

	w.snapshotFiles[fileTag] = snapshotFileMetadata{
		fileDesc: &babuzapb.SnapshotFileDesc{
			FileType:    babuzapb.SnapshotFileType_StateMachine,
			Tag:         fileTag,
			IsExternal:  true,
			LocationUri: descriptor.LocationUri,
			Metadata:    descriptor.Metadata,
		},
		// crcWriter is nil for external files since content is not written locally
	}
	return nil
}

func (w *Writer) create(fileTag string, filePath string, compression babuzapb.SnapshotFileCompressionType,
	fileType babuzapb.SnapshotFileType) (io.WriteCloser, error) {

	w.mu.Lock()
	defer w.mu.Unlock()

	_, ok := w.snapshotFiles[fileTag]
	if ok {
		return nil, fmt.Errorf("snapshotor[index=%d]: duplicated tag(%s)", w.snapshotIndex, fileTag)
	}
	crcW, err := w.fs.CrcFileWrite(filePath)
	if err != nil {
		return nil, err
	}
	cw, err := codec.CreateCompressor(compression, crcW)
	w.snapshotFiles[fileTag] = snapshotFileMetadata{
		filePath: filePath,
		fileDesc: &babuzapb.SnapshotFileDesc{
			FileType:        fileType,
			Tag:             fileTag,
			CompressionType: compression,
		},
		crcWriter: crcW,
	}
	return cw, nil
}
