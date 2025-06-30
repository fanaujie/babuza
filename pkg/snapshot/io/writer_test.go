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
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"path/filepath"
	"testing"
)

func writeData(t *testing.T, w *Writer, tag string, compression babuzapb.SnapshotFileCompressionType, dataSize int) {
	wc, err := w.CreateStateMachineFile(tag, compression)
	assert.Nil(t, err)
	defer wc.Close()
	d := make([]byte, dataSize)
	rand.Read(d)
	_, err = wc.Write(d)
	assert.Nil(t, err)
}

func TestWriter_Create(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		cw, err := w.CreateStateMachineFile("one", babuzapb.SnapshotFileCompression_None)
		assert.Nil(t, err)
		assert.Nil(t, cw.Close())
		stateMachine1, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "one")
		stateMachine1Path := filepath.Join(targetDir, stateMachine1)
		assert.Equal(t, true, fs.ExistFilePath(stateMachine1Path))
	}

}

func TestWriter_Create_Fail(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		writeData(t, w, "one", babuzapb.SnapshotFileCompression_None, 1024)
		_, err = w.CreateStateMachineFile("one", babuzapb.SnapshotFileCompression_None)
		assert.Errorf(t, err, fmt.Sprintf("snapshotor[index=%d]: duplicated tag(%s)", 1, "one"))
	}
}

func TestWriter_Write(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		writeData(t, w, "one", babuzapb.SnapshotFileCompression_None, 1024)
		writeData(t, w, "two", babuzapb.SnapshotFileCompression_Snappy, 1024)
		stateMachine1, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "one")
		stateMachine1Path := filepath.Join(targetDir, stateMachine1)
		stateMachine2, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "two")
		stateMachine2Path := filepath.Join(targetDir, stateMachine2)

		assert.Equal(t, true, fs.ExistFilePath(stateMachine1Path))
		assert.Equal(t, true, fs.ExistFilePath(stateMachine2Path))
	}

}

func TestWriter_AddMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		wc, err := w.CreateStateMachineFile("one", babuzapb.SnapshotFileCompression_None)
		assert.Nil(t, err)
		defer wc.Close()
		metadata := []byte("hello world!")
		assert.Nil(t, w.AddStateMachineFileMetadata("one", metadata))

		m, ok := w.snapshotFiles["one"]
		assert.Equal(t, true, ok)
		assert.Equal(t, metadata, m.fileDesc.Metadata)
	}
}

func TestWriter_AddMetadata_Fail(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		wc, err := w.CreateStateMachineFile("one", babuzapb.SnapshotFileCompression_None)
		assert.Nil(t, err)
		defer wc.Close()
		metadata := []byte("hello world!")
		assert.Errorf(t, w.AddStateMachineFileMetadata("two", metadata), fmt.Sprintf("snapshotor: not found tag(%s)", "two"))
	}

}

func TestWriter_CreateNoneStateMachineFile(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		w := NewWriter(fs, targetDir, &codec.Metadata{}, &mockInstaller{vesion: 1}, 1)
		for _, tc := range []babuzapb.SnapshotFileType{
			babuzapb.SnapshotFileType_Cluster,
			babuzapb.SnapshotFileType_Session} {
			func(sft babuzapb.SnapshotFileType) {
				switch sft {
				case babuzapb.SnapshotFileType_Cluster:
					wc, err := w.CreateClusterFile(babuzapb.SnapshotFileCompression_None)
					assert.Nil(t, err)
					defer wc.Close()
					filename, err := fs.PathHelper().SnapshotFileName(sft, 1, "")
					_, ok := w.snapshotFiles[filename]
					assert.Equal(t, true, ok)
				case babuzapb.SnapshotFileType_Session:
					wc, err := w.CreateSessionFile(babuzapb.SnapshotFileCompression_None)
					assert.Nil(t, err)
					defer wc.Close()
					filename, err := fs.PathHelper().SnapshotFileName(sft, 1, "")
					_, ok := w.snapshotFiles[filename]
					assert.Equal(t, true, ok)
				}
			}(tc)
		}
	}
}

func TestWriter_Commit(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		snapshotTerm := uint64(100)
		snapshotIndex := uint64(101)
		snapshotVersion := uint64(1)

		fdFiles := []snapFileDesc{
			{
				fileType:        babuzapb.SnapshotFileType_StateMachine,
				tag:             "one",
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
				metadata:        []byte("hello world one"),
			},
			{
				fileType:        babuzapb.SnapshotFileType_StateMachine,
				tag:             "two",
				compressionType: babuzapb.SnapshotFileCompression_Snappy,
				dataSize:        1024,
				metadata:        []byte("hello world two"),
			},
			{
				fileType:        babuzapb.SnapshotFileType_Cluster,
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
			},
			{
				fileType:        babuzapb.SnapshotFileType_Session,
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
			},
		}
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, snapshotIndex)
		assert.Nil(t, err)
		genSnapshotFiles(t, fs, targetDir, snapshotVersion, snapshotTerm, snapshotIndex, fdFiles)

		fileName, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, snapshotIndex, "")
		metadataPath := filepath.Join(targetDir, fileName)

		assert.Equal(t, true, fs.ExistFilePath(metadataPath))
		f := codec.Metadata{}
		r, err := fs.FileRead(metadataPath)
		assert.Nil(t, err)
		defer r.Close()
		sm, err := f.Decode(r)
		assert.Nil(t, err)
		assert.Equal(t, snapshotTerm, sm.Snapshot.Metadata.Term)
		assert.Equal(t, snapshotIndex, sm.Snapshot.Metadata.Index)
		assert.Equal(t, snapshotVersion, sm.Version)
		for _, fm := range fdFiles {
			switch fm.fileType {
			case babuzapb.SnapshotFileType_StateMachine:
				filename, err := fs.PathHelper().SnapshotFileName(fm.fileType, snapshotIndex, fm.tag)
				assert.Nil(t, err)
				fsmFilePath := filepath.Join(targetDir, filename)
				assert.Equal(t, true, fs.ExistFilePath(fsmFilePath))
				assert.Equal(t, fm.metadata, sm.Files[fm.tag].Metadata)
			case babuzapb.SnapshotFileType_Cluster:
			case babuzapb.SnapshotFileType_Session:
				filename, err := fs.PathHelper().SnapshotFileName(fm.fileType, snapshotIndex, "")
				assert.Nil(t, err)
				assert.Equal(t, true, fs.ExistFilePath(filepath.Join(targetDir, filename)))
			default:
				assert.Fail(t, "not support file type")
			}
		}

	}
}
