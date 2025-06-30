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
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestReader_Create(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		_ = genSnapshotFiles(t, fs, targetDir, 1, 1, 1, []snapFileDesc{
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
		})

		m, err := NewFileValidator(fs, &codec.Metadata{}).GetMetadataFile(targetDir)
		assert.Nil(t, err)
		c := NewReader(fs, targetDir, m, &codec.Metadata{})
		assert.NotNil(t, c)
	}
}

func TestReader_Open(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		fd := []snapFileDesc{
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
		_ = genSnapshotFiles(t, fs, targetDir, 1, 1, 1, fd)
		m, err := NewFileValidator(fs, &codec.Metadata{}).GetMetadataFile(targetDir)
		assert.Nil(t, err)
		c := NewReader(fs, targetDir, m, &codec.Metadata{})
		assert.NotNil(t, c)
		defer c.Close()
		for _, file := range fd {
			switch file.fileType {
			case babuzapb.SnapshotFileType_StateMachine:
				_, fileDesc, err := c.Open(file.tag)
				assert.Nil(t, err)
				stateMachinePath, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, file.tag)
				assert.Nil(t, err)
				assert.Equal(t, fileDesc.Tag, file.tag)
				assert.Equal(t, fileDesc.Metadata, m.Files[file.tag].Metadata)
				assert.Equal(t, fileDesc.FilePath, filepath.Join(targetDir, stateMachinePath))
			case babuzapb.SnapshotFileType_Cluster:
				_, err = c.Cluster()
				assert.Nil(t, err)
			case babuzapb.SnapshotFileType_Session:
				_, err = c.Session()
				assert.Nil(t, err)
			default:
				assert.Fail(t, "not support file type")
			}
		}
	}
}

func TestReader_ForEachFile(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
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
				fileType:        babuzapb.SnapshotFileType_StateMachine,
				tag:             "three",
				compressionType: babuzapb.SnapshotFileCompression_Snappy,
				dataSize:        1024,
				metadata:        []byte("hello world three"),
			},
			{
				fileType:        babuzapb.SnapshotFileType_Cluster,
				tag:             "cluster",
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
			},
			{
				fileType:        babuzapb.SnapshotFileType_Session,
				tag:             "session",
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
			},
		}
		metadata := genSnapshotFiles(t, fs, targetDir, 1, 1, 1, fdFiles)
		vMetadata, err := NewFileValidator(fs, &codec.Metadata{}).GetMetadataFile(targetDir)
		assert.Nil(t, err)
		assert.Equal(t, metadata, vMetadata)
		r := NewReader(fs, targetDir, metadata, &codec.Metadata{})
		assert.NotNil(t, r)

		assert.Nil(t, r.ForEachFile(func(reader io.Reader, fileDesc babuzapb.SnapshotFileDesc) error {
			check := metadata.Files[fileDesc.Tag]
			assert.Equal(t, check.Metadata, fileDesc.Metadata)
			assert.Equal(t, check.Tag, fileDesc.Tag)
			assert.Equal(t, check.CompressionType, fileDesc.CompressionType)
			data, err := io.ReadAll(reader)
			assert.Nil(t, err)
			assert.Equal(t, int64(len(data)), fileDesc.FileSize)
			return nil
		}))
	}
}

func TestReader_ForEachFile_Fail(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		targetDir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		stateMachineFiles := []snapFileDesc{
			{
				fileType:        babuzapb.SnapshotFileType_StateMachine,
				tag:             "one",
				compressionType: babuzapb.SnapshotFileCompression_None,
				dataSize:        1024,
				metadata:        []byte("hello world one"),
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
		genSnapshotFiles(t, fs, targetDir, 1, 1, 1, stateMachineFiles)
		m, err := NewFileValidator(fs, &codec.Metadata{}).GetMetadataFile(targetDir)
		assert.Nil(t, err)
		r := NewReader(fs, targetDir, m, &codec.Metadata{})
		assert.NotNil(t, r)
		stateMachinePath, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "one")
		assert.Nil(t, err)
		assert.Nil(t, fs.RemoveFilePath(filepath.Join(targetDir, stateMachinePath)))
		assert.Error(t, r.ForEachFile(func(io.Reader, babuzapb.SnapshotFileDesc) error {
			return nil
		}))
	}
}
