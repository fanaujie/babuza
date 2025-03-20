package io

import (
	"bytes"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"io"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestValidator_ValidateMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.FileSystem{
		volatile.NewFileSystem(),
		durable.NewFileSystem(),
	} {
		dir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		targetDir := filepath.Join(tmpDir, dir)
		// Generate test snapshot files
		genSnapshotFiles(t, fs, targetDir, 1, 1, 1, []snapFileDesc{
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

		f := NewFileValidator(fs, &codec.Metadata{})
		_, err = f.ValidateMetadataFile(targetDir)
		assert.Nil(t, err)
	}
}

func TestValidator_ValidateFsmFile(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.FileSystem{
		volatile.NewFileSystem(),
		durable.NewFileSystem(),
	} {
		dir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		targetDir := filepath.Join(tmpDir, dir)
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
		genSnapshotFiles(t, fs, targetDir, 1, 1, 1, fd)

		f := NewFileValidator(fs, &codec.Metadata{})
		m, err := f.ValidateMetadataFile(targetDir)
		assert.Nil(t, err)
		assert.Nil(t, f.ValidateSnapshotFiles(targetDir, m))
	}
}

func TestValidator_ValidateMetadata_Failures(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.FileSystem{
		volatile.NewFileSystem(),
		durable.NewFileSystem(),
	} {

		t.Run("more than one metadata file", func(t *testing.T) {
			dir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempWrite, 1)
			assert.Nil(t, err)
			targetDir := filepath.Join(tmpDir, dir)
			assert.Nil(t, fs.CreateDirAndTouch(targetDir))
			f := NewFileValidator(fs, &codec.Metadata{})

			metadataFileName, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, 1, "")
			assert.Nil(t, err)
			appendMetadataFilePath1 := filepath.Join(targetDir, metadataFileName)
			mf1, err := fs.FileWrite(appendMetadataFilePath1)
			assert.Nil(t, err)
			defer fs.RemoveFilePath(appendMetadataFilePath1)
			assert.Nil(t, mf1.Close())
			metadataFileName2, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, 2, "")
			appendMetadataFilePath2 := filepath.Join(targetDir, metadataFileName2)
			mf2, err := fs.FileWrite(appendMetadataFilePath2)
			assert.Nil(t, err)
			defer fs.RemoveFilePath(appendMetadataFilePath2)
			assert.Nil(t, mf2.Close())

			_, err = f.ValidateMetadataFile(targetDir)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "found more than one metadata file")
		})

		t.Run("metadata file not found", func(t *testing.T) {
			dir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempWrite, 10)
			assert.Nil(t, err)
			targetDir := filepath.Join(tmpDir, dir)
			assert.Nil(t, fs.CreateDirAndTouch(targetDir))
			f := NewFileValidator(fs, &codec.Metadata{})
			_, err = f.ValidateMetadataFile(targetDir)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found metadata file")
		})
	}
}

func TestValidator_ChunkValidation(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.FileSystem{
		volatile.NewFileSystem(),
		durable.NewFileSystem(),
	} {
		dir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		targetDir := filepath.Join(tmpDir, dir)
		assert.Nil(t, fs.CreateDirAndTouch(targetDir))

		t.Run("successful chunk validation", func(t *testing.T) {
			stateMachineFileName, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "one")
			assert.Nil(t, err)
			validateFilePath := filepath.Join(targetDir, stateMachineFileName)
			v := NewChunkValidator(fs, validateFilePath)
			msgs, data := genSnapshotChunkMessage(babuzapb.SnapshotFileType_StateMachine, "one", 8, 8)

			for _, m := range msgs {
				assert.Nil(t, v.ValidateAndAppend(m))
			}

			rf, err := fs.FileRead(validateFilePath)
			assert.Nil(t, err)
			defer rf.Close()

			fileSize, err := fs.FileSize(validateFilePath)
			assert.Nil(t, err)

			bw := bytes.NewBuffer(make([]byte, 0, fileSize))
			io.Copy(bw, rf)
			assert.Equal(t, data, bw.Bytes())
		})

		t.Run("failure cases", func(t *testing.T) {
			stateMachineFileName, err := fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, 1, "one")
			assert.Nil(t, err)
			validateFilePath := filepath.Join(targetDir, stateMachineFileName)
			v := NewChunkValidator(fs, validateFilePath)

			table := crc32.MakeTable(crc32.Castagnoli)
			data := make([]byte, 64)
			rand.Read(data)

			// Test unexpected chunk ID
			msg := babuzapb.SnapshotChunkMessage{
				FileType: babuzapb.SnapshotFileType_StateMachine,
				FileTag:  "one",
				Id:       2,
				Data:     nil,
			}
			err = v.ValidateAndAppend(msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not expectd chunk id")

			// Test already finished chunk
			msg = babuzapb.SnapshotChunkMessage{
				FileType:      babuzapb.SnapshotFileType_StateMachine,
				FileTag:       "one",
				Id:            1,
				Data:          data,
				ContinueCrc32: crc32.Update(0, table, data),
				LastChunk:     true,
			}
			assert.Nil(t, v.ValidateAndAppend(msg))
			err = v.ValidateAndAppend(msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "already finished chunk")
		})
	}
}
