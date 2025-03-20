package io

import (
	"bytes"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"path/filepath"
	"testing"
)

func TestReceiver_SaveChunk(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		//volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		t.Run("success", func(t *testing.T) {
			testCases := []struct {
				fileType  babuzapb.SnapshotFileType
				tag       string
				msgCount  int
				chunkSize int
			}{
				{
					fileType:  babuzapb.SnapshotFileType_Cluster,
					tag:       "one",
					msgCount:  4,
					chunkSize: 128,
				},
				{
					fileType:  babuzapb.SnapshotFileType_Session,
					tag:       "one",
					msgCount:  8,
					chunkSize: 256,
				},
				{
					fileType:  babuzapb.SnapshotFileType_StateMachine,
					tag:       "one",
					msgCount:  16,
					chunkSize: 512,
				},
			}

			for i, tc := range testCases {
				func(snapIndex uint64, c struct {
					fileType  babuzapb.SnapshotFileType
					tag       string
					msgCount  int
					chunkSize int
				}) {
					receiverDir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempReceive, snapIndex)
					assert.Nil(t, err)
					targetSrc := filepath.Join(tmpDir, receiverDir)
					assert.Nil(t, fs.CreateDirAndTouch(targetSrc))

					m := babuzapb.SnapshotMetadata{
						Version: 1,
						Snapshot: raftpb.Snapshot{
							Metadata: raftpb.SnapshotMetadata{
								ConfState: raftpb.ConfState{},
								Index:     snapIndex,
								Term:      1,
							},
						},
						Files: map[string]babuzapb.SnapshotFileDesc{
							c.tag: {
								FileType: c.fileType,
								Tag:      c.tag,
								FileSize: int64(c.msgCount * c.chunkSize),
							},
						},
					}
					metaCodec := &codec.Metadata{}
					r := NewReceiver(fs, targetSrc, m, metaCodec, &mockInstaller{vesion: 1}, NewFileValidator(fs, metaCodec))

					msgs, data := genSnapshotChunkMessage(c.fileType, c.tag, c.msgCount, c.chunkSize)
					for index := 0; index < c.msgCount; index++ {
						assert.Nil(t, r.SaveChunk(snapIndex, msgs[index]))
						if index == 0 {
							_, ok := r.chunkValidator[c.tag]
							assert.Equal(t, true, ok)
						} else if index+1 == c.msgCount {
							_, ok := r.chunkValidator[c.tag]
							assert.Equal(t, false, ok)
						}
					}

					receiveFileName, err := fs.PathHelper().SnapshotFileName(c.fileType, snapIndex, "one")
					assert.Nil(t, err)
					receivedFilePath := filepath.Join(targetSrc, receiveFileName)
					rf, err := fs.FileRead(receivedFilePath)
					assert.Nil(t, err)
					defer rf.Close()

					fileSize, err := fs.FileSize(receivedFilePath)
					assert.Nil(t, err)

					bw := bytes.NewBuffer(make([]byte, 0, fileSize))
					io.Copy(bw, rf)
					assert.Equal(t, data, bw.Bytes())
				}(uint64(i+1), tc)
			}
		})

		t.Run("failure cases", func(t *testing.T) {
			// Test ErrReceiverMismatchedSnapshotIndex
			snapIndex := uint64(10)
			version := uint64(1)
			receiverDir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempReceive, snapIndex)
			assert.Nil(t, err)
			targetSrc := filepath.Join(tmpDir, receiverDir)
			assert.Nil(t, fs.CreateDirAndTouch(targetSrc))

			metaCodec := &codec.Metadata{}
			r := NewReceiver(fs, targetSrc, babuzapb.SnapshotMetadata{
				Version: version,
				Snapshot: raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{
						ConfState: raftpb.ConfState{},
						Index:     snapIndex,
						Term:      1,
					},
				},
			}, metaCodec, &mockInstaller{vesion: version}, NewFileValidator(fs, metaCodec))

			err = r.SaveChunk(2, babuzapb.SnapshotChunkMessage{})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "mismatch snapshot index")

			// Test ErrUnknownFileType
			err = r.SaveChunk(snapIndex, babuzapb.SnapshotChunkMessage{
				FileType: 6,
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "unkonwn file type")
		})
	}
}

func TestReceiver_Commit(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
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
		version := uint64(1)
		snapIndex := uint64(1)
		snapTerm := uint64(1)
		receiverDir, err := fs.PathHelper().SnapshotFolderName(babuzapb.SnapshotFolderType_TempReceive, snapIndex)
		assert.Nil(t, err)
		targetSrc := filepath.Join(tmpDir, receiverDir)
		m := genSnapshotFiles(t, fs, targetSrc, version, snapTerm, snapIndex, fd)
		metaCodec := &codec.Metadata{}
		r := NewReceiver(fs, targetSrc, m, metaCodec, &mockInstaller{version}, NewFileValidator(fs, metaCodec))
		assert.Nil(t, r.Commit(snapIndex))
	}
}
