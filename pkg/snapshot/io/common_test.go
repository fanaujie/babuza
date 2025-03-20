package io

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"hash/crc32"
	"io"
	"math/rand"
	"testing"
)

type mockInstaller struct {
	vesion uint64
}

func (m *mockInstaller) CommitSnapshot(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	return nil
}

func (m *mockInstaller) SnapshotVersion() uint64 {
	return m.vesion
}

type snapFileDesc struct {
	fileType        babuzapb.SnapshotFileType
	tag             string
	compressionType babuzapb.SnapshotFileCompressionType
	dataSize        int
	metadata        []byte
}

var (
	snapshotMetadata = babuzapb.SnapshotMetadata{
		Version: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 1,
				Term:  1,
			},
		},
		Files: map[string]babuzapb.SnapshotFileDesc{
			"one": {
				FileType:  babuzapb.SnapshotFileType_StateMachine,
				Tag:       "one",
				FileSize:  1024,
				FileCrc64: 0,
				Metadata:  []byte{1, 2, 3, 4, 5},
			},
		},
	}
)

func genSnapshotChunkMessage(fileType babuzapb.SnapshotFileType, tag string, msgCount int, chunkSize int) ([]babuzapb.SnapshotChunkMessage, []byte) {
	table := crc32.MakeTable(crc32.Castagnoli)
	data := make([]byte, chunkSize*msgCount)
	rand.Read(data)
	continueCrc32 := uint32(0)
	var msgs []babuzapb.SnapshotChunkMessage
	for i := 0; i < msgCount; i++ {
		d := data[chunkSize*i : chunkSize*i+chunkSize]
		continueCrc32 = crc32.Update(continueCrc32, table, d)
		msgs = append(msgs, babuzapb.SnapshotChunkMessage{
			FileType:      fileType,
			FileTag:       tag,
			Id:            int64(i + 1),
			Data:          d,
			ContinueCrc32: continueCrc32,
			LastChunk:     i == msgCount-1,
		})
	}
	return msgs, data
}

func writeRandomData(t *testing.T, w ibabuza.AtomicSnapshotWriter, fd snapFileDesc) {
	var wc io.WriteCloser
	var err error
	switch fd.fileType {
	case babuzapb.SnapshotFileType_StateMachine:
		wc, err = w.CreateStateMachineFile(fd.tag, fd.compressionType)
		assert.Nil(t, err)
	case babuzapb.SnapshotFileType_Session:
		wc, err = w.CreateClusterFile(babuzapb.SnapshotFileCompression_None)
		assert.Nil(t, err)
	case babuzapb.SnapshotFileType_Cluster:
		wc, err = w.CreateSessionFile(babuzapb.SnapshotFileCompression_None)
		assert.Nil(t, err)
	default:
		assert.Fail(t, "not support file type")
	}

	defer wc.Close()
	d := make([]byte, fd.dataSize)
	rand.Read(d)
	_, err = wc.Write(d)
	assert.Nil(t, err)
}

func genSnapshotFiles(t *testing.T, fs api.SnapshotFileSystem, dir string, version, snapshotTerm, snapshotIndex uint64,
	files []snapFileDesc) babuzapb.SnapshotMetadata {

	assert.Nil(t, fs.CreateDirAndTouch(dir))
	w := NewWriter(fs, dir, &codec.Metadata{}, &mockInstaller{vesion: version}, snapshotIndex)

	for _, fd := range files {
		writeRandomData(t, w, fd)
		if fd.fileType == babuzapb.SnapshotFileType_StateMachine {
			assert.Nil(t, w.AddStateMachineFileMetadata(fd.tag, fd.metadata))
		}
	}
	m, err := w.Commit(raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: snapshotIndex,
			Term:  snapshotTerm,
		},
	})
	assert.Nil(t, err)
	return m
}
