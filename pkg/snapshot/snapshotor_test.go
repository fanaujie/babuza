package snapshot

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	sanpshotio "github.com/fanaujie/babuza/pkg/snapshot/io"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/rand"
	"testing"
)

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

func writeRandomData(t *testing.T, w ibabuza.AtomicSnapshotWriter, fileType babuzapb.SnapshotFileType, tag string,
	compression babuzapb.SnapshotFileCompressionType, dataSize int) {
	var wc io.WriteCloser
	var err error
	switch fileType {
	case babuzapb.SnapshotFileType_StateMachine:
		wc, err = w.CreateStateMachineFile(tag, compression)
		assert.Nil(t, err)
	case babuzapb.SnapshotFileType_Session:
		wc, err = w.CreateSessionFile(compression)
		assert.Nil(t, err)
	case babuzapb.SnapshotFileType_Cluster:
		wc, err = w.CreateClusterFile(compression)
		assert.Nil(t, err)
	default:
		assert.Fail(t, "not support file type")
	}

	defer wc.Close()
	d := make([]byte, dataSize)
	rand.Read(d)
	_, err = wc.Write(d)
	assert.Nil(t, err)
}

func genSnapFiles(t *testing.T, snap *Snapshotor, snapshotTerm, snapshotIndex uint64,
	snapFiles []snapFileDesc) {
	w, err := snap.CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex)
	assert.Nil(t, err)
	for _, file := range snapFiles {
		writeRandomData(t, w, file.fileType, file.tag, file.compressionType, file.dataSize)
		if file.fileType == babuzapb.SnapshotFileType_StateMachine {
			assert.Nil(t, w.AddStateMachineFileMetadata(file.tag, file.metadata))
		}
	}
	_, err = w.Commit(raftpb.Snapshot{
		Data: nil,
		Metadata: raftpb.SnapshotMetadata{
			Index: snapshotIndex,
			Term:  snapshotTerm,
		},
	})
	assert.Nil(t, err)
}

func TestSnapshotor_CreateFileWriterAndReader(t *testing.T) {
	p := t.TempDir()

	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {
		fds := []snapFileDesc{
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
		snapTerm := uint64(1)
		snapIndex := uint64(1)
		snapMaxFiles := uint(3)
		snapVer := uint64(1)
		s := New(Config{
			SnapshotVersion: snapVer,
			MaxSnapFiles:    snapMaxFiles,
			SnapshotDir:     p,
		}, fs, &logger.Mock{})
		genSnapFiles(t, s, snapTerm, snapIndex, fds)
		reader, err := s.CreateInstalledSnapshotReader(snapIndex, true)
		assert.Nil(t, err)
		assert.NotNil(t, reader)
	}
}

func TestSnapshotor_ValidateFileReceiverAndInstall(t *testing.T) {
	p := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {
		snapVer := uint64(1)
		snapMaxFiles := uint(3)

		s := New(Config{
			SnapshotVersion: snapVer,
			MaxSnapFiles:    snapMaxFiles,
			SnapshotDir:     p,
		}, fs, &logger.Mock{})
		tmpSnapshotMetadata := snapshotMetadata
		d := make([]byte, tmpSnapshotMetadata.Files["one"].FileSize)
		rand.Read(d)
		fsmDesc := tmpSnapshotMetadata.Files["one"]
		fsmDesc.FileCrc64 = crc64.Checksum(d, crcfile.Crc64Table)
		tmpSnapshotMetadata.Files["one"] = fsmDesc
		receiver, err := s.CreateAtomicSnapshotReceiver(tmpSnapshotMetadata)
		assert.Nil(t, err)
		assert.NotNil(t, receiver)
		dir, err := fs.PathHelper().GenerateSnapshotFolderPath(p, babuzapb.SnapshotFolderType_TempReceive, tmpSnapshotMetadata.Snapshot.Metadata.Index)
		assert.Nil(t, err)
		assert.Equal(t, true, fs.ExistDir(dir))

		assert.Nil(t, receiver.SaveChunk(snapshotMetadata.Snapshot.Metadata.Index, babuzapb.SnapshotChunkMessage{
			FileType:      babuzapb.SnapshotFileType_StateMachine,
			FileTag:       "one",
			Id:            1,
			Data:          d,
			ContinueCrc32: crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), d),
			LastChunk:     true,
		}))

		assert.Nil(t, receiver.Commit(snapshotMetadata.Snapshot.Metadata.Index))
		for _, index := range s.getInstalledSnapshotIndexSlice() {
			if index == 1 {
				return
			}
		}
		assert.Fail(t, "not found install snapshot by snapshot index 1")
	}
}

func TestSnapshotor_ValidateFileReceiverAndInstall_Fail(t *testing.T) {
	p := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {

		snapVer := uint64(1)
		snapMaxFiles := uint(3)
		s := New(Config{
			SnapshotVersion: snapVer,
			MaxSnapFiles:    snapMaxFiles,
			SnapshotDir:     p,
		}, fs, &logger.Mock{})
		receiver, err := s.CreateAtomicSnapshotReceiver(snapshotMetadata)
		assert.Nil(t, err)
		assert.NotNil(t, receiver)
		dir, err := fs.PathHelper().GenerateSnapshotFolderPath(p, babuzapb.SnapshotFolderType_TempReceive, snapshotMetadata.Snapshot.Metadata.Index)
		assert.Nil(t, err)
		assert.Equal(t, true, fs.ExistDir(dir))
		err = receiver.Commit(snapshotMetadata.Snapshot.Metadata.Index + 1)
		assert.Errorf(t, err, fmt.Sprintf("snapshotor: mismatch snapshot index(expected=%d,get=%d)", snapshotMetadata.Snapshot.Metadata.Index,
			snapshotMetadata.Snapshot.Metadata.Index+1))

	}
}

func TestSnapshotor_LoadLastValidSnapshot(t *testing.T) {

	for _, tc := range []struct {
		querySnaps        []walpb.Snapshot
		realSnaps         []raftpb.Snapshot
		expectedSnapshot  *raftpb.Snapshot
		findValidSnapShot bool
	}{
		{
			querySnaps: []walpb.Snapshot{
				{
					Index: 1,
					Term:  1,
				},
			},
			realSnaps: []raftpb.Snapshot{
				{
					Metadata: raftpb.SnapshotMetadata{
						Index: 1,
						Term:  1,
					},
				},
				{
					Metadata: raftpb.SnapshotMetadata{
						Index: 2,
						Term:  1,
					},
				},
			},
			expectedSnapshot: &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 1,
					Term:  1,
				},
			},
			findValidSnapShot: true,
		},
		{
			querySnaps: []walpb.Snapshot{
				{
					Index: 10,
					Term:  1,
				},
			},
			expectedSnapshot:  nil,
			findValidSnapShot: false,
		},
		{
			querySnaps: []walpb.Snapshot{
				{
					Index: 20,
					Term:  1,
				},
				{
					Index: 21,
					Term:  1,
				},
				{
					Index: 22,
					Term:  1,
				},
			},
			realSnaps: []raftpb.Snapshot{
				{
					Metadata: raftpb.SnapshotMetadata{
						Index: 21,
						Term:  1,
					},
				},
				{
					Metadata: raftpb.SnapshotMetadata{
						Index: 22,
						Term:  1,
					},
				},
			},
			expectedSnapshot: &raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 22,
					Term:  1,
				},
			},
			findValidSnapShot: true,
		},
	} {
		func() {
			p := t.TempDir()
			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {

				snapMaxFiles := uint(3)
				snapVer := uint64(1)

				s := New(Config{
					SnapshotVersion: snapVer,
					MaxSnapFiles:    snapMaxFiles,
					SnapshotDir:     p,
				}, fs, &logger.Mock{})
				for _, m := range tc.realSnaps {
					genSnapFiles(t, s, m.Metadata.Term, m.Metadata.Index, []snapFileDesc{
						{
							tag:             "one",
							compressionType: babuzapb.SnapshotFileCompression_None,
							dataSize:        1024,
							metadata:        []byte("hello world one")},
					})
				}
				snap, _ := s.LoadLastValidSnapshot(tc.querySnaps)
				assert.Equal(t, tc.findValidSnapShot, snap != nil)
				assert.Equal(t, tc.expectedSnapshot, snap)
			}
		}()
	}

}

func TestSnapshotor_Purge(t *testing.T) {

	snapVer := uint64(1)
	snapMaxFiles := uint(3)
	for _, tc := range []struct {
		snapIndex   []uint64
		remainIndex []uint64
		purgeIndex  uint64
	}{
		{
			snapIndex:   []uint64{1, 2, 3, 4, 5},
			remainIndex: []uint64{3, 4, 5},
			purgeIndex:  3,
		},
		{
			snapIndex:   []uint64{1, 3, 5, 7, 9, 10},
			remainIndex: []uint64{5, 7, 9, 10},
			purgeIndex:  5,
		},
	} {
		func() {
			p := t.TempDir()
			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {
				s := New(Config{
					SnapshotVersion: snapVer,
					MaxSnapFiles:    snapMaxFiles,
					SnapshotDir:     p,
				}, fs, &logger.Mock{})
				for _, snapIndex := range tc.snapIndex {
					wDir, err := fs.PathHelper().GenerateSnapshotFolderPath(p, babuzapb.SnapshotFolderType_TempWrite, snapIndex)
					assert.Nil(t, err)
					assert.Nil(t, fs.CreateDirAndTouch(wDir))
					w := sanpshotio.NewWriter(fs, wDir, &codec.Metadata{}, s, snapIndex)
					tmpSnapshotMetadata := snapshotMetadata
					tmpSnapshotMetadata.Snapshot.Metadata.Index = snapIndex
					_, err = w.Commit(tmpSnapshotMetadata.Snapshot)
					assert.Nil(t, err)
				}
				assert.Nil(t, s.Purge(raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{
						Index: tc.purgeIndex,
					},
				}))
				for _, snapIndex := range tc.remainIndex {
					dir, err := fs.PathHelper().GenerateSnapshotFolderPath(p, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex)
					assert.Nil(t, err)
					assert.Equal(t, true, fs.ExistDir(dir))
				}
			}
		}()

	}

}

func TestSnapshotor_CommitSnapshot(t *testing.T) {

	for _, dirType := range []babuzapb.SnapshotFolderType{
		babuzapb.SnapshotFolderType_TempWrite, babuzapb.SnapshotFolderType_TempReceive,
	} {
		func(dt babuzapb.SnapshotFolderType) {
			p := t.TempDir()
			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {
				dir, err := fs.PathHelper().GenerateSnapshotFolderPath(p, dt, 1)
				assert.Nil(t, err)
				assert.Nil(t, fs.CreateDirAndTouch(dir))
				s := New(Config{
					SnapshotVersion: 1,
					SnapshotDir:     p,
				}, fs, &logger.Mock{})
				assert.Nil(t, s.commitSnapshot(dt, 1))
				_, ok := s.installedSnapshot[1]
				assert.Equal(t, true, ok)
				dir2, err := fs.PathHelper().GenerateSnapshotFolderPath(p, babuzapb.SnapshotFolderType_InstallSnapshot, 1)
				assert.Nil(t, err)
				assert.Equal(t, true, fs.ExistDir(dir2))
				assert.Equal(t, "snapshot: the installed snapshot already exists. (snapshot index=1)",
					s.commitSnapshot(dt, 1).Error())
			}
		}(dirType)
	}

}

func TestSnapshotor_scanInstalledSnapshot(t *testing.T) {

	fds := []snapFileDesc{
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
	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS()} {
		p := t.TempDir()
		s := New(Config{
			SnapshotVersion: 1,
			SnapshotDir:     p,
		}, fs, &logger.Mock{})
		for index := uint64(1); index <= 8; index++ {
			genSnapFiles(t, s, 1, index, fds)
		}
		assert.Nil(t, s.scanInstalledSnapshot())
		for index := uint64(1); index <= 8; index++ {
			_, ok := s.installedSnapshot[index]
			assert.Equal(t, true, ok)
		}
	}

}
