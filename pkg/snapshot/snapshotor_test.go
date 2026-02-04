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


package snapshot

import (
	"context"
	"fmt"
	"sync"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	sanpshotio "github.com/fanaujie/babuza/pkg/snapshot/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/rand"
	"testing"
	"time"
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

const (
	rustfsImage    = "rustfs/rustfs:latest"
	rustfsUsername = "rustfsadmin"
	rustfsPassword = "rustfsadmin"
)

func setupS3Container(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        rustfsImage,
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": rustfsUsername,
			"RUSTFS_SECRET_KEY": rustfsPassword,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err)

	endpoint := "http://" + host + ":" + mappedPort.Port()
	return container, endpoint
}

func setupS3FS(t *testing.T, prefix string) (api.SnapshotFileSystem, func()) {
	// Setup S3-compatible container (RustFS)
	container, endpoint := setupS3Container(t)

	// Create cleanup function
	cleanup := func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}

	// Create S3 FS
	s3Config := cloudstorage.S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     rustfsUsername,
		SecretAccessKey: rustfsPassword,
		UsePathStyle:    true,
		Bucket:          "test-bucket",
		Prefix:          prefix,
	}

	s3FS, err := cloudstorage.NewS3SnapshotFS(s3Config)
	require.NoError(t, err)

	return s3FS, cleanup
}

func TestSnapshotor_CreateFileWriterAndReader(t *testing.T) {
	p := t.TempDir()

	// Setup S3 FS (RustFS)
	s3FS, cleanup := setupS3FS(t, "test-writerreader")
	defer cleanup()

	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {
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
		}, fs, &logger.Mock{}, nil)
		genSnapFiles(t, s, snapTerm, snapIndex, fds)
		reader, err := s.CreateInstalledSnapshotReader(snapIndex, true)
		assert.Nil(t, err)
		assert.NotNil(t, reader)
	}
}

func TestSnapshotor_ValidateFileReceiverAndInstall(t *testing.T) {
	p := t.TempDir()

	// Setup S3 FS (RustFS)
	s3FS, cleanup := setupS3FS(t, "test-receiver")
	defer cleanup()

	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {
		snapVer := uint64(1)
		snapMaxFiles := uint(3)

		s := New(Config{
			SnapshotVersion: snapVer,
			MaxSnapFiles:    snapMaxFiles,
			SnapshotDir:     p,
		}, fs, &logger.Mock{}, nil)
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

	// Setup S3 FS (RustFS)
	s3FS, cleanup := setupS3FS(t, "test-receiver-fail")
	defer cleanup()

	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {

		snapVer := uint64(1)
		snapMaxFiles := uint(3)
		s := New(Config{
			SnapshotVersion: snapVer,
			MaxSnapFiles:    snapMaxFiles,
			SnapshotDir:     p,
		}, fs, &logger.Mock{}, nil)
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

			// Create a unique prefix for this specific test case
			prefix := fmt.Sprintf("load-snapshot-%d", tc.querySnaps[0].Index)
			s3FS, cleanup := setupS3FS(t, prefix)
			defer cleanup()

			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {

				snapMaxFiles := uint(3)
				snapVer := uint64(1)

				s := New(Config{
					SnapshotVersion: snapVer,
					MaxSnapFiles:    snapMaxFiles,
					SnapshotDir:     p,
				}, fs, &logger.Mock{}, nil)
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

			// Create a unique S3 FS for this test case
			prefix := fmt.Sprintf("test-purge-%d", tc.purgeIndex)
			s3FS, cleanup := setupS3FS(t, prefix)
			defer cleanup()

			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {
				s := New(Config{
					SnapshotVersion: snapVer,
					MaxSnapFiles:    snapMaxFiles,
					SnapshotDir:     p,
				}, fs, &logger.Mock{}, nil)

				// Start the async purger
				sp := s.Purger()
				sp.Start()
				defer s.Close()

				for _, snapIndex := range tc.snapIndex {
					wDir, err := fs.CreateDirAndTouch(p, babuzapb.SnapshotFolderType_TempWrite, snapIndex)
					assert.Nil(t, err)
					w := sanpshotio.NewWriter(fs, wDir, &codec.Metadata{}, s, snapIndex)
					tmpSnapshotMetadata := snapshotMetadata
					tmpSnapshotMetadata.Snapshot.Metadata.Index = snapIndex
					_, err = w.Commit(tmpSnapshotMetadata.Snapshot)
					assert.Nil(t, err)
				}

				// Send purge request asynchronously
				assert.Nil(t, s.Purge(raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{
						Index: tc.purgeIndex,
					},
				}))

				// Wait a bit for async purging to complete
				// In a real test environment, you might want to use channels or other synchronization
				// For now, we'll use a simple sleep to ensure the async operation completes
				// Consider adding a synchronization mechanism in production code
				time.Sleep(100 * time.Millisecond)

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

			// Create a unique S3 FS for this folder type
			prefix := fmt.Sprintf("test-commit-%d", dt)
			s3FS, cleanup := setupS3FS(t, prefix)
			defer cleanup()

			for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {
				_, err := fs.CreateDirAndTouch(p, dt, 1)
				assert.Nil(t, err)
				s := New(Config{
					SnapshotVersion: 1,
					SnapshotDir:     p,
				}, fs, &logger.Mock{}, nil)
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
	// Setup S3 FS (RustFS)
	s3FS, cleanup := setupS3FS(t, "test-scan-installed")
	defer cleanup()

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
	for _, fs := range []api.SnapshotFileSystem{volatile.NewFileSystem(), durable.NewSnapshotFS(), s3FS} {
		p := t.TempDir()
		s := New(Config{
			SnapshotVersion: 1,
			SnapshotDir:     p,
		}, fs, &logger.Mock{}, nil)
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

// --- External file notification tests ---

// recordingExternalFileHandler records OnSnapshotReceived calls for verification.
type recordingExternalFileHandler struct {
	mu    sync.Mutex
	calls []struct {
		snapshotIndex uint64
		files         []ibabuza.ExternalFileDescriptor
	}
	err error
}

func (h *recordingExternalFileHandler) OnSnapshotReceived(snapshotIndex uint64, files []ibabuza.ExternalFileDescriptor) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, struct {
		snapshotIndex uint64
		files         []ibabuza.ExternalFileDescriptor
	}{snapshotIndex, files})
	return h.err
}

func TestSnapshotor_CommitNotifiesExternalFileHandler(t *testing.T) {
	p := t.TempDir()
	fs := durable.NewSnapshotFS()

	handler := &recordingExternalFileHandler{}

	s := New(Config{
		SnapshotVersion: 1,
		SnapshotDir:     p,
	}, fs, &logger.Mock{}, nil)
	s.SetExternalFileHandler(handler)

	// Create a snapshot that has both a regular file and an external file.
	w, err := s.CreateAtomicSnapshotWriter(1, 1)
	require.NoError(t, err)

	writeRandomData(t, w, babuzapb.SnapshotFileType_StateMachine, "regular", babuzapb.SnapshotFileCompression_None, 512)
	writeRandomData(t, w, babuzapb.SnapshotFileType_Cluster, "", babuzapb.SnapshotFileCompression_None, 512)
	writeRandomData(t, w, babuzapb.SnapshotFileType_Session, "", babuzapb.SnapshotFileCompression_None, 512)

	require.NoError(t, w.AddExternalFile(ibabuza.ExternalFileDescriptor{
		FileTag: "ext_present", LocationUri: "s3://bucket/ext_present",
	}))
	require.NoError(t, w.AddExternalFile(ibabuza.ExternalFileDescriptor{
		FileTag: "ext_missing", LocationUri: "s3://bucket/ext_missing",
	}))

	_, err = w.Commit(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1}})
	require.NoError(t, err)

	// OnSnapshotReceived should have been called with the external files.
	handler.mu.Lock()
	defer handler.mu.Unlock()
	require.Len(t, handler.calls, 1)
	assert.Equal(t, uint64(1), handler.calls[0].snapshotIndex)
	assert.Len(t, handler.calls[0].files, 2)
}

func TestSnapshotor_CommitNoNotificationWithoutExternalFiles(t *testing.T) {
	p := t.TempDir()
	fs := durable.NewSnapshotFS()

	handler := &recordingExternalFileHandler{}

	s := New(Config{
		SnapshotVersion: 1,
		SnapshotDir:     p,
	}, fs, &logger.Mock{}, nil)
	s.SetExternalFileHandler(handler)

	// Create snapshot with only regular files.
	genSnapFiles(t, s, 1, 1, []snapFileDesc{
		{fileType: babuzapb.SnapshotFileType_StateMachine, tag: "one",
			compressionType: babuzapb.SnapshotFileCompression_None, dataSize: 512},
		{fileType: babuzapb.SnapshotFileType_Cluster,
			compressionType: babuzapb.SnapshotFileCompression_None, dataSize: 512},
		{fileType: babuzapb.SnapshotFileType_Session,
			compressionType: babuzapb.SnapshotFileCompression_None, dataSize: 512},
	})

	// No notification should have been sent.
	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Empty(t, handler.calls, "OnSnapshotReceived should not be called when no external files")
}

