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


package fs

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestCreateDirAndTouch(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		p, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		assert.True(t, fs.ExistDir(p))
	}
}

func TestOpenForWrite(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		// First case - create directory and file
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 1, "")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		assert.True(t, fs.ExistFilePath(fp))

		// Second case - attempting write without directory
		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 2, "")
		assert.Nil(t, err)
		_, err = fs.FileWrite(fp)
		assert.Error(t, err)
		assert.False(t, fs.ExistFilePath(fp))
	}
}

func TestOpenForRead(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 1, "")
		assert.Nil(t, err)

		func() {
			w, err := fs.FileWrite(fp)
			assert.Nil(t, err)
			defer w.Close()
			data := []byte{1, 2, 3, 4}
			_, err = w.Write(data)
			assert.Nil(t, err)

			r, err := fs.FileRead(fp)
			assert.Nil(t, err)
			defer r.Close()
			readData := make([]byte, 4)
			_, err = r.Read(readData)
			assert.Nil(t, err)
			assert.Equal(t, data, readData)
		}()

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 2, "")
		assert.Nil(t, err)
		_, err = fs.FileRead(fp)
		assert.Error(t, err)
	}
}

func TestOpenForCrcWrite(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 1, "")
		assert.Nil(t, err)
		w, err := fs.CrcFileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		assert.True(t, fs.ExistFilePath(fp))

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 2, "")
		assert.Nil(t, err)
		_, err = fs.CrcFileWrite(fp)
		assert.Error(t, err)
		assert.False(t, fs.ExistFilePath(fp))
	}
}

func TestOpenForCrcRead(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 1, "")
		assert.Nil(t, err)

		func() {
			w, err := fs.CrcFileWrite(fp)
			assert.Nil(t, err)
			defer w.Close()
			data := []byte{1, 2, 3, 4}
			_, err = w.Write(data)
			assert.Nil(t, err)

			r, err := fs.CrcFileRead(fp)
			assert.Nil(t, err)
			defer r.Close()
			readData := make([]byte, 4)
			_, err = r.Read(readData)
			assert.Nil(t, err)
			assert.Equal(t, data, readData)
		}()

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 2, "")
		assert.Nil(t, err)
		_, err = fs.CrcFileRead(fp)
		assert.Error(t, err)
	}
}

func TestAppendData(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempReceive, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 1, "one")
		assert.Nil(t, err)

		func() {
			data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
			assert.Nil(t, fs.FileAppendData(fp, 1, data[:4]))
			assert.Nil(t, fs.FileAppendData(fp, 1, data[4:]))

			r, err := fs.FileRead(fp)
			assert.Nil(t, err)
			defer r.Close()
			readData := make([]byte, 8)
			_, err = r.Read(readData)
			assert.Nil(t, err)
			assert.Equal(t, data, readData)
		}()
	}
}

func TestFindMetadataFile(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 100)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 100, "")
		assert.Nil(t, err)
		w1, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w1.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 2, "")
		assert.Nil(t, err)
		w2, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w2.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 3, "")
		assert.Nil(t, err)
		w3, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w3.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 100, "1")
		assert.Nil(t, err)
		w4, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w4.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 100, "2")
		assert.Nil(t, err)
		w5, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w5.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Cluster, 100, "")
		assert.Nil(t, err)
		w6, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w6.Close())

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Session, 100, "")
		assert.Nil(t, err)
		w7, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w7.Close())

		indexes, err := fs.FindMetadataFile(dir)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(indexes))
		sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
		assert.Equal(t, []uint64{2, 3, 100}, indexes)
	}
}

func TestScanInstalledSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		//durable.NewFileSystem(),
	} {
		installed := []uint64{1, 2, 3, 4, 5}
		for _, snapIndex := range installed {
			_, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex)
			assert.Nil(t, err)
		}

		_, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		_, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempReceive, 2)
		assert.Nil(t, err)

		indexes, err := fs.ScanInstalledSnapshot(tmpDir)
		assert.Nil(t, err)
		assert.Equal(t, len(installed), len(indexes))
		sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
		assert.Equal(t, installed, indexes)
	}
}

func TestScanTempSnapshotFolder(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		installed := []uint64{1, 2, 3, 4, 5}
		for _, snapIndex := range installed {
			_, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex)
			assert.Nil(t, err)
		}

		_, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		_, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempReceive, 2)
		assert.Nil(t, err)

		_, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempReceive, 3)
		assert.Nil(t, err)

		tempWriter, tempReceiver, err := fs.ScanTempSnapshotFolder(tmpDir)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(tempWriter))
		assert.Equal(t, 2, len(tempReceiver))
	}
}

func TestExistFilePath(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 1, "tag")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		assert.True(t, fs.ExistFilePath(fp))

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 1, "not found")
		assert.Nil(t, err)
		assert.False(t, fs.ExistFilePath(fp))

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 2, "not found")
		assert.Nil(t, err)
		assert.False(t, fs.ExistFilePath(fp))
	}
}

func TestExistDir(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		assert.True(t, fs.ExistDir(dir))

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)
		assert.False(t, fs.ExistDir(dir))

		dir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_TempReceive, 2)
		assert.Nil(t, err)
		assert.False(t, fs.ExistDir(dir))
	}
}

func TestFileSize(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, 1, "")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		data := []byte{1, 2, 3, 4}
		_, err = w.Write(data)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())

		fileSize, err := fs.FileSize(fp)
		assert.Nil(t, err)
		assert.Equal(t, int64(4), fileSize)
	}
}

func TestRenameDir(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		// First case - simple directory rename
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		installDir, err := fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_InstallSnapshot, 1)
		assert.Nil(t, err)
		assert.Nil(t, fs.RenameDir(dir, installDir))
		assert.True(t, fs.ExistDir(installDir))
		assert.False(t, fs.ExistDir(dir))

		// Second case - directory with file rename
		dir, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 2, "one")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())

		installDir, err = fs.PathHelper().GenerateSnapshotFolderPath(tmpDir, babuzapb.SnapshotFolderType_InstallSnapshot, 2)
		assert.Nil(t, err)
		assert.Nil(t, fs.RenameDir(dir, installDir))
		assert.True(t, fs.ExistDir(installDir))
		assert.False(t, fs.ExistDir(dir))

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(installDir, babuzapb.SnapshotFileType_StateMachine, 2, "one")
		assert.Nil(t, err)
		assert.True(t, fs.ExistFilePath(fp))

		originalFp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 2, "one")
		assert.Nil(t, err)
		assert.False(t, fs.ExistFilePath(originalFp))
	}
}

func TestRemoveDir(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		assert.Nil(t, fs.RemoveDir(dir))
		assert.False(t, fs.ExistDir(dir))

		dir, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)
		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Cluster, 1, "")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		assert.True(t, fs.ExistFilePath(fp))

		assert.Nil(t, fs.RemoveDir(dir))
		assert.False(t, fs.ExistFilePath(fp))
	}
}

func TestRemoveFilePath(t *testing.T) {
	tmpDir := t.TempDir()
	for _, fs := range []api.SnapshotFileSystem{
		volatile.NewFileSystem(),
		durable.NewSnapshotFS(),
	} {
		dir, err := fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 1)
		assert.Nil(t, err)

		fp, err := fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Cluster, 1, "")
		assert.Nil(t, err)
		w, err := fs.FileWrite(fp)
		assert.Nil(t, err)
		assert.Nil(t, w.Close())
		assert.True(t, fs.ExistFilePath(fp))
		assert.Nil(t, fs.RemoveFilePath(fp))
		assert.False(t, fs.ExistFilePath(fp))

		dir, err = fs.CreateDirAndTouch(tmpDir, babuzapb.SnapshotFolderType_TempWrite, 2)
		assert.Nil(t, err)

		fp, err = fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_StateMachine, 2, "not found")
		assert.Nil(t, err)
		assert.Error(t, fs.RemoveFilePath(fp))
	}
}
