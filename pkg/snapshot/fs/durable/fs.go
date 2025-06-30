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


package durable

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"os"
	"path/filepath"
)

type SnapshotFS struct {
	ph api.PathHelper
}

var _ api.SnapshotFileSystem = (*SnapshotFS)(nil)

func NewSnapshotFS() *SnapshotFS {
	return &SnapshotFS{
		ph: api.NewPathHelper("temp-writer", "temp-receiver", "snapshot"),
	}
}

func (fs *SnapshotFS) FileRead(path string) (io.ReadCloser, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("path %s is a directory, not a file", path)
	}
	return os.Open(path)
}

func (fs *SnapshotFS) FileWrite(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
}

func (fs *SnapshotFS) CrcFileRead(path string) (api.CrcFileReader, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateReader(r), nil
}

func (fs *SnapshotFS) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	w, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateWriter(w), nil
}

func (fs *SnapshotFS) CreateDirAndTouch(snapshotDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	dir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	if err != nil {
		return "", err
	}
	if err = fileutil.CreateDirAndTouch(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func (fs *SnapshotFS) FileAppendData(path string, chunkId int64, data []byte) error {
	w, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

func (fs *SnapshotFS) FileAppendFinalize(path string, totalChunks int64) error {
	w, err := os.OpenFile(path, os.O_WRONLY, fileutil.FileMode)
	if err != nil {
		return err
	}
	defer w.Close()
	return fileutil.Sync(w)
}

func (fs *SnapshotFS) FindMetadataFile(dirPath string) ([]uint64, error) {
	dirs, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, d := range dirs {
		name := d.Name()
		if d.IsDir() {
			continue
		}
		index, err := fs.ph.ParseMetadataFileName(name)
		if err == nil {
			ids = append(ids, index)
		}
	}
	return ids, nil
}

func (fs *SnapshotFS) ScanInstalledSnapshot(dirPath string) ([]uint64, error) {
	var installed []uint64

	dirs, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	var index uint64
	for _, d := range dirs {
		name := d.Name()
		if !d.IsDir() {
			continue
		}
		index, err = fs.ph.ParseSnapshotFolderName(name)
		if err == nil {
			installed = append(installed, index)
			continue
		}
	}
	return installed, nil
}

func (fs *SnapshotFS) ScanTempSnapshotFolder(dirPath string) ([]string, []string, error) {
	dirs, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}
	var _ uint64
	var tempWrite, tempReceiver []string
	for _, d := range dirs {
		name := d.Name()
		if !d.IsDir() {
			continue
		}
		_, err = fs.ph.ParseWriterTmpFolderName(name)
		if err == nil {
			tempWrite = append(tempWrite, filepath.Join(dirPath, name))
			continue
		}
		_, err = fs.ph.ParseReceiverTmpFolderName(name)
		if err == nil {
			tempReceiver = append(tempReceiver, filepath.Join(dirPath, name))
		}
	}
	return tempWrite, tempReceiver, nil
}

func (fs *SnapshotFS) InstallSnapshotFromTempFolder(snapshotDirPath string, folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	installDir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDirPath, babuzapb.SnapshotFolderType_InstallSnapshot, snapshotIndex)
	if err != nil {
		return err
	}
	if fs.ExistDir(installDir) {
		return fmt.Errorf("snapshot: the installation directory already exists. path(%s) snapshot idnex(%d)",
			installDir, snapshotIndex)
	}
	sourceDir, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDirPath, folderType, snapshotIndex)
	if err != nil {
		return err
	}
	return fs.RenameDir(sourceDir, installDir)
}

func (fs *SnapshotFS) ExistFilePath(path string) bool {
	return fileutil.Exist(path)
}

func (fs *SnapshotFS) ExistDir(path string) bool {
	return fileutil.Exist(path)
}
func (fs *SnapshotFS) FileSize(path string) (int64, error) {
	return fileutil.FileSize(path)
}

func (fs *SnapshotFS) SyncDir(path string) error {
	return fileutil.SyncDir(path)
}

func (fs *SnapshotFS) SyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = fileutil.Sync(f); err != nil {
		return err
	}
	return nil
}

func (fs *SnapshotFS) RenameDir(sourcePath string, destPath string) error {
	return os.Rename(sourcePath, destPath)
}

func (fs *SnapshotFS) RemoveDir(path string) error {
	return os.RemoveAll(path)
}

func (fs *SnapshotFS) RemoveFilePath(path string) error {
	return os.Remove(path)
}

func (fs *SnapshotFS) PathHelper() api.PathHelper {
	return fs.ph
}

func (fs *SnapshotFS) Close() error {
	return nil
}
