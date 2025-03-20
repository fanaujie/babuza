package durable

import (
	"fmt"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"os"
	"path/filepath"
)

type FileSystem struct {
	ph api.PathHelper
}

func NewFileSystem() api.FileSystem {
	return &FileSystem{
		ph: api.NewPathHelper("temp-writer", "temp-receiver", "snapshot"),
	}
}

func (fs *FileSystem) FileRead(path string) (io.ReadCloser, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("path %s is a directory, not a file", path)
	}
	return os.Open(path)
}

func (fs *FileSystem) FileWrite(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
}

func (fs *FileSystem) CrcFileRead(path string) (api.CrcFileReader, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateReader(r), nil
}

func (fs *FileSystem) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	w, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateWriter(w), nil
}

func (fs *FileSystem) CreateDirAndTouch(path string) error {
	return fileutil.CreateDirAndTouch(path)
}

func (fs *FileSystem) FileAppendData(path string, data []byte, sync bool) error {
	w, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err = w.Write(data); err != nil {
		return err
	}
	if sync {
		if err = fileutil.Sync(w); err != nil {
			return err
		}
	}
	return nil
}

func (fs *FileSystem) FindMetadataFile(dirPath string) ([]uint64, error) {
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

func (fs *FileSystem) ScanInstalledSnapshot(dirPath string) ([]uint64, error) {
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

func (fs *FileSystem) ScanTempSnapshotFolder(dirPath string) ([]string, []string, error) {
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

func (fs *FileSystem) ExistFilePath(path string) bool {
	return fileutil.Exist(path)
}

func (fs *FileSystem) ExistDir(path string) bool {
	return fileutil.Exist(path)
}
func (fs *FileSystem) FileSize(path string) (int64, error) {
	return fileutil.FileSize(path)
}

func (fs *FileSystem) SyncDir(path string) error {
	return fileutil.SyncDir(path)
}

func (fs *FileSystem) SyncFile(path string) error {
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

func (fs *FileSystem) RenameDir(sourcePath string, destPath string) error {
	return os.Rename(sourcePath, destPath)
}

func (fs *FileSystem) RemoveDir(path string) error {
	return os.RemoveAll(path)
}

func (fs *FileSystem) RemoveFilePath(path string) error {
	return os.Remove(path)
}

func (fs *FileSystem) PathHelper() api.PathHelper {
	return fs.ph
}
