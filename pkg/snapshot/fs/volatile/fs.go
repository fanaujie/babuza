package volatile

import (
	"bytes"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type bytesFile struct {
	*bytes.Buffer
}

func newWriteBytesFile() *bytesFile {
	return &bytesFile{
		Buffer: bytes.NewBuffer(make([]byte, 0, 2048)),
	}
}

func newReadBytesFile(w *bytesFile) *bytesFile {
	return &bytesFile{
		Buffer: bytes.NewBuffer(w.Bytes()),
	}
}

func (b *bytesFile) Close() error {
	return nil
}

type SnapshotFS struct {
	files map[string]*bytesFile
	dirs  map[string]struct{}
	ph    api.PathHelper
	mu    *sync.RWMutex
}

func NewFileSystem() api.SnapshotFileSystem {
	return &SnapshotFS{
		files: make(map[string]*bytesFile),
		dirs:  make(map[string]struct{}),
		ph:    api.NewPathHelper("temp-writer", "temp-receiver", "snapshot"),
		mu:    &sync.RWMutex{},
	}
}

func (fs *SnapshotFS) isDirExist(path string) bool {
	_, exists := fs.dirs[path]
	return exists
}

func (fs *SnapshotFS) CreateDirAndTouch(snapshotDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	dir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	if err != nil {
		return "", err
	}
	_, ok := fs.dirs[dir]
	if ok {
		return "", os.ErrExist
	}
	fs.dirs[dir] = struct{}{}
	return dir, nil
}

func (fs *SnapshotFS) FileRead(path string) (io.ReadCloser, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	dir := filepath.Dir(path)
	if !fs.isDirExist(dir) {
		return nil, os.ErrNotExist
	}

	file, exists := fs.files[path]
	if !exists {
		return nil, os.ErrNotExist
	}
	return newReadBytesFile(file), nil
}

func (fs *SnapshotFS) FileWrite(path string) (io.WriteCloser, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir := filepath.Dir(path)
	if !fs.isDirExist(dir) {
		return nil, os.ErrNotExist
	}

	file := newWriteBytesFile()
	fs.files[path] = file
	return file, nil
}

func (fs *SnapshotFS) CrcFileRead(path string) (api.CrcFileReader, error) {
	reader, err := fs.FileRead(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateReader(reader), nil
}

func (fs *SnapshotFS) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	writer, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateWriter(writer), nil
}

func (fs *SnapshotFS) FileAppendData(path string, chunkId int64, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	dir := filepath.Dir(path)
	if !fs.isDirExist(dir) {
		return os.ErrNotExist
	}

	file, exists := fs.files[path]
	if !exists {
		file = newWriteBytesFile()
		fs.files[path] = file
	}

	_, err := file.Write(data)
	return err
}

func (fs *SnapshotFS) FileAppendFinalize(path string, totalChunks int64) error {
	// not implemented
	return nil
}

func (fs *SnapshotFS) FindMetadataFile(dirPath string) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var ids []uint64
	for fp := range fs.files {
		if strings.HasPrefix(fp, dirPath) {
			index, err := fs.ph.ParseMetadataFileName(filepath.Base(fp))
			if err == nil {
				ids = append(ids, index)
			}
		}
	}
	return ids, nil
}

func (fs *SnapshotFS) ScanInstalledSnapshot(dirPath string) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var installed []uint64
	for dir := range fs.dirs {
		if strings.HasPrefix(dir, filepath.Join(dirPath, fs.ph.SnapshotFolderPrefix())) {
			dirName := filepath.Base(dir)
			index, err := fs.ph.ParseSnapshotFolderName(dirName)
			if err == nil {
				installed = append(installed, index)
			}
		}
	}
	return installed, nil
}

func (fs *SnapshotFS) ScanTempSnapshotFolder(dirPath string) ([]string, []string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var tmpWriter, tmpReceiver []string

	for dir := range fs.dirs {
		if strings.HasPrefix(dir, filepath.Join(dirPath, fs.ph.TempWriterFolderPrefix())) {
			dirName := filepath.Base(dir)
			_, err := fs.ph.ParseWriterTmpFolderName(dirName)
			if err == nil {
				tmpWriter = append(tmpWriter, dir)
			}
		} else if strings.HasPrefix(dir, filepath.Join(dirPath, fs.ph.TempReceiverFolderPrefix())) {
			dirName := filepath.Base(dir)
			_, err := fs.ph.ParseReceiverTmpFolderName(dirName)
			if err == nil {
				tmpReceiver = append(tmpReceiver, dir)
			}
		}
	}
	return tmpWriter, tmpReceiver, nil
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
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	_, exists := fs.files[path]
	return exists
}

func (fs *SnapshotFS) ExistDir(path string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.isDirExist(path)
}

func (fs *SnapshotFS) FileSize(path string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	file, exists := fs.files[path]
	if !exists {
		return 0, os.ErrNotExist
	}

	return int64(file.Len()), nil
}

func (fs *SnapshotFS) SyncDir(path string) error {
	return nil
}

func (fs *SnapshotFS) SyncFile(path string) error {
	return nil
}

func (fs *SnapshotFS) RenameDir(oldPath string, newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if !fs.isDirExist(oldPath) {
		return os.ErrNotExist
	}

	for filePath, file := range fs.files {
		if strings.HasPrefix(filePath, oldPath+string(filepath.Separator)) {
			newFilePath := filepath.Join(newPath, strings.TrimPrefix(filePath, oldPath))
			delete(fs.files, filePath)
			fs.files[newFilePath] = file
		}
	}

	delete(fs.dirs, oldPath)
	fs.dirs[newPath] = struct{}{}

	return nil
}

func (fs *SnapshotFS) RemoveDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if !fs.isDirExist(path) {
		return os.ErrNotExist
	}

	for filePath := range fs.files {
		if strings.HasPrefix(filePath, path+string(filepath.Separator)) {
			delete(fs.files, filePath)
		}
	}

	delete(fs.dirs, path)
	return nil
}

func (fs *SnapshotFS) RemoveFilePath(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, exists := fs.files[path]; !exists {
		return os.ErrNotExist
	}
	delete(fs.files, path)
	return nil
}

func (fs *SnapshotFS) PathHelper() api.PathHelper {
	return fs.ph
}
