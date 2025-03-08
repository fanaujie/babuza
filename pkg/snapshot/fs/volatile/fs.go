package volatile

import (
	"bytes"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcFile"
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

type FileSystem struct {
	files map[string]*bytesFile
	dirs  map[string]struct{}
	mu    *sync.RWMutex
}

func NewFileSystem() api.FileSystem {
	return &FileSystem{
		files: make(map[string]*bytesFile),
		dirs:  make(map[string]struct{}),
		mu:    &sync.RWMutex{},
	}
}

func (fs *FileSystem) isDirExist(path string) bool {
	_, exists := fs.dirs[path]
	return exists
}

func (fs *FileSystem) CreateDirAndTouch(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, ok := fs.dirs[path]
	if ok {
		return os.ErrExist
	}
	fs.dirs[path] = struct{}{}
	return nil
}

func (fs *FileSystem) FileRead(path string) (io.ReadCloser, error) {
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

func (fs *FileSystem) FileWrite(path string) (io.WriteCloser, error) {
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

func (fs *FileSystem) CrcFileRead(path string) (api.CrcFileReader, error) {
	reader, err := fs.FileRead(path)
	if err != nil {
		return nil, err
	}
	return crcFile.CreateReader(reader), nil
}

func (fs *FileSystem) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	writer, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}
	return crcFile.CreateWriter(writer), nil
}

func (fs *FileSystem) FileAppendData(path string, data []byte, sync bool) error {
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

func (fs *FileSystem) FindMetadataFile(dirPath string) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var ids []uint64
	for fp := range fs.files {
		if strings.HasPrefix(fp, dirPath) {
			index, err := api.ParseMetadataFileName(filepath.Base(fp))
			if err == nil {
				ids = append(ids, index)
			}
		}
	}
	return ids, nil
}

func (fs *FileSystem) ScanInstalledSnapshot(dirPath string) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	var installed []uint64
	for dir := range fs.dirs {
		if strings.HasPrefix(dir, filepath.Join(dirPath, api.SnapshotFolderNamePrefix)) {
			dirName := filepath.Base(dir)
			index, err := api.ParseSnapshotFolderName(dirName)
			if err == nil {
				installed = append(installed, index)
			}
		}
	}
	return installed, nil
}

func (fs *FileSystem) ScanTempSnapshotFolder(dirPath string) ([]string, []string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var tmpWriter, tmpReceiver []string

	for dir := range fs.dirs {
		if strings.HasPrefix(dir, filepath.Join(dirPath, api.TempWriterFolderNamePrefix)) {
			dirName := filepath.Base(dir)
			_, err := api.ParseWriterTmpFolderName(dirName)
			if err == nil {
				tmpWriter = append(tmpWriter, dir)
			}
		} else if strings.HasPrefix(dir, filepath.Join(dirPath, api.TempReceiverFolderNamePrefix)) {
			dirName := filepath.Base(dir)
			_, err := api.ParseReceiverTmpFolderName(dirName)
			if err == nil {
				tmpReceiver = append(tmpReceiver, dir)
			}
		}
	}
	return tmpWriter, tmpReceiver, nil
}

func (fs *FileSystem) ExistFilePath(path string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	_, exists := fs.files[path]
	return exists
}

func (fs *FileSystem) ExistDir(path string) bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.isDirExist(path)
}

func (fs *FileSystem) FileSize(path string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	file, exists := fs.files[path]
	if !exists {
		return 0, os.ErrNotExist
	}

	return int64(file.Len()), nil
}

func (fs *FileSystem) SyncDir(path string) error {
	return nil
}

func (fs *FileSystem) SyncFile(path string) error {
	return nil
}

func (fs *FileSystem) RenameDir(oldPath string, newPath string) error {
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

func (fs *FileSystem) RemoveDir(path string) error {
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

func (fs *FileSystem) RemoveFilePath(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, exists := fs.files[path]; !exists {
		return os.ErrNotExist
	}
	delete(fs.files, path)
	return nil
}
