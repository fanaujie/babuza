package api

import (
	"io"
)

type CrcFileReader interface {
	Read(p []byte) (int, error)
	Crc() uint64
	FileSize() int
	Close() error
}

type CrcFileWriter interface {
	Write(p []byte) (int, error)
	Crc() uint64
	FileSize() int
	Close() error
}

type FileSystem interface {
	FileRead(path string) (io.ReadCloser, error)
	FileWrite(path string) (io.WriteCloser, error)
	CrcFileRead(path string) (CrcFileReader, error)
	CrcFileWrite(path string) (CrcFileWriter, error)
	CreateDirAndTouch(path string) error
	FileAppendData(path string, data []byte, sync bool) error
	FindMetadataFile(dirPath string) ([]uint64, error)
	ScanInstalledSnapshot(dirPath string) ([]uint64, error)
	ScanTempSnapshotFolder(dirPath string) (tmpWriter []string, tmpReceiver []string, err error)
	ExistFilePath(path string) bool
	ExistDir(path string) bool
	FileSize(path string) (int64, error)
	SyncDir(path string) error
	SyncFile(path string) error
	RenameDir(oldPath string, newPath string) error
	RemoveDir(path string) error
	RemoveFilePath(path string) error
}
