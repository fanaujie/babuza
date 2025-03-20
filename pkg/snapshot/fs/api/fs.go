package api

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
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
	PathHelper() PathHelper
}

type PathHelper interface {
	SnapshotFileName(fileType babuzapb.SnapshotFileType, snapshotIndex uint64, tag string) (string, error)
	SnapshotFolderName(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) (string, error)
	GenerateSnapshotFilePath(snapshotDir string, fileType babuzapb.SnapshotFileType, snapIndex uint64, tag string) (string, error)
	GenerateSnapshotFolderPath(srcDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error)
	ParseMetadataFileName(name string) (uint64, error)
	ParseWriterTmpFolderName(name string) (uint64, error)
	ParseReceiverTmpFolderName(name string) (uint64, error)
	ParseSnapshotFolderName(name string) (uint64, error)
	ParseBrokenFolderName(name string) (uint64, error)
	SnapshotFolderPrefix() string
	TempWriterFolderPrefix() string
	TempReceiverFolderPrefix() string
}
