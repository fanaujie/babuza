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

type BaseFileSystem interface {
	FileRead(path string) (io.ReadCloser, error)
	FileWrite(path string) (io.WriteCloser, error)
	CrcFileRead(path string) (CrcFileReader, error)
	CrcFileWrite(path string) (CrcFileWriter, error)
	CreateDirAndTouch(snapshotDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error)
	FileAppendData(path string, chunkId int64, data []byte) error
	FileAppendFinalize(path string, totalChunks int64) error
	ExistFilePath(path string) bool
	ExistDir(path string) bool
	FileSize(path string) (int64, error)
	SyncDir(path string) error
	SyncFile(path string) error
	RenameDir(oldPath string, newPath string) error
	RemoveDir(path string) error
	RemoveFilePath(path string) error
}

type SnapshotManager interface {
	FindMetadataFile(snapshotDirPath string) ([]uint64, error)
	ScanInstalledSnapshot(snapshotDirPath string) ([]uint64, error)
	ScanTempSnapshotFolder(snapshotDirPath string) (tmpWriter []string, tmpReceiver []string, err error)
	InstallSnapshotFromTempFolder(snapshotDirPath string, folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error
	PathHelper() PathHelper
	Close() error
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

type SnapshotFileSystem interface {
	BaseFileSystem
	SnapshotManager
}
