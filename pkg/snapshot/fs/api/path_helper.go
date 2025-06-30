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
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"path/filepath"
)

type pathHelperImpl struct {
	tempWriterFolderNamePrefix   string
	tempReceiverFolderNamePrefix string
	snapshotFolderNamePrefix     string
}

func NewPathHelper(tempWriterFolderNamePrefix, tempReceiverFolderNamePrefix, snapshotFolderNamePrefix string) PathHelper {
	return &pathHelperImpl{
		tempWriterFolderNamePrefix:   tempWriterFolderNamePrefix,
		tempReceiverFolderNamePrefix: tempReceiverFolderNamePrefix,
		snapshotFolderNamePrefix:     snapshotFolderNamePrefix,
	}
}

func (pb *pathHelperImpl) SnapshotFileName(fileType babuzapb.SnapshotFileType, snapshotIndex uint64, tag string) (string, error) {
	switch fileType {
	case babuzapb.SnapshotFileType_StateMachine:
		return fmt.Sprintf("%016x-%s.fsm", snapshotIndex, tag), nil
	case babuzapb.SnapshotFileType_Cluster:
		return fmt.Sprintf("%016x.cluster", snapshotIndex), nil
	case babuzapb.SnapshotFileType_Session:
		return fmt.Sprintf("%016x.session", snapshotIndex), nil
	case babuzapb.SnapshotFileType_Metadata:
		return fmt.Sprintf("%016x.metadata", snapshotIndex), nil
	default:
		return "", fmt.Errorf("snapshotor[index=%d]: unkonwn file type (%d) (tag=%s)", snapshotIndex, fileType, tag)
	}
}

func (pb *pathHelperImpl) SnapshotFolderName(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) (string, error) {
	switch folderType {
	case babuzapb.SnapshotFolderType_TempWrite:
		return fmt.Sprintf("%s%016x", pb.tempWriterFolderNamePrefix, snapshotIndex), nil
	case babuzapb.SnapshotFolderType_TempReceive:
		return fmt.Sprintf("%s%016x", pb.tempReceiverFolderNamePrefix, snapshotIndex), nil
	case babuzapb.SnapshotFolderType_InstallSnapshot:
		return fmt.Sprintf("%s%016x", pb.snapshotFolderNamePrefix, snapshotIndex), nil
	default:
		return "", fmt.Errorf("snapshotor[index=%d]: unkonwn folder type (%d) ", snapshotIndex, folderType)
	}
}

func (pb *pathHelperImpl) GenerateSnapshotFilePath(snapshotDir string, fileType babuzapb.SnapshotFileType, snapIndex uint64, tag string) (string, error) {
	fileName, err := pb.SnapshotFileName(fileType, snapIndex, tag)
	if err != nil {
		return "", err
	}
	return filepath.Join(snapshotDir, fileName), nil
}

func (pb *pathHelperImpl) GenerateSnapshotFolderPath(srcDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	folderName, err := pb.SnapshotFolderName(folderType, snapIndex)
	if err != nil {
		return "", err
	}
	return filepath.Join(srcDir, folderName), nil
}

func (pb *pathHelperImpl) ParseMetadataFileName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, "%016x.metadata", &index)
	return index, err
}

func (pb *pathHelperImpl) ParseWriterTmpFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, pb.tempWriterFolderNamePrefix+"%016x", &index)
	return index, err
}
func (pb *pathHelperImpl) ParseReceiverTmpFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, pb.tempReceiverFolderNamePrefix+"%016x", &index)
	return index, err
}
func (pb *pathHelperImpl) ParseSnapshotFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, pb.snapshotFolderNamePrefix+"%016x", &index)
	return index, err
}

func (pb *pathHelperImpl) ParseBrokenFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, "broken-%016x", &index)
	return index, err
}

func (pb *pathHelperImpl) SnapshotFolderPrefix() string {
	return pb.snapshotFolderNamePrefix
}

func (pb *pathHelperImpl) TempWriterFolderPrefix() string {
	return pb.tempWriterFolderNamePrefix
}

func (pb *pathHelperImpl) TempReceiverFolderPrefix() string {
	return pb.tempReceiverFolderNamePrefix
}
