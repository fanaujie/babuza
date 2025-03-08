package api

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"path/filepath"
)

const (
	TempWriterFolderNamePrefix   = "tmp-writer-"
	TempReceiverFolderNamePrefix = "tmp-receiver-"
	SnapshotFolderNamePrefix     = "snapshot-"
)

func SnapshotFileName(fileType babuzapb.SnapshotFileType, snapshotIndex uint64, tag string) (string, error) {
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

func SnapshotFolderName(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) (string, error) {
	switch folderType {
	case babuzapb.SnapshotFolderType_TempWrite:
		return fmt.Sprintf("%s%016x", TempWriterFolderNamePrefix, snapshotIndex), nil
	case babuzapb.SnapshotFolderType_TempReceive:
		return fmt.Sprintf("%s%016x", TempReceiverFolderNamePrefix, snapshotIndex), nil
	case babuzapb.SnapshotFolderType_InstallSnapshot:
		return fmt.Sprintf("%s%016x", SnapshotFolderNamePrefix, snapshotIndex), nil
	default:
		return "", fmt.Errorf("snapshotor[index=%d]: unkonwn folder type (%d) ", snapshotIndex, folderType)
	}
}

func GenerateSnapshotFilePath(snapshotDir string, fileType babuzapb.SnapshotFileType, snapIndex uint64, tag string) (string, error) {
	fileName, err := SnapshotFileName(fileType, snapIndex, tag)
	if err != nil {
		return "", err
	}
	return filepath.Join(snapshotDir, fileName), nil
}

func GenerateSnapshotFolderPath(srcDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	folderName, err := SnapshotFolderName(folderType, snapIndex)
	if err != nil {
		return "", err
	}
	return filepath.Join(srcDir, folderName), nil
}

func ParseMetadataFileName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, "%016x.metadata", &index)
	return index, err
}

func ParseWriterTmpFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, TempWriterFolderNamePrefix+"%016x", &index)
	return index, err
}
func ParseReceiverTmpFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, TempReceiverFolderNamePrefix+"%016x", &index)
	return index, err
}
func ParseSnapshotFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, SnapshotFolderNamePrefix+"%016x", &index)
	return index, err
}

func ParseBrokenFolderName(name string) (uint64, error) {
	var index uint64
	_, err := fmt.Sscanf(name, "broken-%016x", &index)
	return index, err
}
