package io

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"path/filepath"
)

type ValidateFile interface {
	ValidateMetadataFile(dir string) (babuzapb.SnapshotMetadata, error)
	ValidateSnapshotFiles(dir string, m babuzapb.SnapshotMetadata) error
}

type Receiver struct {
	fs             api.FileSystem
	dir            string
	metadata       babuzapb.SnapshotMetadata
	metadataEn     MetadataEncoder
	installer      Installer
	fileValidator  ValidateFile
	chunkValidator map[string]*ChunkValidator
}

func NewReceiver(fs api.FileSystem, dir string, metadata babuzapb.SnapshotMetadata, metadataEn MetadataEncoder, installer Installer,
	fileValidator ValidateFile) *Receiver {
	return &Receiver{
		fs:             fs,
		dir:            dir,
		metadata:       metadata,
		metadataEn:     metadataEn,
		installer:      installer,
		fileValidator:  fileValidator,
		chunkValidator: make(map[string]*ChunkValidator),
	}
}

func (r *Receiver) SaveChunk(snapshotIndex uint64, msg babuzapb.SnapshotChunkMessage) error {
	if r.metadata.Snapshot.Metadata.Index != snapshotIndex {
		return fmt.Errorf("snapshotor: mismatch snapshot index(expected=%d,get=%d)", r.metadata.Snapshot.Metadata.Index, snapshotIndex)
	}

	chunkValidator, ok := r.chunkValidator[msg.FileTag]
	if !ok {
		filename, err := r.fs.PathHelper().SnapshotFileName(msg.FileType, snapshotIndex, msg.FileTag)
		if err != nil {
			return err
		}
		fp := filepath.Join(r.dir, filename)
		chunkValidator = NewChunkValidator(r.fs, fp)
		r.chunkValidator[msg.FileTag] = chunkValidator
	}

	if err := chunkValidator.ValidateAndAppend(msg); err != nil {
		return err
	}
	if msg.LastChunk {
		delete(r.chunkValidator, msg.FileTag)
	}
	return nil
}

func (r *Receiver) DeleteDir() error {
	return r.fs.RemoveDir(r.dir)
}

func (r *Receiver) Commit(snapshotIndex uint64) error {
	if r.metadata.Snapshot.Metadata.Index != snapshotIndex {
		return fmt.Errorf("snapshotor: mismatch snapshot index(expected=%d,get=%d)", r.metadata.Snapshot.Metadata.Index, snapshotIndex)
	}

	filename, err := r.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, snapshotIndex, "")
	if err != nil {
		return err
	}
	fp := filepath.Join(r.dir, filename)

	w, err := r.fs.FileWrite(fp)
	if err != nil {
		return err
	}
	defer w.Close()

	if err := r.metadataEn.Encode(w, r.metadata); err != nil {
		return err
	}

	if err := r.fs.SyncDir(r.dir); err != nil {
		return err
	}

	m, err := r.fileValidator.ValidateMetadataFile(r.dir)
	if err != nil {
		return err
	}

	if m.Version != r.installer.SnapshotVersion() {
		return fmt.Errorf("snapshotor: mismatch snapshot version(expected=%d,get=%d)", r.installer.SnapshotVersion(), m.Version)
	}

	if err = r.fileValidator.ValidateSnapshotFiles(r.dir, m); err != nil {
		return err
	}

	if err = r.installer.CommitSnapshot(babuzapb.SnapshotFolderType_TempReceive, snapshotIndex); err != nil {
		return err
	}

	return nil
}
