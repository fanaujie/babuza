package io

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type ChunkValidator struct {
	fs            api.FileSystem
	filePath      string
	nextChunkId   int64
	receivedSize  int
	continueCrc32 uint32
	finish        bool
	snapshotIndex uint64
}

func NewChunkValidator(fs api.FileSystem, path string) *ChunkValidator {
	return &ChunkValidator{
		fs:          fs,
		filePath:    path,
		nextChunkId: 1,
	}
}

func (v *ChunkValidator) ValidateAndAppend(msg babuzapb.SnapshotChunkMessage) error {
	if v.finish {
		return fmt.Errorf("snapshotor[index=%d]: already finished chunk (tag=%s)", v.snapshotIndex, msg.FileTag)
	}
	if v.nextChunkId != msg.Id {
		return fmt.Errorf("snapshotor[index=%d]: not expectd chunk id (expectd=%d,get=%d)", v.snapshotIndex, v.nextChunkId, msg.Id)
	}
	v.continueCrc32 = crc32.Update(v.continueCrc32, crcTable, msg.Data)
	if v.continueCrc32 != msg.ContinueCrc32 {
		return fmt.Errorf("snapshotor[index=%d]: mismatch chunk crc (expected=%d,get=%d) (tag=%s)",
			v.snapshotIndex, v.continueCrc32, msg.ContinueCrc32, msg.FileTag)
	}
	v.receivedSize += len(msg.Data)
	v.nextChunkId++
	if msg.LastChunk {
		v.finish = true
	}
	if err := v.fs.FileAppendData(v.filePath, msg.Data, msg.LastChunk); err != nil {
		return err
	}
	return nil
}

type MetadataDecoder interface {
	Decode(srcR io.Reader) (babuzapb.SnapshotMetadata, error)
}

type FileValidator struct {
	fs             api.FileSystem
	metadataDecode MetadataDecoder
}

func NewFileValidator(fs api.FileSystem, metadataDecode MetadataDecoder) *FileValidator {
	return &FileValidator{
		fs:             fs,
		metadataDecode: metadataDecode,
	}
}

func (f *FileValidator) ValidateMetadataFile(dir string) (babuzapb.SnapshotMetadata, error) {
	snapshotIndexs, err := f.fs.FindMetadataFile(dir)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	if len(snapshotIndexs) == 0 {
		return babuzapb.SnapshotMetadata{}, fmt.Errorf("snapshotor: not found metadata file in dir %s", dir)
	} else if len(snapshotIndexs) > 1 {
		return babuzapb.SnapshotMetadata{}, fmt.Errorf("snapshotor: found more than one metadata file in dir %s (files=%d)", dir, len(snapshotIndexs))
	}

	filename, err := f.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, snapshotIndexs[0], "")
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	fp := filepath.Join(dir, filename)

	r, err := f.fs.FileRead(fp)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	defer r.Close()
	return f.metadataDecode.Decode(r)
}

func (f *FileValidator) ValidateSnapshotFiles(dir string, m babuzapb.SnapshotMetadata) error {
	for _, snapFile := range m.Files {
		filename, err := f.fs.PathHelper().SnapshotFileName(snapFile.FileType, m.Snapshot.Metadata.Index, snapFile.Tag)
		if err != nil {
			return err
		}
		fp := filepath.Join(dir, filename)

		if f.fs.ExistFilePath(fp) == false {
			return fmt.Errorf("snapshotor[index=%d]: not found snapshot file (tag=%s)", m.Snapshot.Metadata.Index, snapFile.Tag)
		}

		if err = func() error {
			crcR, err := f.fs.CrcFileRead(fp)
			if err != nil {
				return err
			}
			defer crcR.Close()
			_, err = io.Copy(io.Discard, crcR)
			if err != nil {
				return err
			}
			if int64(crcR.FileSize()) != snapFile.FileSize {
				return fmt.Errorf("snapshotor[index=%d]: mismatch file size(%d != %d) (tag=%s)", m.Snapshot.Metadata.Index, crcR.FileSize(),
					snapFile.FileSize, snapFile.Tag)
			}
			crc := crcR.Crc()
			if crc != snapFile.FileCrc64 {
				return fmt.Errorf("snapshotor[index=%d]: mismatch crc(%d != %d) (tag=%s)", m.Snapshot.Metadata.Index, crc, snapFile.FileCrc64,
					snapFile.Tag)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}
