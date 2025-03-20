package io

import (
	"archive/tar"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"io"
	"path/filepath"
)

type Reader struct {
	fs              api.FileSystem
	dir             string
	metadata        babuzapb.SnapshotMetadata
	metadataEncoder MetadataEncoder
	reader          map[string]io.ReadCloser
}

func NewReader(fs api.FileSystem, dir string, metadata babuzapb.SnapshotMetadata, metadataEncoder MetadataEncoder) *Reader {
	return &Reader{
		fs:              fs,
		dir:             dir,
		metadata:        metadata,
		metadataEncoder: metadataEncoder,
		reader:          make(map[string]io.ReadCloser),
	}
}

func (r *Reader) Open(fileTag string) (io.Reader, ibabuza.StateMachineFileDesc, error) {
	filename, err := r.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_StateMachine, r.metadata.Snapshot.Metadata.Index, fileTag)
	if err != nil {
		return nil, ibabuza.StateMachineFileDesc{}, err
	}
	fp := filepath.Join(r.dir, filename)
	reader, fs, err := r.open(fileTag, fp)
	if err != nil {
		return nil, ibabuza.StateMachineFileDesc{}, err
	}
	return reader, fs, nil
}

func (r *Reader) Close() error {
	me := multierror.New()
	for _, cf := range r.reader {
		if err := cf.Close(); err != nil {
			me.Append(err)
		}
	}
	return me.Get()
}

func (r *Reader) ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error {
	visit := func(snapshotIndex uint64, fileDesc babuzapb.SnapshotFileDesc) error {
		filename, err := r.fs.PathHelper().SnapshotFileName(fileDesc.FileType, snapshotIndex, fileDesc.Tag)
		if err != nil {
			return err
		}
		fp := filepath.Join(r.dir, filename)
		f, err := r.fs.FileRead(fp)
		if err != nil {
			return err
		}
		defer f.Close()
		return visitor(f, fileDesc)
	}

	for tag := range r.metadata.Files {
		if err := visit(r.metadata.Snapshot.Metadata.Index, r.metadata.Files[tag]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) Metadata() babuzapb.SnapshotMetadata {
	return r.metadata
}

func (r *Reader) Cluster() (io.Reader, error) {
	filename, err := r.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Cluster, r.metadata.Snapshot.Metadata.Index, "")
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(r.dir, filename)
	reader, _, err := r.open(filename, fp)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *Reader) Session() (io.Reader, error) {
	filename, err := r.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Session, r.metadata.Snapshot.Metadata.Index, "")
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(r.dir, filename)
	reader, _, err := r.open(filename, fp)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *Reader) CreateTarArchiveReader() (io.ReadCloser, error) {
	pipeR, pipeW := io.Pipe()
	go func() {
		var wErr error
		defer func() {
			pipeW.CloseWithError(wErr)
		}()
		tw := tar.NewWriter(pipeW)
		addToTarFile := func(srcFile string, destW io.Writer) error {
			f, err := r.fs.FileRead(srcFile)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(destW, f)
			return err
		}

		metadataFilename, err := r.fs.PathHelper().SnapshotFileName(babuzapb.SnapshotFileType_Metadata, r.metadata.Snapshot.Metadata.Index, "")
		if err != nil {
			wErr = err
			return
		}
		metadataPath := filepath.Join(r.dir, metadataFilename)
		fileSize, err := r.fs.FileSize(metadataPath)
		if err != nil {
			wErr = err
			return
		}
		if err = tw.WriteHeader(&tar.Header{
			Name: metadataFilename,
			Mode: 0600,
			Size: fileSize,
		}); err != nil {
			wErr = err
			return
		}
		if err = r.metadataEncoder.Encode(tw, r.metadata); err != nil {
			wErr = err
			return
		}
		for _, fileDesc := range r.metadata.Files {
			filename, err := r.fs.PathHelper().SnapshotFileName(fileDesc.FileType, r.metadata.Snapshot.Metadata.Index, fileDesc.Tag)
			if err != nil {
				wErr = err
				return
			}
			fp := filepath.Join(r.dir, filename)
			if err = tw.WriteHeader(&tar.Header{
				Name: filename,
				Mode: 0600,
				Size: fileDesc.FileSize,
			}); err != nil {
				wErr = err
				return
			}
			if err = addToTarFile(fp, tw); err != nil {
				wErr = err
				return
			}
		}
	}()
	return pipeR, nil
}

func (r *Reader) open(fileTag string, filePath string) (io.Reader, ibabuza.StateMachineFileDesc, error) {
	f, err := r.fs.FileRead(filePath)
	if err != nil {
		return nil, ibabuza.StateMachineFileDesc{}, err
	}
	metadata, ok := r.metadata.Files[fileTag]
	if !ok {
		return nil, ibabuza.StateMachineFileDesc{}, fmt.Errorf("snapshotor[index=%d]: not found tag(%s)", r.metadata.Snapshot.Metadata.Index, fileTag)
	}
	decompressor, err := codec.CreateDeCompressor(metadata.CompressionType, f)
	if err != nil {
		return nil, ibabuza.StateMachineFileDesc{}, err
	}
	var fm = ibabuza.StateMachineFileDesc{
		Tag:      fileTag,
		Metadata: make([]byte, len(metadata.Metadata)),
		FilePath: filePath,
	}
	copy(fm.Metadata, metadata.Metadata)
	r.reader[fileTag] = f
	return decompressor, fm, nil
}
