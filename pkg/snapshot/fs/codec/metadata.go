package codec

import (
	"encoding/binary"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"hash/crc64"
	"io"
)

const (
	metadataFileSizeFieldLength = 8
	mMetadataFileCrcFieldLength = 8
)

type Metadata struct{}

func (md *Metadata) Encode(destW io.Writer, m babuzapb.SnapshotMetadata) error {
	d, err := m.Marshal()
	if err != nil {
		return nil
	}
	h := crc64.New(crcfile.Crc64Table)
	if _, err = h.Write(d); err != nil {
		return err
	}
	byteSlice := allocator.Acquire(metadataFileSizeFieldLength)
	defer allocator.Release(byteSlice)
	buf := byteSlice.Buffer[:metadataFileSizeFieldLength]
	binary.LittleEndian.PutUint64(buf, uint64(len(d)))
	if _, err = destW.Write(buf); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buf, h.Sum64())
	if _, err = destW.Write(buf); err != nil {
		return err
	}
	if _, err = destW.Write(d); err != nil {
		return err
	}
	return nil
}

func (md *Metadata) Decode(srcR io.Reader) (babuzapb.SnapshotMetadata, error) {
	byteSlice := allocator.Acquire(metadataFileSizeFieldLength)
	defer allocator.Release(byteSlice)
	buf := byteSlice.Buffer[:metadataFileSizeFieldLength]
	_, err := srcR.Read(buf)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	dataSize := binary.LittleEndian.Uint64(buf)

	//read crc
	_, err = srcR.Read(buf)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	crc := binary.LittleEndian.Uint64(buf)
	h := crc64.New(crcfile.Crc64Table)
	te := io.TeeReader(srcR, h)
	dataByteSlice := allocator.Acquire(int(dataSize))
	defer allocator.Release(dataByteSlice)
	dataBuf := dataByteSlice.Buffer[:dataSize]
	_, err = te.Read(dataBuf)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	if crc != h.Sum64() {
		return babuzapb.SnapshotMetadata{}, fmt.Errorf("snapshotor: mismatch metadata crc(%d != %d)", crc, h.Sum64())
	}
	m := babuzapb.SnapshotMetadata{}
	if err = m.Unmarshal(dataBuf); err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	return m, nil
}
