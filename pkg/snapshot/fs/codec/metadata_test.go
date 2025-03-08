package codec

import (
	"bytes"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var (
	snapshotMetadata = babuzapb.SnapshotMetadata{
		Version: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 1,
				Term:  1,
			},
		},
		Files: map[string]babuzapb.SnapshotFileDesc{
			"one": {
				Tag:             "one",
				Metadata:        []byte{1, 2, 3, 4, 5},
				FileSize:        1024,
				CompressionType: 0,
			},
		},
	}
)

func TestCodec_Encode_Decode_MetadataFile(t *testing.T) {
	p := t.TempDir()
	genMetadataFileName, err := api.SnapshotFileName(babuzapb.SnapshotFileType_Metadata, 1, "")
	assert.Nil(t, err)
	metaFilePath := filepath.Join(p, genMetadataFileName)
	f := Metadata{}
	w, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
	assert.Nil(t, err)
	assert.Nil(t, f.Encode(w, snapshotMetadata))
	assert.Nil(t, w.Close())
	assert.Equal(t, true, fileutil.Exist(metaFilePath))
	f = Metadata{}
	r, err := os.Open(metaFilePath)
	assert.Nil(t, err)
	defer r.Close()
	m, err := f.Decode(r)
	assert.Nil(t, err)
	assert.Equal(t, snapshotMetadata, m)
}

func TestCodec_UnmarshalFile_Fail(t *testing.T) {
	p := t.TempDir()

	genMetadataFileName, err := api.SnapshotFileName(babuzapb.SnapshotFileType_Metadata, 1, "")
	assert.Nil(t, err)
	metaFilePath := filepath.Join(p, genMetadataFileName)
	f := Metadata{}
	w, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
	assert.Nil(t, err)
	assert.Nil(t, f.Encode(w, snapshotMetadata))
	assert.Nil(t, w.Close())
	assert.Equal(t, true, fileutil.Exist(metaFilePath))

	fileSize, err := fileutil.FileSize(metaFilePath)
	assert.Nil(t, err)
	rf, err := os.Open(metaFilePath)
	assert.Nil(t, err)
	tmpBufWriter := bytes.NewBuffer(make([]byte, 0, fileSize))
	io.Copy(tmpBufWriter, rf)
	assert.Nil(t, rf.Close())
	assert.Nil(t, os.Remove(metaFilePath))
	tmpBuf := tmpBufWriter.Bytes()
	//modify crc
	tmpBuf[metadataFileSizeFieldLength] = 0xff
	tmpBuf[metadataFileSizeFieldLength+1] = 0xff

	aw, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_WRONLY, fileutil.FileMode)
	_, err = aw.Write(tmpBuf)
	assert.Nil(t, err)
	fileutil.Sync(aw)
	assert.Nil(t, aw.Close())
	f = Metadata{}
	r, err := os.Open(metaFilePath)
	assert.Nil(t, err)
	defer r.Close()
	_, err = f.Decode(r)
	assert.Error(t, err)

}
