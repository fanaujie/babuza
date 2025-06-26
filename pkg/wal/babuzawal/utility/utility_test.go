package utility

import (
	"crypto/rand"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io/ioutil"
	mathRand "math/rand"
	"os"
	"path/filepath"
	"testing"
)

func genEntries(t *testing.T, dir string, desc iwal.LogFileDesc, count, minEntryDataSize, maxEntryDataSize int) (
	[]walbase.EntryIndex[storage.EntryIndexMetadata], []raftpb.Entry, [][]byte) {

	maxLogSize := maxEntryDataSize + ((8 - (maxEntryDataSize % 8)) % 8)
	handle, err := CreateLogFileHandle(filepath.Join(dir, desc.GetLogFileName()), count*maxLogSize)
	assert.Nil(t, err)
	defer handle.Close()
	var expectData [][]byte
	var entries []raftpb.Entry
	var entsIndex []walbase.EntryIndex[storage.EntryIndexMetadata]
	var offset = int64(LogFileHeaderLength)
	entIndex := walbase.EntryIndex[storage.EntryIndexMetadata]{
		Term:  1,
		Index: 1,
		Type:  raftpb.EntryNormal,
		Metadata: storage.EntryIndexMetadata{
			FileId: desc.Id,
		},
	}
	ent := raftpb.Entry{
		Term:  1,
		Index: 1,
		Type:  raftpb.EntryNormal,
	}
	cp := allocator.NewByteSlicePool(64, 1024, 2)

	for i := 0; i < count; i++ {
		func() {
			dataSize := mathRand.Intn(maxEntryDataSize-minEntryDataSize+1) + minEntryDataSize
			totalSize := dataSize + (8-(dataSize%8))%8
			data := make([]byte, dataSize, totalSize)
			rand.Read(data)
			b := cp.Acquire(totalSize)
			defer cp.Release(b)
			copy(b.Buffer, data)
			entIndex.Index = uint64(i + 1)
			ent.Index = entIndex.Index
			entIndex.Metadata.Offset = offset
			entIndex.Metadata.DataLen = int64(dataSize)
			entIndex.Metadata.DataCapacity = int64(totalSize)
			_, err = handle.Write(b.Buffer[:totalSize])
			assert.Nil(t, err)
			offset += int64(totalSize)

			expectData = append(expectData, data)
			entsIndex = append(entsIndex, entIndex)
			entries = append(entries, ent)
		}()
	}

	return entsIndex, entries, expectData
}

func TestUtitlity_ReadEntriesData(t *testing.T) {

	p, err := ioutil.TempDir("", "utility")
	assert.Nil(t, err)
	defer os.RemoveAll(p)

	for _, tc := range []struct {
		desc             iwal.LogFileDesc
		count            int
		minEntryDataSize int
		maxEntryDataSize int
	}{
		{
			desc: iwal.LogFileDesc{
				Id:            1,
				StartLogIndex: 1,
			},
			count:            128,
			minEntryDataSize: 32,
			maxEntryDataSize: 64,
		},
		{
			desc: iwal.LogFileDesc{
				Id:            2,
				StartLogIndex: 1,
			},
			count:            142,
			minEntryDataSize: 34,
			maxEntryDataSize: 77,
		},
	} {
		readMetadata, destEnts, expectEntriesData := genEntries(t, p, tc.desc, tc.count, tc.minEntryDataSize, tc.maxEntryDataSize)
		assert.Nil(t, ReadEntriesData(filepath.Join(p, tc.desc.GetLogFileName()), readMetadata, destEnts))
		for i := range destEnts {
			assert.Equal(t, destEnts[i].Data, expectEntriesData[i])
		}
	}
}
