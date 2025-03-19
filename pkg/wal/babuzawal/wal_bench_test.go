package babuzawal

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/logfile"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkEtcdWalWrite100EntryOneBatch(b *testing.B) { benchmarkEtcdWalWriteEntry(b, 1000, 1000) }

func BenchmarkBabuzaWalWrite100EntryOneBatch(b *testing.B) {
	benchmarkBabuzaWalWriteEntry(b, 1000, 1000)
}
func BenchmarkBabuzaWalWrite100EntryOneBatchWithEntryIndex(b *testing.B) {
	benchmarkBabuzaWalWriteEntryWithEntryIndex(b, 1000, 1000)
}
func benchmarkEtcdWalWriteEntry(b *testing.B, size int, batch int) {
	p, err := ioutil.TempDir(os.TempDir(), "wal-bench-etcdwal")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := wal.Create(nil, p, []byte("somedata"))
	assert.Nil(b, err)

	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}
	n := 0
	var ents []raftpb.Entry
	e := raftpb.Entry{
		Term: 1,
		Type: raftpb.EntryNormal,
		Data: data,
	}
	b.ResetTimer()
	b.SetBytes(int64(e.Size()))
	for i := 0; i < b.N; i++ {
		e.Index++
		ents = append(ents, e)
		n++
		if n > batch {
			assert.Nil(b, w.Save(raftpb.HardState{}, ents))
			n = 0
			ents = ents[:0]
		}
	}
}
func benchmarkBabuzaWalWriteEntry(b *testing.B, size int, batch int) {
	p, err := ioutil.TempDir(os.TempDir(), "wal-bench-babuzawal")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(p)
	cfg := logfile.ManagerConfig{
		WalDir:            p,
		LogFileChunkSize:  64 * 1024 * 1024,
		AlignmentPageSize: 4096,
		PageWriterBufSize: 1024 * 64,
	}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte("somedata")
	logMgr, err := logfile.NewManager(cfg, cp)
	w, err := CreateWal(metadata, logMgr)
	assert.Nil(b, err)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}
	var ents []raftpb.Entry
	e := raftpb.Entry{
		Term: 1,
		Type: raftpb.EntryNormal,
		Data: data,
	}
	n := 0
	assert.Nil(b, w.currentLogFile.NextEntry(pb.WalNextEntry{
		NextTerm:  1,
		NextIndex: 1,
	}))
	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		e.Index++
		ents = append(ents, e)
		n++
		if n > batch {
			assert.Nil(b, w.Save(raftpb.HardState{}, ents))
			n = 0
			ents = ents[:0]
		}
	}
}

func benchmarkBabuzaWalWriteEntryWithEntryIndex(b *testing.B, size int, batch int) {
	p, err := ioutil.TempDir(os.TempDir(), "wal-bench-babuzawal")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(p)
	cfg := logfile.ManagerConfig{
		WalDir:            p,
		LogFileChunkSize:  64 * 1024 * 1024,
		AlignmentPageSize: 4096,
		PageWriterBufSize: 1024 * 64,
	}
	cp := allocator.NewDefaultTwoLevelPool(4096, 1024*1024)
	metadata := []byte("somedata")
	logMgr, err := logfile.NewManager(cfg, cp)
	w, err := CreateWal(metadata, logMgr)
	w.SetEntryIndexStorage(walbase.NewEntryStorage[storage.EntryMetadata](w.logMgr))
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}
	var ents []raftpb.Entry
	e := raftpb.Entry{
		Term: 1,
		Type: raftpb.EntryNormal,
		Data: data,
	}
	n := 0
	assert.Nil(b, w.currentLogFile.NextEntry(pb.WalNextEntry{
		NextTerm:  1,
		NextIndex: 1,
	}))
	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		e.Index++
		ents = append(ents, e)
		n++
		if n > batch {
			assert.Nil(b, w.Save(raftpb.HardState{}, ents))
			n = 0
			ents = ents[:0]
		}
	}
}
