package kvstore

type KVPair struct {
	Key   []byte
	Value []byte
}

type BatchKVPair []KVPair

const (
	batchKvCount        = 16
	BadgerDBSnapshotTag = "kv-badgerDB"
	MemorySnapshotTag   = "kv-memory"
)
