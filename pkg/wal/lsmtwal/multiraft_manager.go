package lsmtwal

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal/storage"
	"github.com/fanaujie/babuza/pkg/wal/walbase"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type purgeRequest struct {
	groupID  ibabuza.RaftGroupID
	snapshot raftpb.Snapshot
}

type MultiRaftBadgerWalManager struct {
	logger       ibabuza.Logger
	db           *badger.DB
	prefixCache  *keyPrefixCache
	stopCh       chan struct{}
	purgerSnapCh chan purgeRequest
	purgerStopCh chan struct{}
}

type GroupEntryDataReader struct {
	manager *MultiRaftBadgerWalManager
	groupID ibabuza.RaftGroupID
}

func (r *GroupEntryDataReader) ReadEntriesData(readEntryIndex []walbase.EntryIndex[storage.EntryMetadata], destEnts []raftpb.Entry) error {
	return r.manager.ReadEntriesData(r.groupID, readEntryIndex, destEnts)
}

type MultiRaftConfig struct {
	InMemory           bool
	WalDir             string
	KeyPrefixCacheSize int
	ManagerType        WalManagerType
}

func NewMultiRaftWalManager(config MultiRaftConfig, logger ibabuza.Logger) ibabuza.MultiRaftWalManager {
	switch config.ManagerType {
	case WalManagerTypePebble:
		return NewMultiRaftPebbleWalManager(config, logger)
	case WalManagerTypeBadger:
		fallthrough
	default:
		return NewMultiRaftBadgerWalManager(config, logger)
	}
}
