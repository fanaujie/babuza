package lsmtwal

import (
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type purgeRequest struct {
	groupID  ibabuza.RaftGroupID
	snapshot raftpb.Snapshot
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
