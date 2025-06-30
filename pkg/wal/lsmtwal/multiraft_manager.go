// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
