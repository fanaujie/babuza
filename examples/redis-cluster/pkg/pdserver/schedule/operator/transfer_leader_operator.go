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


package operator

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/infostore"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

type TransferLeaderOperator struct {
	raftGroupID uint64
	newLeader   babuzapb.RaftPeerAttribute
}

func NewTransferLeaderOperator(raftGroupID uint64, newLeader babuzapb.RaftPeerAttribute) *TransferLeaderOperator {
	return &TransferLeaderOperator{
		raftGroupID: raftGroupID,
		newLeader:   newLeader,
	}
}

func (t *TransferLeaderOperator) RaftGroupID() uint64 {
	return t.raftGroupID
}

func (t *TransferLeaderOperator) Finish(groupInfo infostore.GroupInfo) bool {
	leader, _ := groupInfo.Leader()
	if leader.PeerID == t.newLeader.PeerID {
		return true
	}
	return false
}

func (t *TransferLeaderOperator) Payload() any {
	return t.newLeader
}
