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
