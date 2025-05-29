package cluster

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	multiraft "github.com/fanaujie/babuza/raft/multiraft"
	"github.com/tidwall/redcon"
)

type MultiRaft struct {
	localNode *multiraft.Node
}

func NewMultiRaft(localNode *multiraft.Node) *MultiRaft {
	return &MultiRaft{
		localNode: localNode,
	}
}

func (m *MultiRaft) IsLocalLeaderForGroup(groupID ibabuza.RaftGroupID) bool {
	return true
}

func (m *MultiRaft) RedirectToLeader(conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MultiRaft) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, log []byte) babuza.ProposedResult {
	return m.localNode.Propose(ctx, groupID, babuza.ClientSession{}, log)
}

func (m *MultiRaft) Query(groupID ibabuza.RaftGroupID, key *pb.RedisCommand) (any, error) {
	return m.localNode.Query(groupID, key)
}
