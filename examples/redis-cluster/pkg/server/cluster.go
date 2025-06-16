package server

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft"
	"github.com/tidwall/redcon"
)

type Cluster struct {
	store *multiraft.Store
}

func NewCluster(store *multiraft.Store) *Cluster {
	return &Cluster{
		store: store,
	}
}

func (m *Cluster) IsLocalLeaderForGroup(groupID ibabuza.RaftGroupID) bool {
	return true
}

func (m *Cluster) RedirectToLeader(conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Cluster) LocalPropose(ctx context.Context, groupID ibabuza.RaftGroupID, log []byte) babuza.ProposedResult {
	return m.store.Propose(ctx, groupID, babuza.ClientSession{}, log)
}

func (m *Cluster) LocalQuery(groupID ibabuza.RaftGroupID, key *pb.RedisCommand) (any, error) {
	return m.store.Query(groupID, key)
}
