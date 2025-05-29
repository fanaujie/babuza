package command

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/tidwall/redcon"
	"hash"
	"hash/fnv"
	"strings"
)

type ClusterManager interface {
	IsLocalLeaderForGroup(groupID ibabuza.RaftGroupID) bool
	RedirectToLeader(conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID) ([]byte, error)
	Propose(ctx context.Context, groupID ibabuza.RaftGroupID, log []byte) babuza.ProposedResult
	Query(groupID ibabuza.RaftGroupID, key *pb.RedisCommand) (any, error)
}

type Handler struct {
	OperationCmd bool
	Executor     func(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager)
}
type Router struct {
	shards     int
	hash       hash.Hash32
	table      map[string]Handler
	clusterMgr ClusterManager
}

func NewRouter(shards int, clusterMgr ClusterManager) *Router {
	return &Router{
		shards:     shards,
		hash:       fnv.New32(),
		table:      make(map[string]Handler),
		clusterMgr: clusterMgr,
	}
}

func (r *Router) RegisterCommand(name string, handler Handler) {
	name = strings.ToLower(name)
	r.table[name] = handler
}

func (r *Router) RunCommand(conn redcon.Conn, cmd redcon.Command) {
	redisCmd := strings.ToLower(string(cmd.Args[0]))
	handler, exists := r.table[redisCmd]
	if !exists {
		conn.WriteError("ERR command '" + redisCmd + "' not implemented yet")
	} else {
		if handler.OperationCmd {
			handler.Executor(context.Background(), conn, cmd, 0, r.clusterMgr)
			return
		}
		groupID := ibabuza.RaftGroupID(r.hashKey(cmd.Args[1]) % uint32(r.shards))
		if !r.clusterMgr.IsLocalLeaderForGroup(groupID) {
			resp, err := r.clusterMgr.RedirectToLeader(conn, cmd, groupID)
			if err != nil {
				conn.WriteError("ERR redirect failed: " + err.Error())
			}
			conn.WriteRaw(resp)
			return
		}
		handler.Executor(context.Background(), conn, cmd, groupID, r.clusterMgr)
	}
}

func (r *Router) hashKey(key []byte) uint32 {
	r.hash.Reset()
	r.hash.Write(key)
	return r.hash.Sum32()
}
