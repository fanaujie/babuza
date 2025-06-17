package command

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/tidwall/redcon"
	"hash/crc32"
	"strings"
)

type ClusterManager interface {
	UpdateRoutingTable(map[uint64]string)
	IsLocalLeaderForGroup(groupID ibabuza.RaftGroupID) bool
	RedirectToLeader(conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID)
	LocalPropose(ctx context.Context, groupID ibabuza.RaftGroupID, log []byte) babuza.ProposedResult
	LocalQuery(groupID ibabuza.RaftGroupID, key *pb.RedisCommand) (any, error)
}

type Handler struct {
	OperationCmd bool
	Executor     func(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager)
}
type Router struct {
	shards     int
	table      map[string]Handler
	clusterMgr ClusterManager
}

func NewRouter(shards int, clusterMgr ClusterManager) *Router {
	return &Router{
		shards:     shards,
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
	fmt.Println("redisCmd", redisCmd)
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
			fmt.Printf("Redirecting command '%s' to leader for group %d\n", redisCmd, groupID)
			r.clusterMgr.RedirectToLeader(conn, cmd, groupID)
			return
		}
		fmt.Printf("Executing local command '%s' for group %d\n", redisCmd, groupID)
		handler.Executor(context.Background(), conn, cmd, groupID, r.clusterMgr)
	}
}

func (r *Router) hashKey(key []byte) uint32 {
	return crc32.ChecksumIEEE(key)
}
