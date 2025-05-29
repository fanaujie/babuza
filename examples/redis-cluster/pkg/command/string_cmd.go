package command

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/tidwall/redcon"
)

func Set(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}
	proposal := pb.RedisCommand{
		Type:       pb.String,
		Command:    RedisSet,
		ArgsLength: 2,
		Args:       [][]byte{cmd.Args[1], cmd.Args[2]},
	}
	data, err := proposal.Marshal()
	if err != nil {
		conn.WriteError("ERR failed to marshal proposal: " + err.Error())
		return
	}
	result := clusterMgr.Propose(ctx, groupID, data)
	defer result.Release()
	ar := result.WaitForApplyResult()
	if ar.Error != nil {
		conn.WriteError("ERR " + ar.Error.Error())
		return
	}
	conn.WriteString("OK")
}
