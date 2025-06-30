package command

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/tidwall/redcon"
)

func Ping(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager) {
	conn.WriteString("PONG")
}

func Echo(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'echo' command")
		return
	}
	conn.WriteBulk(cmd.Args[1])
}
