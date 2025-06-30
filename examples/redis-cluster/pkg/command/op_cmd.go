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
