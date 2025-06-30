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
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/rediscommon"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/tidwall/redcon"
)

func Set(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}
	proposal := pb.RedisCommand{
		Type:       pb.RedisDataType_String,
		Command:    rediscommon.RedisSet,
		ArgsLength: 2,
		Args:       [][]byte{cmd.Args[1], cmd.Args[2]},
	}
	data, err := proposal.Marshal()
	if err != nil {
		conn.WriteError("ERR failed to marshal proposal: " + err.Error())
		return
	}
	result := clusterMgr.LocalPropose(ctx, groupID, data)
	defer result.Release()
	ar := result.WaitForApplyResult()
	if ar.Error != nil {
		conn.WriteError("ERR " + ar.Error.Error())
		return
	}
	conn.WriteString("OK")
}

func Get(ctx context.Context, conn redcon.Conn, cmd redcon.Command, groupID ibabuza.RaftGroupID, clusterMgr ClusterManager) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'get' command")
		return
	}
	fmt.Printf("Executing GET command for group %d with key %s\n", groupID, cmd.Args[1])
	v, err := clusterMgr.LocalQuery(groupID, &pb.RedisCommand{
		Type:       pb.RedisDataType_String,
		Command:    rediscommon.RedisGet,
		ArgsLength: 1,
		Args:       [][]byte{cmd.Args[1]},
	})
	if err != nil {
		if errors.Is(err, rediscommon.ErrKeyNotExist) {
			conn.WriteNull()
			return
		}
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteBulkString(v.(string))
}
