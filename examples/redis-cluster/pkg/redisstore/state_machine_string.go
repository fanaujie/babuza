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


package redisstore

import (
	"errors"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/rediscommon"
)

func (r *RedisStateMachine) processString(cmd pb.RedisCommand) (any, error) {
	switch cmd.Command {
	case rediscommon.RedisSet:
		if len(cmd.Args) != 2 {
			return nil, errors.New("ERR wrong number of arguments for 'set' command")
		}
		r.kv.Set(string(cmd.Args[0]), string(cmd.Args[1]))
		return nil, nil
	case rediscommon.RedisGet:
		if len(cmd.Args) != 1 {
			return nil, errors.New("ERR wrong number of arguments for 'get' command")
		}
		value, exist := r.kv.Get(string(cmd.Args[0]))
		if !exist {
			return nil, rediscommon.ErrKeyNotExist
		}
		return value, nil
	default:
		return nil, rediscommon.ErrUnknownCommand
	}
}
