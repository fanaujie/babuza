package redisstore

import (
	"errors"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/command"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
)

func (r *RedisStateMachine) processString(cmd pb.RedisCommand) (any, error) {
	switch cmd.Command {
	case command.RedisSet:
		if len(cmd.Args) != 2 {
			return nil, errors.New("ERR wrong number of arguments for 'set' command")
		}
		r.kv.Set(string(cmd.Args[0]), string(cmd.Args[1]))
		return nil, nil
	default:
		return nil, errors.New("ERR unknown command '" + cmd.Command + "'")
	}
}
