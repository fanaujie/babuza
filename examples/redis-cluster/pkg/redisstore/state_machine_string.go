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
