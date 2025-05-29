package rediscommon

import "errors"

var (
	ErrKeyNotExist      = errors.New("ERR key does not exist")
	ErrUnknownCommand   = errors.New("ERR unknown command")
	ErrInvalidQueryType = errors.New("ERR invalid query type")
)
