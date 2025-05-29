package redisstore

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/rediscommon"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/redisstore/datastruct"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

const (
	StringSnapshotTag = "redis_string_snapshot"
)

type RedisStateMachine struct {
	logger ibabuza.Logger
	kv     *datastruct.String
}

func NewRedisStateMachine(logger ibabuza.Logger) *RedisStateMachine {
	return &RedisStateMachine{
		logger: logger,
		kv:     datastruct.NewString(),
	}
}

// Apply processes a command and updates the state machine
func (r *RedisStateMachine) Apply(entry ibabuza.Entry) ibabuza.ApplyResult {
	var redisCmd pb.RedisCommand
	if err := redisCmd.Unmarshal(entry.Command); err != nil {
		r.logger.Panicf("failed to unmarshal command: %v", err)
	}
	var result any
	var err error
	switch redisCmd.Type {
	case pb.String:
		result, err = r.processString(redisCmd)
	}
	return ibabuza.ApplyResult{
		LogIndex: entry.Index,
		Response: result,
		Error:    err,
	}
}

// SaveSnapshot saves the current state to a snapshot
func (r *RedisStateMachine) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	snapshotTags := []string{StringSnapshotTag}
	for _, tag := range snapshotTags {
		w, err := writer.CreateStateMachineFile(tag, babuzapb.SnapshotFileCompression_Snappy)
		if err != nil {
			return err
		}
		switch tag {
		case StringSnapshotTag:
			if err = r.kv.SaveSnapshot(w); err != nil {
				return err
			}
		}
		_ = w.Close()
	}
	return nil
}

// RestoreFromSnapshot restores the state from a snapshot
func (r *RedisStateMachine) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	snapshotTags := []string{StringSnapshotTag}
	for _, tag := range snapshotTags {
		w, _, err := reader.Open(tag)
		if err != nil {
			return err
		}
		switch tag {
		case StringSnapshotTag:
			if err = r.kv.RestoreFromSnapshot(w); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close cleans up resources
func (r *RedisStateMachine) Close() error {
	return nil
}

// Query retrieves a value based on the key
func (r *RedisStateMachine) Query(key any) (any, error) {
	if cmd, ok := key.(*pb.RedisCommand); ok {
		switch cmd.Type {
		case pb.String:
			return r.processString(*cmd)
		}
	}
	return nil, rediscommon.ErrInvalidQueryType
}
