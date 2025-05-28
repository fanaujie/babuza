package redisstore

import (
	"github.com/fanaujie/babuza/ibabuza"
)

// RedisStateMachine implements a simple Redis-like key-value store state machine
type RedisStateMachine struct {
	logger ibabuza.Logger
}

// NewRedisStateMachine creates a new Redis state machine
func NewRedisStateMachine(logger ibabuza.Logger) *RedisStateMachine {
	return &RedisStateMachine{
		logger: logger,
	}
}

// Apply processes a command and updates the state machine
func (r *RedisStateMachine) Apply(entry ibabuza.Entry) ibabuza.ApplyResult {

	return ibabuza.ApplyResult{
		LogIndex: entry.Index,
		Response: nil,
	}
}

// SaveSnapshot saves the current state to a snapshot
func (r *RedisStateMachine) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {

	return nil
}

// RestoreFromSnapshot restores the state from a snapshot
func (r *RedisStateMachine) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {

	return nil
}

// Close cleans up resources
func (r *RedisStateMachine) Close() error {
	return nil
}
