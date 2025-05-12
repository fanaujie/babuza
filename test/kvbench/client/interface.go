package client

import (
	"context"
	"time"
)

// Response represents a response from the KV service
type Response struct {
	Error   error
	EndTime time.Time
}

// Client defines the interface for interacting with the key-value store
type Client interface {
	// Put puts a key-value pair into the store
	Put(ctx context.Context, key, value []byte) Response

	// Get retrieves a value for the given key
	Get(ctx context.Context, key []byte) Response

	// Delete removes a key-value pair
	Delete(ctx context.Context, key []byte) Response
}

// Config stores configuration for creating a client
type Config struct {
	// Endpoints is a list of server endpoints to connect to
	Endpoints []string

	Connections uint

	// TargetLeader when true, requests will be sent only to the leader
	TargetLeader bool

	// ShardCount is the number of shards in the server
	ShardCount uint
}

// GetShardForKey determines which shard a key belongs to
func GetShardForKey(key []byte, shardCount uint) uint {
	if shardCount <= 1 {
		return 0
	}

	// Simple hash function - sum of bytes modulo shard count
	var sum uint
	for _, b := range key {
		sum += uint(b)
	}

	return sum % shardCount
}

// Factory defines functions for creating client instances
type Factory interface {
	// NewClient creates a new client with the given configuration
	NewClient(config Config) Client

	// Close closes the client connection
	Close() error
}
