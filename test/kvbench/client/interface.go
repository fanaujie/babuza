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

	// Close closes the client connection
	Close() error
}

// Config stores configuration for creating a client
type Config struct {
	// Endpoints is a list of server endpoints to connect to
	Endpoints []string

	// TargetLeader when true, requests will be sent only to the leader
	TargetLeader bool

	// ShardCount is the number of shards in the server
	ShardCount int

	// DialTimeout is the timeout for establishing connection
	DialTimeout time.Duration

	// RequestTimeout is the timeout for a single request
	RequestTimeout time.Duration
}

// GetShardForKey determines which shard a key belongs to
func GetShardForKey(key []byte, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}

	// Simple hash function - sum of bytes modulo shard count
	var sum int
	for _, b := range key {
		sum += int(b)
	}

	return sum % shardCount
}

// Factory defines functions for creating client instances
type Factory interface {
	// NewClient creates a new client with the given configuration
	NewClient(config Config) (Client, error)
}
