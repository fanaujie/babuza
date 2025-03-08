package transport

import (
	"time"
)

type Options struct {
	DialTimeout                    time.Duration
	PeerLimiterMaxBatchMessageSize int64
	PeerQueueSize                  int64
	PeerSnapshotChunkSize          int64
}

func DefaultOptions() Options {
	return Options{
		DialTimeout:                    time.Second * 3,
		PeerLimiterMaxBatchMessageSize: 3 * 1024 * 1024,
		PeerQueueSize:                  256,
		PeerSnapshotChunkSize:          3 * 1024 * 1024,
	}
}
