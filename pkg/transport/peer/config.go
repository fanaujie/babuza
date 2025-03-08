package peer

import (
	"time"
)

type RaftPeerConfig struct {
	LimiterMaxBatchMessageSize int64
	SnapshotChunkSize          int64
	RaftMsgQueueSize           int64
	DialTimeout                time.Duration
	SendSnapshotChunkInterval  time.Duration
}
