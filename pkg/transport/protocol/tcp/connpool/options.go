package connpool

import "time"

type Options struct {
	WriteDeadline         time.Duration
	ReadDeadline          time.Duration
	MaxBufferSize         int
	MaxConnectionsPerHost int
	DialTimeout           time.Duration
	IdleTimeout           time.Duration
}
