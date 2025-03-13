package connpool

import "time"

type Options struct {
	MaxConnectionsPerHost int
	DialTimeout           time.Duration
	IdleTimeout           time.Duration
	ReadDeadline          time.Duration
	WriteDeadline         time.Duration
}
