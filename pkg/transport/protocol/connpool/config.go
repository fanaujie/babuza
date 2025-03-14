package connpool

import "time"

type Config struct {
	MaxConnectionsPerHost int
	DialTimeout           time.Duration
	IdleTimeout           time.Duration
}
