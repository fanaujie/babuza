package connpool

import "time"

type Config struct {
	MaxConnectionsPerHost int
	IdleTimeout           time.Duration
}
