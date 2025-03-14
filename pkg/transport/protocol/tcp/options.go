package tcp

import "time"

type Options struct {
	MaxConnectionsPerHost int
	DialTimeout           time.Duration
	IdleConnTimeout       time.Duration
	ReadDeadline          time.Duration
	WriteDeadline         time.Duration
}
