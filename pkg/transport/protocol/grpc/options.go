package grpc

import "time"

type Options struct {
	MaxConnectionsPerHost int
	DialTimeout           time.Duration
	IdleConnTimeout       time.Duration
	GrpcDeadline          time.Duration
}
