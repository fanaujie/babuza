package transport

import (
	"time"
)

type Options struct {
	DialTimeout                    time.Duration
	PeerLimiterMaxBatchMessageSize int64
	PeerQueueSize                  int64
	PeerSnapshotChunkSize          int64
	PeerQueuePoolSize              int
}

func DefaultOptions() Options {
	return Options{
		DialTimeout:                    time.Second * 3,
		PeerLimiterMaxBatchMessageSize: 3 * 1024 * 1024,
		PeerQueueSize:                  256,
		PeerSnapshotChunkSize:          3 * 1024 * 1024,
		PeerQueuePoolSize:              8,
	}
}

type SetTransportOptions func(opt *Options)

func SetTransportOptionsWithDialTimeout(d time.Duration) SetTransportOptions {
	return func(opt *Options) {
		opt.DialTimeout = d
	}
}

func SetTransportOptionsWithPeerLimiterMaxBatchMessageSize(d int64) SetTransportOptions {
	return func(opt *Options) {
		opt.PeerLimiterMaxBatchMessageSize = d
	}
}

func SetTransportOptionsWithPeerQueueSize(d int64) SetTransportOptions {
	return func(opt *Options) {
		opt.PeerQueueSize = d
	}
}

func SetTransportOptionsWithPeerSnapshotChunkSize(d int64) SetTransportOptions {
	return func(opt *Options) {
		opt.PeerSnapshotChunkSize = d
	}
}

func SetTransportOptionsWithPeerQueuePoolSize(d int) SetTransportOptions {
	return func(opt *Options) {
		opt.PeerQueuePoolSize = d
	}
}

func NewOptions(setters ...SetTransportOptions) Options {
	opts := DefaultOptions()
	for _, setter := range setters {
		setter(&opts)
	}
	return opts
}
