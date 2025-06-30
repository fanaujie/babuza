// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package transport

import (
	"time"
)

type Options struct {
	DialTimeout                    time.Duration
	PeerLimiterMaxBatchMessageSize int64
	PeerQueueSize                  int64
	PeerSnapshotChunkSize          int64
	HeartbeatBufferSize            int
}

func DefaultOptions() Options {
	return Options{
		DialTimeout:                    time.Second * 3,
		PeerLimiterMaxBatchMessageSize: 3 * 1024 * 1024,
		PeerQueueSize:                  256,
		PeerSnapshotChunkSize:          3 * 1024 * 1024,
		HeartbeatBufferSize:            256,
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

func SetTransportOptionsWithHeartbeatBufferSize(d int) SetTransportOptions {
	return func(opt *Options) {
		opt.HeartbeatBufferSize = d
	}
}

func NewOptions(setters ...SetTransportOptions) Options {
	opts := DefaultOptions()
	for _, setter := range setters {
		setter(&opts)
	}
	return opts
}
