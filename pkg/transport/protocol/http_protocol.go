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

package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	raftHttp "github.com/fanaujie/babuza/pkg/transport/protocol/http"
	"net/http"
	"time"
)

type Http struct {
	config  ibabuza.TransportConfig
	options raftHttp.ServerConfig
	logger  ibabuza.Logger
	client  *http.Client
}

func DefaultHttpOptions() raftHttp.ServerConfig {
	return raftHttp.ServerConfig{
		WriteDeadline:     time.Second * 5,
		ReadDeadline:      time.Second * 5,
		ShutdownTimeout:   time.Second * 5,
		StreamIdleTimeout: time.Second * 30,
	}
}

type SetHttpOptions func(opt *raftHttp.ServerConfig)

func SetHttpOptsWithWriteDeadline(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.ServerConfig) {
		opt.WriteDeadline = d
	}
}

func SetHttpOptsWithReadDeadline(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.ServerConfig) {
		opt.ReadDeadline = d
	}
}

func SetHttpOptsWithShutdownTimeout(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.ServerConfig) {
		opt.ShutdownTimeout = d
	}
}

func SetHttpOptsWithMessageStreamEnabled(enabled bool) SetHttpOptions {
	return func(opt *raftHttp.ServerConfig) {
		opt.MessageStreamEnabled = enabled
	}
}

func SetHttpOptsWithStreamIdleTimeout(d time.Duration) SetHttpOptions {
	return func(opt *raftHttp.ServerConfig) {
		opt.StreamIdleTimeout = d
	}
}

func NewHttp(logger ibabuza.Logger, setOpts ...SetHttpOptions) *Http {
	opts := DefaultHttpOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("http protocol: creating http protocol")
	return &Http{
		options: opts,
		logger:  logger,
	}
}

func (h *Http) Setup(cfg ibabuza.TransportConfig) error {
	h.config = cfg
	client, err := raftHttp.NewClient(h.config.TLSConfig, h.options)
	if err != nil {
		return err
	}
	h.client = client
	return nil
}

func (h *Http) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return raftHttp.NewRaftMsgServer(h.config, h.options, handler, h.logger), nil
}

func (h *Http) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return raftHttp.NewRaftMsgClient(h.client, resolver, h.config.TLSConfig.EnableTLS, h.options), nil
}

func (h *Http) Close() error {
	return nil
}
