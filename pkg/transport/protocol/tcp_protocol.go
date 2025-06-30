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
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn"
	"time"
)

type Tcp struct {
	network tcp.NetworkIO
	config  ibabuza.TransportConfig
	options tcp.Options
	logger  ibabuza.Logger
	pool    connpool.Pool[*conn.FrameConnection]
}

func defaultTcpOptions() tcp.Options {
	return tcp.Options{
		WriteDeadline:         time.Second * 5,
		ReadDeadline:          time.Second * 5,
		MaxConnectionsPerHost: 5,
		DialTimeout:           3 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
	}
}

type SetTcpOptions func(opt *tcp.Options)

func SetTcpOptsWithWriteDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.WriteDeadline = d
	}
}

func SetTcpOptsWithReadDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.ReadDeadline = d
	}
}

func SetTcpOptsWithMaxConnectionsPerHost(max int) SetTcpOptions {
	return func(opt *tcp.Options) {
		if max > 0 {
			opt.MaxConnectionsPerHost = max
		}
	}
}

func SetTcpOptsWithDialTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		if timeout > 0 {
			opt.DialTimeout = timeout
		}
	}
}

func SetTcpOptsWithIdleTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		if timeout > 0 {
			opt.IdleConnTimeout = timeout
		}
	}
}

func NewTcp(network tcp.NetworkIO, logger ibabuza.Logger, setOpts ...SetTcpOptions) *Tcp {
	opts := defaultTcpOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("tcp protocol: creating tcp protocol")
	return &Tcp{
		network: network,
		options: opts,
		logger:  logger,
	}
}

func (t *Tcp) Setup(cfg ibabuza.TransportConfig) error {
	t.config = cfg
	t.pool = connpool.NewConnectionPool[*conn.FrameConnection](t, connpool.Config{
		MaxConnectionsPerHost: t.options.MaxConnectionsPerHost,
		IdleTimeout:           t.options.IdleConnTimeout,
	})
	return nil
}

func (t *Tcp) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return tcp.NewRaftMsgServer(t.config, tcp.ServerConfig{
		ReadDeadline:  t.options.ReadDeadline,
		WriteDeadline: t.options.WriteDeadline,
	}, t.network, handler, t.logger), nil
}

func (t *Tcp) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return tcp.NewRaftMsgClient(t.pool, resolver), nil
}

func (t *Tcp) Close() error {
	return t.pool.Close()
}

func (t *Tcp) Dial(address string) (*conn.FrameConnection, error) {
	netConn, err := t.network.DialWithTimeout(t.config.TLSConfig, t.config.LocalNodeID, address, t.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return conn.NewConnection(netConn, conn.Config{
		ReadDeadline:  t.options.ReadDeadline,
		WriteDeadline: t.options.WriteDeadline,
	}), nil
}
