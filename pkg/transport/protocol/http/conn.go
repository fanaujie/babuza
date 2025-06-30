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


package http

import (
	"context"
	"crypto/tls"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
	"time"
)

type Connection struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *Connection) Read(b []byte) (n int, err error) {
	if err = c.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}
func (c *Connection) Write(b []byte) (n int, err error) {
	if err = c.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func dialContext(cfg ibabuza.TLSConfig, options ServerConfig) (func(ctx context.Context, network string, addr string) (net.Conn, error), error) {
	tlsCfg, err := netutil.GetClientTlsConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return func(ctx context.Context, network string, addr string) (net.Conn, error) {
			conn, dErr := net.Dial(network, addr)
			if dErr != nil {
				return nil, dErr
			}
			return &Connection{
				Conn:         conn,
				readTimeout:  options.ReadDeadline,
				writeTimeout: options.WriteDeadline,
			}, nil
		}, nil
	} else {
		return func(ctx context.Context, network string, addr string) (net.Conn, error) {
			conn, dErr := tls.Dial(network, addr, tlsCfg)
			if dErr != nil {
				return nil, dErr
			}
			return &Connection{
				Conn:         conn,
				readTimeout:  options.ReadDeadline,
				writeTimeout: options.WriteDeadline,
			}, nil
		}, nil
	}
}
