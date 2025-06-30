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


package proxynetwork

import (
	"net"
	"time"
)

type proxyDialConn struct {
	conn   net.Conn
	closed bool
}

func newProxyDialConn(conn net.Conn) *proxyDialConn {
	return &proxyDialConn{
		conn: conn,
	}
}

func (c *proxyDialConn) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}
func (c *proxyDialConn) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}
func (c *proxyDialConn) Close() error {
	if !c.closed {
		if err := c.conn.Close(); err != nil {
			return err
		}
		c.closed = true
	}
	return nil
}
func (c *proxyDialConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}
func (c *proxyDialConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
func (c *proxyDialConn) SetDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
func (c *proxyDialConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}
func (c *proxyDialConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}
