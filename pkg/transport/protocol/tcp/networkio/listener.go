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


package networkio

import (
	"context"
	"errors"
	"net"
)

type Addr struct {
	net     string
	address string
}

func (a *Addr) Network() string {
	return a.net
}
func (a *Addr) String() string {
	return a.address
}

type Listener struct {
	addr           net.Addr
	listenCh       chan net.Conn
	closeCtx       context.Context
	closeCtxCancel context.CancelFunc
}

func NewListener(address string) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		addr: &Addr{
			net:     "memory",
			address: address,
		},
		listenCh:       make(chan net.Conn, 1),
		closeCtx:       ctx,
		closeCtxCancel: cancel,
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	conn, ok := <-l.listenCh
	if !ok {
		// close
		return nil, errors.New("listener: close channel")
	}
	return conn, nil
}

func (l *Listener) Close() error {
	close(l.listenCh)
	l.closeCtxCancel()
	return nil
}

func (l *Listener) Addr() net.Addr {
	return l.addr
}
