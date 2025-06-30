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
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"net"
	"sync"
	"time"
)

type TcpMemoryIO struct {
	listener map[string]*Listener
	mu       sync.Mutex
}

func NewTcpMemoryIO() *TcpMemoryIO {
	return &TcpMemoryIO{
		listener: make(map[string]*Listener),
	}
}

func (n *TcpMemoryIO) Dial(cfg ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	l, ok := n.listener[toEndpoint]
	if !ok {
		return nil, fmt.Errorf("memory network: peer (id=%d address=%s) does not start to listen", fromPeerId, toEndpoint)
	}
	select {
	case <-l.closeCtx.Done():
		return nil, fmt.Errorf("memory network: peer (id=%d address=%s) already closed", fromPeerId, toEndpoint)
	default:
	}
	w, r := net.Pipe()
	l.listenCh <- r
	return w, nil
}
func (n *TcpMemoryIO) DialWithTimeout(cfg ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string, timeout time.Duration) (net.Conn, error) {
	return n.Dial(cfg, fromPeerId, toEndpoint)
}

func (n *TcpMemoryIO) Listen(cfg ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.listener[endpoint]
	if ok {
		select {
		case <-s.closeCtx.Done():
			delete(n.listener, endpoint)
		default:
			return nil, fmt.Errorf("memory network: peer (address=%s) has started to listen", endpoint)
		}
	}
	l := NewListener(endpoint)
	n.listener[endpoint] = l
	return l, nil
}
