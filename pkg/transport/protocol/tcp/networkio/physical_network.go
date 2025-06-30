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
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
	"time"
)

type TcpPhysicalIO struct {
}

func NewTcpPhysicalIO() *TcpPhysicalIO {
	return &TcpPhysicalIO{}
}

func (n *TcpPhysicalIO) Dial(cfg ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string) (net.Conn, error) {
	return netutil.TcpDial(cfg, toEndpoint)
}
func (n *TcpPhysicalIO) DialWithTimeout(cfg ibabuza.TLSConfig, fromProxyId uint64, toProxyInEndpoint string, timeout time.Duration) (net.Conn, error) {
	return netutil.TcpDialTimeout(cfg, toProxyInEndpoint, timeout)
}

func (n *TcpPhysicalIO) Listen(cfg ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	return netutil.TcpListen(cfg, endpoint)
}
