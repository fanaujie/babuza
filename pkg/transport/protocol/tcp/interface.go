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


package tcp

import (
	"github.com/fanaujie/babuza/ibabuza"
	"net"
	"time"
)

type Dialer interface {
	Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (net.Conn, error)
	DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string,
		timeout time.Duration) (net.Conn, error)
}

type Listener interface {
	Listen(ibabuza.TLSConfig, string) (net.Listener, error)
}

type NetworkIO interface {
	Dialer
	Listener
}
