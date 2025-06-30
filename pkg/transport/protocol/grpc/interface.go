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


package grpc

import (
	"github.com/fanaujie/babuza/ibabuza"
	"google.golang.org/grpc"
	"net"
	"time"
)

type Dialer interface {
	Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (*grpc.ClientConn, error)
	DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string, timeout time.Duration) (*grpc.ClientConn, error)
}

type Listener interface {
	Listen(string) (net.Listener, error)
}

type Server interface {
	NewServer(config ibabuza.TLSConfig) (*grpc.Server, error)
}

type NetworkIO interface {
	Dialer
	Listener
	Server
}
