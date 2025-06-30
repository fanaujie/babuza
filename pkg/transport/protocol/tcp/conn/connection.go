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


package conn

import (
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
)

type Dialer interface {
	Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string) (net.Conn, error)
	DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string, timeout time.Duration) (net.Conn, error)
}

type Config struct {
	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

type FrameConnection struct {
	conn   net.Conn
	reader *frame.Reader
	writer *frame.Writer
	config Config
}

func NewConnection(conn net.Conn, config Config) *FrameConnection {
	return &FrameConnection{
		conn:   conn,
		reader: frame.NewReader(conn),
		writer: frame.NewWriter(conn),
		config: config,
	}
}

func (c *FrameConnection) Close() error {
	return c.conn.Close()
}

func (c *FrameConnection) SendFrame(msgType frame.MessageType, msg frame.Message) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteDeadline)); err != nil {
		return err
	}

	byteSlice := allocator.Acquire(frame.EncodeSize(msg.Size()))
	defer allocator.Release(byteSlice)
	return c.writer.Encode(byteSlice.Buffer, msgType, msg)
}

func (c *FrameConnection) ReadFrame(msgHandler func(msgType frame.MessageType, msgBuf []byte) error) error {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.config.ReadDeadline)); err != nil {
		return err
	}
	return c.reader.ReadFrame(msgHandler)
}
