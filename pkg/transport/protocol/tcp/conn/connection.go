package conn

import (
	"github.com/fanaujie/babuza/pkg/transport/protocol/connpool"
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

type FrameConnection struct {
	conn    net.Conn
	reader  *frame.Reader
	writer  *frame.Writer
	options connpool.Options
}

func NewConnection(conn net.Conn, options connpool.Options) *FrameConnection {
	return &FrameConnection{
		conn:    conn,
		reader:  frame.NewReader(conn),
		writer:  frame.NewWriter(conn),
		options: options,
	}
}

func (c *FrameConnection) Close() error {
	return c.conn.Close()
}

func (c *FrameConnection) SendFrame(msgType frame.MessageType, msg frame.Message) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteDeadline)); err != nil {
		return err
	}

	byteSlice := allocator.Acquire(frame.EncodeSize(msg.Size()))
	defer allocator.Release(byteSlice)
	return c.writer.Encode(byteSlice.Buffer, msgType, msg)
}

func (c *FrameConnection) ReadFrame(msgHandler func(msgType frame.MessageType, msgBuf []byte) error) error {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.ReadDeadline)); err != nil {
		return err
	}
	return c.reader.ReadFrame(msgHandler)
}
