package connpool

import (
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn     net.Conn
	reader   *frame.Reader
	writer   *frame.Writer
	addr     string
	inUse    bool
	lastUsed time.Time
	cfg      Options
	pool     *ConnectionPool
	mu       sync.Mutex // Protects connection state
}

func (c *Connection) SendFrame(msgType frame.MessageType, msg frame.Message) (err error) {
	defer func() {
		if err != nil {
			c.RemoveFromPool()
		} else {
			c.ReturnToPool()
		}
	}()
	if err = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteDeadline)); err != nil {
		return err
	}
	byteSlice := allocator.Acquire(frame.EncodeSize(msg.Size()))
	defer allocator.Release(byteSlice)
	err = c.writer.Encode(byteSlice.Buffer, msgType, msg)
	return err
}

func (c *Connection) ReadFrame(msgHandler func(msgType frame.MessageType, msgBuf []byte) error) (err error) {
	defer func() {
		if err != nil {
			c.RemoveFromPool()
		} else {
			c.ReturnToPool()
		}
	}()
	if err = c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadDeadline)); err != nil {
		return err
	}
	err = c.reader.ReadFrame(msgHandler)
	return err
}

func (c *Connection) ReturnToPool() {
	if c.pool != nil {
		c.pool.ReturnConnection(c)
	}
}

func (c *Connection) RemoveFromPool() {
	if c.pool != nil {
		c.pool.RemoveConnection(c)
	}
}
