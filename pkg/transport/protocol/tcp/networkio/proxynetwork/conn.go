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
