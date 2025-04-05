package http

import (
	"context"
	"crypto/tls"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
	"time"
)

type Connection struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *Connection) Read(b []byte) (n int, err error) {
	if err = c.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}
func (c *Connection) Write(b []byte) (n int, err error) {
	if err = c.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func dialContext(cfg ibabuza.TLSConfig, options ServerConfig) (func(ctx context.Context, network string, addr string) (net.Conn, error), error) {
	tlsCfg, err := netutil.GetClientTlsConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return func(ctx context.Context, network string, addr string) (net.Conn, error) {
			conn, dErr := net.Dial(network, addr)
			if dErr != nil {
				return nil, dErr
			}
			return &Connection{
				Conn:         conn,
				readTimeout:  options.ReadDeadline,
				writeTimeout: options.WriteDeadline,
			}, nil
		}, nil
	} else {
		return func(ctx context.Context, network string, addr string) (net.Conn, error) {
			conn, dErr := tls.Dial(network, addr, tlsCfg)
			if dErr != nil {
				return nil, dErr
			}
			return &Connection{
				Conn:         conn,
				readTimeout:  options.ReadDeadline,
				writeTimeout: options.WriteDeadline,
			}, nil
		}, nil
	}
}
