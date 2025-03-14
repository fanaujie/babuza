package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn"
	"time"
)

type Tcp struct {
	network tcp.NetworkIO
	config  ibabuza.TransportConfig
	options tcp.Options
	logger  ibabuza.Logger
	pool    connpool.Pool[*conn.FrameConnection]
}

func defaultTcpOptions() tcp.Options {
	return tcp.Options{
		WriteDeadline:         time.Second * 5,
		ReadDeadline:          time.Second * 5,
		MaxConnectionsPerHost: 5,
		DialTimeout:           3 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
	}
}

type SetTcpOptions func(opt *tcp.Options)

func SetTcpOptsWithWriteDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.WriteDeadline = d
	}
}

func SetTcpOptsWithReadDeadline(d time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		opt.ReadDeadline = d
	}
}

func SetTcpOptsWithMaxConnectionsPerHost(max int) SetTcpOptions {
	return func(opt *tcp.Options) {
		if max > 0 {
			opt.MaxConnectionsPerHost = max
		}
	}
}

func SetTcpOptsWithDialTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		if timeout > 0 {
			opt.DialTimeout = timeout
		}
	}
}

func SetTcpOptsWithIdleTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *tcp.Options) {
		if timeout > 0 {
			opt.IdleConnTimeout = timeout
		}
	}
}

func NewTcp(network tcp.NetworkIO, logger ibabuza.Logger, setOpts ...SetTcpOptions) *Tcp {
	opts := defaultTcpOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("tcp protocol: creating tcp protocol")
	return &Tcp{
		network: network,
		options: opts,
		logger:  logger,
	}
}

func (t *Tcp) Setup(cfg ibabuza.TransportConfig) error {
	t.config = cfg
	t.pool = connpool.NewConnectionPool[*conn.FrameConnection](t, connpool.Config{
		MaxConnectionsPerHost: t.options.MaxConnectionsPerHost,
		DialTimeout:           t.options.DialTimeout,
		IdleTimeout:           t.options.IdleConnTimeout,
	})
	return nil
}

func (t *Tcp) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return tcp.NewRaftMsgServer(t.config, tcp.ServerConfig{
		ReadDeadline:  t.options.ReadDeadline,
		WriteDeadline: t.options.WriteDeadline,
	}, t.network, handler, t.logger), nil
}

func (t *Tcp) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return tcp.NewRaftMsgClient(t.pool, resolver), nil
}

func (t *Tcp) Close() error {
	return t.pool.Close()
}

func (t *Tcp) Dial(address string) (*conn.FrameConnection, error) {
	netConn, err := t.network.DialWithTimeout(t.config.TLSConfig, 0, address, t.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return conn.NewConnection(netConn, conn.Config{
		ReadDeadline:  t.options.ReadDeadline,
		WriteDeadline: t.options.WriteDeadline,
	}), nil
}
