package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool"
	"sync"
	"time"
)

type Tcp struct {
	network tcp.NetworkIO
	config  ibabuza.TransportConfig
	options connpool.Options
	logger  ibabuza.Logger
	pool    *connpool.ConnectionPool
	poolMu  sync.Mutex // Protects clientFactory modification
}

func defaultTcpOptions() connpool.Options {
	return connpool.Options{
		// Basic options
		WriteDeadline: time.Second * 5,
		ReadDeadline:  time.Second * 5,
		// Connection pool options
		MaxConnectionsPerHost: 5,                // Default: 5 connections per host
		DialTimeout:           30 * time.Second, // Default: 30 second connection timeout
		IdleTimeout:           5 * time.Minute,  // Default: 5 minute idle timeout
	}
}

type SetTcpOptions func(opt *connpool.Options)

func SetTcpOptsWithWriteDeadline(d time.Duration) SetTcpOptions {
	return func(opt *connpool.Options) {
		opt.WriteDeadline = d
	}
}

func SetTcpOptsWithReadDeadline(d time.Duration) SetTcpOptions {
	return func(opt *connpool.Options) {
		opt.ReadDeadline = d
	}
}

func SetTcpOptsWithMaxConnectionsPerHost(max int) SetTcpOptions {
	return func(opt *connpool.Options) {
		if max > 0 {
			opt.MaxConnectionsPerHost = max
		}
	}
}

func SetTcpOptsWithDialTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *connpool.Options) {
		if timeout > 0 {
			opt.DialTimeout = timeout
		}
	}
}

func SetTcpOptsWithIdleTimeout(timeout time.Duration) SetTcpOptions {
	return func(opt *connpool.Options) {
		if timeout > 0 {
			opt.IdleTimeout = timeout
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
	t.pool = connpool.NewConnectionPool(t.network, cfg.TLSConfig, t.options)
	return nil
}

func (t *Tcp) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return tcp.NewRaftMsgServer(t.config, t.options, t.network, handler, t.logger), nil
}

func (t *Tcp) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return tcp.NewRaftMsgClient(t.pool, resolver), nil
}

func (t *Tcp) Close() error {
	return t.pool.Close()
}
