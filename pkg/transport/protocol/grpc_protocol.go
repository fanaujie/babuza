package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/connpool"
	transGrpc "github.com/fanaujie/babuza/pkg/transport/protocol/grpc"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/networkio"
	"google.golang.org/grpc"
	"time"
)

type Grpc struct {
	network transGrpc.NetworkIO
	config  ibabuza.TransportConfig
	options transGrpc.Options
	logger  ibabuza.Logger
	pool    connpool.Pool[*grpc.ClientConn]
}

func defaultGrpcOptions() transGrpc.Options {
	return transGrpc.Options{
		MaxConnectionsPerHost: 5,
		DialTimeout:           3 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
		GrpcDeadline:          5 * time.Second,
	}
}

type SetGrpcOptions func(opt *transGrpc.Options)

func SetGrpcOptsWithMaxConnectionsPerHost(max int) SetGrpcOptions {
	return func(opt *transGrpc.Options) {
		if max > 0 {
			opt.MaxConnectionsPerHost = max
		}
	}
}

func SetGrpcOptsWithDialTimeout(timeout time.Duration) SetGrpcOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.DialTimeout = timeout
		}
	}
}

func SetGrpcOptsWithIdleTimeout(timeout time.Duration) SetGrpcOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.IdleConnTimeout = timeout
		}
	}
}

func SetGrpcOptsWithGrpcDeadline(timeout time.Duration) SetGrpcOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.GrpcDeadline = timeout
		}
	}
}

func NewGrpc(logger ibabuza.Logger, setOpts ...SetGrpcOptions) *Grpc {
	opts := defaultGrpcOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("grpc protocol: creating grpc protocol")
	return &Grpc{
		options: opts,
		logger:  logger,
	}
}

func (g *Grpc) Setup(cfg ibabuza.TransportConfig) error {
	g.config = cfg
	g.network = networkio.NewGrpcNetworkIO()
	g.pool = connpool.NewConnectionPool[*grpc.ClientConn](g, connpool.Config{
		MaxConnectionsPerHost: g.options.MaxConnectionsPerHost,
		IdleTimeout:           g.options.IdleConnTimeout,
		DialTimeout:           g.options.DialTimeout,
	})
	return nil
}

func (g *Grpc) CreateServer(handler ibabuza.RaftMessageHandler) (ibabuza.TransportServer, error) {
	return transGrpc.NewRaftMsgServer(g.config, g.network, handler, g.logger), nil
}

func (g *Grpc) CreateClient(resolver ibabuza.TransportResolver) (ibabuza.TransportClient, error) {
	return transGrpc.NewRaftMsgClient(g.pool, resolver, transGrpc.ClientConfig{
		GrpcDeadline: g.options.GrpcDeadline,
	}), nil
}

func (g *Grpc) Close() error {
	return g.pool.Close()
}

func (g *Grpc) Dial(address string) (*grpc.ClientConn, error) {
	grpcConn, err := g.network.DialWithTimeout(g.config.TLSConfig, 0, address, g.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return grpcConn, nil
}
