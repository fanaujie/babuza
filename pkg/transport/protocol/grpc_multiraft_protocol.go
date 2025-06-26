package protocol

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/connpool"
	transGrpc "github.com/fanaujie/babuza/pkg/transport/protocol/grpc"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/networkio"
	"google.golang.org/grpc"
	"time"
)

type GrpcMultiRaft struct {
	network transGrpc.NetworkIO
	config  ibabuza.TransportConfig
	options transGrpc.Options
	logger  ibabuza.Logger
	pool    connpool.Pool[*grpc.ClientConn]
}

func defaultGrpcMultiRaftOptions() transGrpc.Options {
	return transGrpc.Options{
		MaxConnectionsPerHost: 5,
		DialTimeout:           3 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
		GrpcDeadline:          2 * time.Second,
		// RecvMsgMaxSize default is 0, which means using gRPC default message size limit
		RecvMsgMaxSize: 0,
	}
}

type SetGrpcMultiRaftOptions func(opt *transGrpc.Options)

func SetGrpcMultiRaftOptsWithMaxConnectionsPerHost(max int) SetGrpcMultiRaftOptions {
	return func(opt *transGrpc.Options) {
		if max > 0 {
			opt.MaxConnectionsPerHost = max
		}
	}
}

func SetGrpcMultiRaftOptsWithDialTimeout(timeout time.Duration) SetGrpcMultiRaftOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.DialTimeout = timeout
		}
	}
}

func SetGrpcMultiRaftOptsWithIdleTimeout(timeout time.Duration) SetGrpcMultiRaftOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.IdleConnTimeout = timeout
		}
	}
}

func SetGrpcMultiRaftOptsWithGrpcDeadline(timeout time.Duration) SetGrpcMultiRaftOptions {
	return func(opt *transGrpc.Options) {
		if timeout > 0 {
			opt.GrpcDeadline = timeout
		}
	}
}

func SetGrpcMultiRaftOptsWithRecvMsgMaxSize(size int) SetGrpcMultiRaftOptions {
	return func(opt *transGrpc.Options) {
		if size > 0 {
			opt.RecvMsgMaxSize = size
		}
	}
}

func NewGrpcMultiRaft(logger ibabuza.Logger, setOpts ...SetGrpcMultiRaftOptions) *GrpcMultiRaft {
	opts := defaultGrpcMultiRaftOptions()
	for _, s := range setOpts {
		s(&opts)
	}
	logger.Infof("grpc protocol: creating grpc multi-raft protocol")
	return &GrpcMultiRaft{
		network: networkio.NewGrpcNetworkIO(networkio.SetOptionsRecvMsgSize(opts.RecvMsgMaxSize)),
		options: opts,
		logger:  logger,
	}
}

func (g *GrpcMultiRaft) Setup(cfg ibabuza.TransportConfig) error {
	g.config = cfg
	g.pool = connpool.NewConnectionPool[*grpc.ClientConn](g, connpool.Config{
		MaxConnectionsPerHost: g.options.MaxConnectionsPerHost,
		IdleTimeout:           g.options.IdleConnTimeout,
	})
	return nil
}

func (g *GrpcMultiRaft) CreateServer(handler ibabuza.MultiRaftStoreHandler) (ibabuza.TransportServer, error) {
	return transGrpc.NewMultiRaftMsgServer(g.config, g.network, handler, g.logger), nil
}

func (g *GrpcMultiRaft) CreateClient(resolver ibabuza.MultiRaftTransportResolver) (ibabuza.MultiRaftTransportClient, error) {
	return transGrpc.NewMultiRaftMsgClient(g.pool, resolver, transGrpc.ClientConfig{
		GrpcDeadline: g.options.GrpcDeadline,
	}, g.logger), nil
}

func (g *GrpcMultiRaft) Close() error {
	return g.pool.Close()
}

func (g *GrpcMultiRaft) Dial(address string) (*grpc.ClientConn, error) {
	grpcConn, err := g.network.DialWithTimeout(g.config.TLSConfig, g.config.LocalNodeID, address, g.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return grpcConn, nil
}
