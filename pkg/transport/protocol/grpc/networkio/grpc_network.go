package networkio

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"time"
)

type GrpcNetworkIO struct {
	recvMsgSize int
}

type SetOptions func(*Options)

func NewGrpcNetworkIO(setOpts ...SetOptions) *GrpcNetworkIO {
	opts := DefaultOptions()
	for _, setOpt := range setOpts {
		setOpt(&opts)
	}
	return &GrpcNetworkIO{
		recvMsgSize: opts.RecvMsgSize,
	}
}

func (g *GrpcNetworkIO) Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (*grpc.ClientConn, error) {
	return g.DialWithTimeout(config, fromPeerId, toEndPoint, 0)
}

func (g *GrpcNetworkIO) DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string, timeout time.Duration) (*grpc.ClientConn, error) {

	var opts []grpc.DialOption

	if config.EnableTLS {
		tlsConfig, err := netutil.GetClientTlsConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS config: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if g.recvMsgSize > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(g.recvMsgSize)))
	}
	return grpc.NewClient(toEndPoint, opts...)
}

func (g *GrpcNetworkIO) NewServer(config ibabuza.TLSConfig) (*grpc.Server, error) {
	var opts []grpc.ServerOption

	if config.EnableTLS {
		tlsConfig, err := netutil.GetServerTlsConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS config: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	if g.recvMsgSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(g.recvMsgSize))
	}
	return grpc.NewServer(opts...), nil
}

func (g *GrpcNetworkIO) Listen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}
