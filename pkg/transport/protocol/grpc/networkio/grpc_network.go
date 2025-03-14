package networkio

import (
	"context"
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
}

func NewGrpcNetworkIO() *GrpcNetworkIO {
	return &GrpcNetworkIO{}
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

	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		opts = append(opts, grpc.WithBlock())
	}
	conn, err := grpc.DialContext(ctx, toEndPoint, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
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
	return grpc.NewServer(opts...), nil
}

func (g *GrpcNetworkIO) Listen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}
