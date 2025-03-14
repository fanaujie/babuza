package grpc

import (
	"github.com/fanaujie/babuza/ibabuza"
	"google.golang.org/grpc"
	"net"
	"time"
)

type Dialer interface {
	Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (*grpc.ClientConn, error)
	DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string, timeout time.Duration) (*grpc.ClientConn, error)
}

type Listener interface {
	Listen(string) (net.Listener, error)
}

type Server interface {
	NewServer(config ibabuza.TLSConfig) (*grpc.Server, error)
}

type NetworkIO interface {
	Dialer
	Listener
	Server
}
