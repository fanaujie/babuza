package tcp

import (
	"github.com/fanaujie/babuza/ibabuza"
	"net"
)

type Dialer interface {
	Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (net.Conn, error)
}

type Listener interface {
	Listen(ibabuza.TLSConfig, string) (net.Listener, error)
}

type NetworkIO interface {
	Dialer
	Listener
}
