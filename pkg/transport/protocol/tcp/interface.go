package tcp

import (
	"github.com/fanaujie/babuza/ibabuza"
	"net"
)

type Dialer interface {
	Dial(ibabuza.TLSConfig, uint64, string) (net.Conn, error)
}

type Listener interface {
	Listen(ibabuza.TLSConfig, string) (net.Listener, error)
}

type NetworkIO interface {
	Dialer
	Listener
}
