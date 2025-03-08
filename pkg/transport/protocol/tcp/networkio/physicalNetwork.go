package networkio

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
)

type TcpPhysicalIO struct {
}

func NewTcpPhysicalIO() *TcpPhysicalIO {
	return &TcpPhysicalIO{}
}

func (n *TcpPhysicalIO) Dial(cfg ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string) (net.Conn, error) {
	return netutil.TcpDial(cfg, toEndpoint)
}

func (n *TcpPhysicalIO) Listen(cfg ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	return netutil.TcpListen(cfg, endpoint)
}
