package networkio

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
	"time"
)

type TcpPhysicalIO struct {
}

func NewTcpPhysicalIO() *TcpPhysicalIO {
	return &TcpPhysicalIO{}
}

func (n *TcpPhysicalIO) Dial(cfg ibabuza.TLSConfig, fromPeerId uint64, toEndpoint string) (net.Conn, error) {
	return netutil.TcpDial(cfg, toEndpoint)
}
func (n *TcpPhysicalIO) DialWithTimeout(cfg ibabuza.TLSConfig, fromProxyId uint64, toProxyInEndpoint string, timeout time.Duration) (net.Conn, error) {
	return netutil.TcpDialTimeout(cfg, toProxyInEndpoint, timeout)
}

func (n *TcpPhysicalIO) Listen(cfg ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	return netutil.TcpListen(cfg, endpoint)
}
