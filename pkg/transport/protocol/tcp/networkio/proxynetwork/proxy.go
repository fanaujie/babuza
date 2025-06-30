package proxynetwork

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"net"
	"sync"
)

const (
	allocatedBufSize = 32 * 1024
)

type Proxy struct {
	config      ibabuza.ProxyConfig
	listener    net.Listener
	monitorConn []net.Conn
	enable      bool
	disableCh   chan struct{}
	wg          sync.WaitGroup
	mu          sync.Mutex
}

func NewProxy(config ibabuza.ProxyConfig) *Proxy {
	return &Proxy{
		config: config,
	}
}

func (p *Proxy) Enable() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.enable == false {
		p.disableCh = make(chan struct{})
		var err error
		p.listener, err = netutil.TcpListen(p.config.InListenTLSConfig, p.config.InAddr)
		if err != nil {
			return err
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				// TODO: optimize Accept
				conn, lErr := p.listener.Accept()
				if lErr != nil {
					select {
					case <-p.disableCh:
						return
					default:
						return
					}
				} else {
					remoteConn, cErr := netutil.TcpDial(p.config.OutDialTLSConfig, p.config.OutAddr)
					if cErr != nil {
						conn.Close()
						continue
					}
					p.monitorConn = append(p.monitorConn, remoteConn)
					p.wg.Add(2)
					//directional copy
					go p.handleConn(remoteConn, conn)
					go p.handleConn(conn, remoteConn)
				}
			}
		}()
		p.enable = true
	}
	return nil
}

func (p *Proxy) Disable() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.enable {
		p.enable = false
		close(p.disableCh)
		p.listener.Close()
		for _, conn := range p.monitorConn {
			conn.Close()
		}
		p.wg.Wait()
	}
	return nil
}

func (p *Proxy) handleConn(wConn, rConn net.Conn) {
	byteSlice := allocator.Acquire(allocatedBufSize)
	defer func() {
		allocator.Release(byteSlice)
		wConn.Close()
		rConn.Close()
		p.wg.Done()
	}()

	for {
		n, err := rConn.Read(byteSlice.Buffer)
		if err != nil {
			return
		}
		_, err = wConn.Write(byteSlice.Buffer[:n])
		if err != nil {
			return
		}
	}
}

func (p *Proxy) IsEnable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enable
}
