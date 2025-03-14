package proxynetwork

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

var testTLSConfig = []ibabuza.TLSConfig{
	{},
	{
		EnableTLS: true,
		MutualTLS: false,
		TLSCert:   "../../../../../../test/fixtures/server.pem",
		TLSKey:    "../../../../../../test/fixtures/server-key.pem",
		TLSRootCA: "../../../../../../test/fixtures/root.pem",
	},
	{
		EnableTLS: true,
		MutualTLS: true,
		TLSCert:   "../../../../../../test/fixtures/server.pem",
		TLSKey:    "../../../../../../test/fixtures/server-key.pem",
		TLSRootCA: "../../../../../../test/fixtures/root.pem",
	},
}

func TestProxy_Enable(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "localhost:14200",
		OutAddr: "localhost:24200",
	}

	for _, tc := range testTLSConfig {
		func() {
			pc.TLSConfig = tc
			p := NewProxy(pc)
			assert.Nil(t, p.Enable())
			defer func() {
				p.listener.Close()
				p.wg.Wait()
			}()
			assert.Equal(t, true, p.enable)
			assert.NotNil(t, p.listener)
		}()
	}

}

type peerEchoServer struct {
	endpoint    string
	connCount   int
	receiveData map[int][]byte
	listener    net.Listener
	tlsConfig   ibabuza.TLSConfig
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func newPeerEchoServer(tlsConfig ibabuza.TLSConfig, endpoint string) *peerEchoServer {
	return &peerEchoServer{
		endpoint:    endpoint,
		receiveData: make(map[int][]byte),
		tlsConfig:   tlsConfig,
	}
}

func (p *peerEchoServer) start() error {
	var err error
	p.listener, err = netutil.TcpListen(p.tlsConfig, p.endpoint)
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
				return
			} else {
				buf := make([]byte, 1024*1024)
				p.wg.Add(1)
				p.connCount++
				connId := p.connCount
				go func() {
					defer p.wg.Done()
					defer conn.Close()
					for {
						if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
							return
						}
						n, rErr := conn.Read(buf)
						if rErr != nil {
							return
						}
						p.mu.Lock()
						p.receiveData[connId] = append(p.receiveData[connId], buf[:n]...)
						p.mu.Unlock()
						if err = conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
							return
						}
						_, rErr = conn.Write(buf[:n])
						if rErr != nil {
							return
						}
					}

				}()
			}
		}
	}()
	return nil
}

func (p *peerEchoServer) stop() {
	p.listener.Close()
	p.wg.Wait()
}

func TestProxy_Dial(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "127.0.0.1:14200",
		OutAddr: "127.0.0.1:24200",
	}
	t.Run("failure: proxy is disable", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			pc.TLSConfig = tc
			p := NewProxy(pc)
			assert.NotNil(t, p)
			_, err := netutil.TcpDial(tc, pc.InAddr)
			assert.Error(t, err)
		}
	})
	t.Run("failure: downstream raft peer does not start to listen", func(t *testing.T) {
		for index, tc := range testTLSConfig {
			pc.TLSConfig = tc
			func() {
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer p.Disable()
				conn, err := netutil.TcpDial(tc, pc.InAddr)
				if index == 0 {
					assert.Nil(t, err)
					assert.Nil(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
					buf := make([]byte, 8)
					_, err = conn.Read(buf)
					assert.Error(t, err)
				} else {
					assert.Error(t, err)
				}
			}()
		}
	})
	t.Run("success", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			pc.TLSConfig = tc
			func() {
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer p.Disable()
				server := newPeerEchoServer(pc.TLSConfig, pc.OutAddr)
				assert.Nil(t, server.start())
				defer server.stop()
				conn, err := netutil.TcpDial(tc, pc.InAddr)
				assert.Nil(t, err)
				wData := []byte{1, 2, 3, 4}
				rData := make([]byte, len(wData))
				_, err = conn.Write(wData)
				assert.Nil(t, err)
				_, err = conn.Read(rData)
				assert.Nil(t, err)
				assert.Equal(t, wData, server.receiveData[1])
				assert.Equal(t, wData, rData)
			}()
		}
	})
	t.Run("concurrency", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			pc.TLSConfig = tc
			func() {
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer func() {
					p.listener.Close()
					p.wg.Wait()
				}()
				server := newPeerEchoServer(pc.TLSConfig, pc.OutAddr)
				assert.Nil(t, server.start())
				defer server.stop()

				clientCount := 3
				doneCh := make(chan []byte, clientCount)
				wData := []byte{1, 2, 3, 4}
				for i := 0; i < clientCount; i++ {
					go func() {
						conn, err := netutil.TcpDial(tc, pc.InAddr)
						assert.Nil(t, err)
						rData := make([]byte, len(wData))
						defer func() {
							conn.Close()
							doneCh <- rData
						}()
						_, err = conn.Write(wData)
						assert.Nil(t, err)
						_, err = conn.Read(rData)
						assert.Nil(t, err)
					}()
				}
				for i := 0; i < clientCount; i++ {
					assert.Equal(t, wData, <-doneCh)
				}
			}()
		}
	})
}

func TestProxy_Disable(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "127.0.0.1:14200",
		OutAddr: "127.0.0.1:24200",
	}
	for _, tc := range testTLSConfig {
		pc.TLSConfig = tc
		func() {
			p := NewProxy(pc)
			assert.Nil(t, p.Enable())
			defer p.Disable()
			server := newPeerEchoServer(tc, pc.OutAddr)
			assert.Nil(t, server.start())
			defer server.stop()
			dialerCounts := 3
			for i := 0; i < dialerCounts; i++ {
				conn, err := netutil.TcpDial(tc, pc.InAddr)
				assert.Nil(t, err)
				assert.NotNil(t, conn)
				wData := []byte{1, 2, 3, 4}
				rData := make([]byte, len(wData))
				_, err = conn.Write(wData)
				assert.Nil(t, err)
				_, err = conn.Read(rData)
				assert.Nil(t, err)
				assert.Equal(t, wData, rData)
			}
			assert.Nil(t, p.Disable())
			assert.Equal(t, false, p.enable)
			assert.Nil(t, p.Enable())
			conn, err := netutil.TcpDial(tc, pc.InAddr)
			assert.Nil(t, err)
			wData := []byte{1, 2, 3, 4}
			rData := make([]byte, len(wData))
			_, err = conn.Write(wData)
			assert.Nil(t, err)
			_, err = conn.Read(rData)
			assert.Nil(t, err)
			assert.Equal(t, wData, server.receiveData[4])
			assert.Equal(t, wData, rData)
		}()
	}
}
