// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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

type testCase struct {
	clientTls ibabuza.TLSConfig
	serverTLS ibabuza.TLSConfig
}

var testTLSConfig = []testCase{
	{},
	{
		clientTls: ibabuza.TLSConfig{
			EnableTLS: true,
			MutualTLS: false,
			TLSCert:   "../../../../../../test/fixtures/client.pem",
			TLSKey:    "../../../../../../test/fixtures/client-key.pem",
			TLSRootCA: "../../../../../../test/fixtures/root.pem",
		},
		serverTLS: ibabuza.TLSConfig{
			EnableTLS: true,
			MutualTLS: false,
			TLSCert:   "../../../../../../test/fixtures/server.pem",
			TLSKey:    "../../../../../../test/fixtures/server-key.pem",
			TLSRootCA: "../../../../../../test/fixtures/root.pem",
		},
	},
	{
		clientTls: ibabuza.TLSConfig{
			EnableTLS: true,
			MutualTLS: true,
			TLSCert:   "../../../../../../test/fixtures/client.pem",
			TLSKey:    "../../../../../../test/fixtures/client-key.pem",
			TLSRootCA: "../../../../../../test/fixtures/root.pem",
		},
		serverTLS: ibabuza.TLSConfig{
			EnableTLS: true,
			MutualTLS: true,
			TLSCert:   "../../../../../../test/fixtures/server.pem",
			TLSKey:    "../../../../../../test/fixtures/server-key.pem",
			TLSRootCA: "../../../../../../test/fixtures/root.pem",
		},
	},
}

func TestProxy_Enable(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "localhost:14200",
		OutAddr: "localhost:24200",
	}

	for _, tc := range testTLSConfig {
		func() {
			pc.InListenTLSConfig = tc.serverTLS
			pc.OutDialTLSConfig = tc.clientTls
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
			pc.InListenTLSConfig = tc.serverTLS
			pc.OutDialTLSConfig = tc.clientTls
			p := NewProxy(pc)
			assert.NotNil(t, p)
			_, err := netutil.TcpDial(tc.clientTls, pc.InAddr)
			assert.Error(t, err)
		}
	})
	t.Run("failure: downstream raft peer does not start to listen", func(t *testing.T) {
		for index, tc := range testTLSConfig {
			func(tlsCase testCase) {
				pc.InListenTLSConfig = tlsCase.serverTLS
				pc.OutDialTLSConfig = tlsCase.clientTls
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer p.Disable()
				conn, err := netutil.TcpDial(tlsCase.clientTls, pc.InAddr)
				if index == 0 {
					assert.Nil(t, err)
					assert.Nil(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
					buf := make([]byte, 8)
					_, err = conn.Read(buf)
					assert.Error(t, err)
				} else {
					assert.Error(t, err)
				}
			}(tc)
		}
	})
	t.Run("success", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			func(tlsCase testCase) {
				pc.InListenTLSConfig = tlsCase.serverTLS
				pc.OutDialTLSConfig = tlsCase.clientTls
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer p.Disable()
				server := newPeerEchoServer(tlsCase.serverTLS, pc.OutAddr)
				assert.Nil(t, server.start())
				defer server.stop()
				conn, err := netutil.TcpDial(tlsCase.clientTls, pc.InAddr)
				assert.Nil(t, err)
				wData := []byte{1, 2, 3, 4}
				rData := make([]byte, len(wData))
				_, err = conn.Write(wData)
				assert.Nil(t, err)
				_, err = conn.Read(rData)
				assert.Nil(t, err)
				assert.Equal(t, wData, server.receiveData[1])
				assert.Equal(t, wData, rData)
			}(tc)
		}
	})
	t.Run("concurrency", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			func(tlsCase testCase) {
				pc.InListenTLSConfig = tlsCase.serverTLS
				pc.OutDialTLSConfig = tlsCase.clientTls
				p := NewProxy(pc)
				assert.Nil(t, p.Enable())
				defer func() {
					p.listener.Close()
					p.wg.Wait()
				}()
				server := newPeerEchoServer(tlsCase.serverTLS, pc.OutAddr)
				assert.Nil(t, server.start())
				defer server.stop()

				clientCount := 3
				doneCh := make(chan []byte, clientCount)
				wData := []byte{1, 2, 3, 4}
				for i := 0; i < clientCount; i++ {
					go func() {
						conn, err := netutil.TcpDial(tlsCase.clientTls, pc.InAddr)
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
			}(tc)
		}
	})
}

func TestProxy_Disable(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "127.0.0.1:14200",
		OutAddr: "127.0.0.1:24200",
	}
	for _, tc := range testTLSConfig {
		func(tlsCase testCase) {
			pc.InListenTLSConfig = tlsCase.serverTLS
			pc.OutDialTLSConfig = tlsCase.clientTls
			p := NewProxy(pc)
			assert.Nil(t, p.Enable())
			defer p.Disable()
			server := newPeerEchoServer(tlsCase.serverTLS, pc.OutAddr)
			assert.Nil(t, server.start())
			defer server.stop()
			dialerCounts := 3
			for i := 0; i < dialerCounts; i++ {
				conn, err := netutil.TcpDial(tlsCase.clientTls, pc.InAddr)
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
			conn, err := netutil.TcpDial(tlsCase.clientTls, pc.InAddr)
			assert.Nil(t, err)
			wData := []byte{1, 2, 3, 4}
			rData := make([]byte, len(wData))
			_, err = conn.Write(wData)
			assert.Nil(t, err)
			_, err = conn.Read(rData)
			assert.Nil(t, err)
			assert.Equal(t, wData, server.receiveData[4])
			assert.Equal(t, wData, rData)
		}(tc)
	}
}

func TestProxy_FaultInjection(t *testing.T) {
	pc := ibabuza.ProxyConfig{
		InAddr:  "127.0.0.1:14201",
		OutAddr: "127.0.0.1:24201",
	}

	t.Run("SetFault and ClearFault", func(t *testing.T) {
		p := NewProxy(pc)
		assert.False(t, p.IsFaultEnabled())

		p.SetFault(FaultConfig{
			LossRate: 0.5,
			DelayMin: 10 * time.Millisecond,
			DelayMax: 20 * time.Millisecond,
		})
		assert.True(t, p.IsFaultEnabled())

		p.ClearFault()
		assert.False(t, p.IsFaultEnabled())
	})

	t.Run("FaultInjection with delay", func(t *testing.T) {
		p := NewProxy(pc)
		// Set fault before enabling
		p.SetFault(FaultConfig{
			DelayMin: 50 * time.Millisecond,
			DelayMax: 50 * time.Millisecond,
		})
		assert.Nil(t, p.Enable())
		defer p.Disable()

		server := newPeerEchoServer(ibabuza.TLSConfig{}, pc.OutAddr)
		assert.Nil(t, server.start())
		defer server.stop()

		conn, err := netutil.TcpDial(ibabuza.TLSConfig{}, pc.InAddr)
		assert.Nil(t, err)
		defer conn.Close()

		wData := []byte{1, 2, 3, 4}
		rData := make([]byte, len(wData))

		start := time.Now()
		_, err = conn.Write(wData)
		assert.Nil(t, err)
		_, err = conn.Read(rData)
		assert.Nil(t, err)
		elapsed := time.Since(start)

		assert.Equal(t, wData, rData)
		// Should have at least the configured delay
		assert.GreaterOrEqual(t, elapsed, 45*time.Millisecond)
	})

	t.Run("ClearFault restores normal operation", func(t *testing.T) {
		p := NewProxy(ibabuza.ProxyConfig{
			InAddr:  "127.0.0.1:14202",
			OutAddr: "127.0.0.1:24202",
		})
		// First set a long delay
		p.SetFault(FaultConfig{
			DelayMin: 500 * time.Millisecond,
			DelayMax: 500 * time.Millisecond,
		})
		// Then clear it
		p.ClearFault()
		assert.Nil(t, p.Enable())
		defer p.Disable()

		server := newPeerEchoServer(ibabuza.TLSConfig{}, "127.0.0.1:24202")
		assert.Nil(t, server.start())
		defer server.stop()

		conn, err := netutil.TcpDial(ibabuza.TLSConfig{}, "127.0.0.1:14202")
		assert.Nil(t, err)
		defer conn.Close()

		wData := []byte{1, 2, 3, 4}
		rData := make([]byte, len(wData))

		start := time.Now()
		_, err = conn.Write(wData)
		assert.Nil(t, err)
		_, err = conn.Read(rData)
		assert.Nil(t, err)
		elapsed := time.Since(start)

		assert.Equal(t, wData, rData)
		// Should be fast (no delay)
		assert.Less(t, elapsed, 100*time.Millisecond)
	})
}
