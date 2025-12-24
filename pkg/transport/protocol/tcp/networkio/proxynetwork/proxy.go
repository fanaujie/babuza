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
	"net"
	"sync"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
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

	faultConfig  *FaultConfig // nil means no fault
	faultWriters []*faultWriter
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
					// Wrap backend connection with faultWriter
					fw := newFaultWriter(remoteConn)
					p.mu.Lock()
					p.monitorConn = append(p.monitorConn, remoteConn)
					p.faultWriters = append(p.faultWriters, fw)
					// Apply current fault config to new connection
					if p.faultConfig != nil {
						fw.SetFault(*p.faultConfig)
					}
					p.mu.Unlock()

					p.wg.Add(2)
					// Directional copy (handleConn(wConn, rConn) reads from rConn, writes to wConn)
					// Backend to client: read from backend, write to client (no fault)
					go p.handleConn(conn, remoteConn)
					// Client to backend: read from client, write to backend via faultWriter
					go p.handleConn(fw, conn)
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
		p.faultWriters = nil
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

func (p *Proxy) SetFault(config FaultConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.faultConfig = &config
	for _, fw := range p.faultWriters {
		fw.SetFault(config)
	}
}

func (p *Proxy) ClearFault() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.faultConfig = nil
	for _, fw := range p.faultWriters {
		fw.ClearFault()
	}
}

func (p *Proxy) IsFaultEnabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.faultConfig != nil
}
