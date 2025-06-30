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
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/stretchr/testify/assert"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

type mockAddr struct {
}

func (mockAddr) Network() string { return "tcp" }
func (mockAddr) String() string  { return "" }

type mockConn struct {
}

func (mockConn) Read(b []byte) (n int, err error)   { return len(b), nil }
func (mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (mockConn) Close() error                       { return nil }
func (mockConn) LocalAddr() net.Addr                { return mockAddr{} }
func (mockConn) RemoteAddr() net.Addr               { return mockAddr{} }
func (mockConn) SetDeadline(t time.Time) error      { return nil }
func (mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (mockConn) SetWriteDeadline(t time.Time) error { return nil }

func getProxyInEndpoint(peerID uint64) string {
	return fmt.Sprintf("127.0.0.1:%d", 14200+peerID)
}

func TestRaftNetwork_AddProxy(t *testing.T) {
	p := New()
	defer p.TeardownNetwork()
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     1,
		InAddr: "1",
	}))
	_, ok := p.proxy[1]
	assert.Equal(t, true, ok)
	assert.Error(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     1,
		InAddr: "1",
	}))
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     2,
		InAddr: "2",
	}))
	_, ok = p.proxy[2]
	assert.Equal(t, true, ok)
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     3,
		InAddr: "3",
	}))
	_, ok = p.proxy[3]
	assert.Equal(t, true, ok)
	m, ok := p.proxyConnectMap[1]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["2"])
	m, ok = p.proxyConnectMap[1]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["3"])
	m, ok = p.proxyConnectMap[2]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["1"])
	m, ok = p.proxyConnectMap[2]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["3"])
	m, ok = p.proxyConnectMap[3]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["1"])
	m, ok = p.proxyConnectMap[3]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, m["2"])
}

func TestRaftNetwork_EnableDisableProxy(t *testing.T) {
	t.Run("connect failure: not found proxy", func(t *testing.T) {
		p := New()
		assert.Error(t, p.ConnectProxy(2))
		assert.Nil(t, p.TeardownNetwork())
	})
	t.Run("disconnect failure: not found proxy", func(t *testing.T) {
		p := New()
		assert.Error(t, p.DisconnectProxy(2))
		assert.Nil(t, p.TeardownNetwork())
	})
	t.Run("success", func(t *testing.T) {
		for _, tc := range testTLSConfig {
			p := New()
			assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
				Id:                1,
				InAddr:            "127.0.0.1:14200",
				InListenTLSConfig: tc.serverTLS,
				OutDialTLSConfig:  tc.clientTls,
			}))
			assert.Nil(t, p.ConnectProxy(1))
			assert.Nil(t, p.DisconnectProxy(1))
			assert.Nil(t, p.TeardownNetwork())
		}
	})
}

func TestRaftNetwork_DeleteProxy(t *testing.T) {

	t.Run("failure: not found proxy", func(t *testing.T) {
		p := New()
		assert.Error(t, p.DeleteProxy(2))
		assert.Nil(t, p.TeardownNetwork())
	})
	t.Run("success", func(t *testing.T) {
		p := New()
		for i := 0; i < 3; i++ {
			assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
				Id:     uint64(i + 1),
				InAddr: getProxyInEndpoint(uint64(i + 1)),
			}))
		}
		assert.Nil(t, p.SetPartition([]uint64{1, 2, 3}))
		assert.Nil(t, p.DeleteProxy(1))
		_, ok := p.proxy[1]
		assert.Equal(t, false, ok)
		_, ok = p.proxyConnectMap[1]
		assert.Equal(t, false, ok)
		assert.Nil(t, p.TeardownNetwork())
	})
}

type partition struct {
	t          *testing.T
	n          *ProxyNetwork
	connectIds []uint64
}

func newPartition(t *testing.T, n *ProxyNetwork, connectIds []uint64) *partition {
	assert.Nil(t, n.SetPartition(connectIds))
	return &partition{
		t:          t,
		n:          n,
		connectIds: connectIds,
	}
}

func (p *partition) CanConnect(testEndpoints []string) {
	for _, endpoint := range testEndpoints {
		for _, pId := range p.connectIds {
			if p.n.proxy[pId].config.InAddr != endpoint {
				assert.Equal(p.t, true, p.n.proxyConnectMap[pId][endpoint])
				_, err := p.n.Dial(ibabuza.TLSConfig{}, pId, endpoint)
				assert.Nil(p.t, err)
			}
		}
	}
}

func (p *partition) CanNotConnect(testEndpoints []string) {
	for _, endpoint := range testEndpoints {
		for _, pId := range p.connectIds {
			assert.Equal(p.t, false, p.n.proxyConnectMap[pId][endpoint])
			_, err := p.n.Dial(ibabuza.TLSConfig{}, pId, endpoint)
			assert.Error(p.t, err)
		}
	}
}

func TestProxyNetwork_Partition(t *testing.T) {
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}

	t.Run("two partitions", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		for i := 0; i < 3; i++ {
			assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
				Id:     uint64(i + 1),
				InAddr: getProxyInEndpoint(uint64(i + 1)),
			}))
		}
		p1 := newPartition(t, p, []uint64{1, 2})
		p2 := newPartition(t, p, []uint64{3})
		// 1,2 can not connect to 3
		p1.CanNotConnect([]string{getProxyInEndpoint(3)})
		// 1 and 2 can be connected to each other
		p1.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2)})
		// 3 can not connect to 1,2
		p2.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2)})

		//recover
		p3 := newPartition(t, p, []uint64{1, 2, 3})
		p3.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3)})
	})

	t.Run("three partitions", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		for i := 0; i < 5; i++ {
			assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
				Id:     uint64(i + 1),
				InAddr: getProxyInEndpoint(uint64(i + 1)),
			}))
		}
		//1,2 are partition 1
		//3,4 are partition 2
		//5 is partition 3
		p1 := newPartition(t, p, []uint64{1, 2})
		p2 := newPartition(t, p, []uint64{3, 4})
		p3 := newPartition(t, p, []uint64{5})
		// 5 can not connect to 1,2,3,4
		p3.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4)})
		// 3,4 can not connect to 1,2,5
		p2.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(5)})
		// 1,2 can not connect to 3,4,5
		p1.CanNotConnect([]string{getProxyInEndpoint(3), getProxyInEndpoint(4), getProxyInEndpoint(5)})

		//connect each other
		p1.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2)})
		p2.CanConnect([]string{getProxyInEndpoint(3), getProxyInEndpoint(4)})

		//recover
		p4 := newPartition(t, p, []uint64{1, 2, 3, 4, 5})
		p4.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4), getProxyInEndpoint(5)})
	})

	t.Run("re-partitions", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		for i := 0; i < 5; i++ {
			assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
				Id:     uint64(i + 1),
				InAddr: getProxyInEndpoint(uint64(i + 1)),
			}))
		}
		//1,5 are partition 1
		//2,3,4 are partition 2
		p1 := newPartition(t, p, []uint64{1, 5})
		p2 := newPartition(t, p, []uint64{2, 3, 4})

		p1.CanNotConnect([]string{getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4)})
		p2.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(5)})
		p1.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(5)})
		p2.CanConnect([]string{getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4)})

		//1,2,3 are partition 1
		//4,5 are partition 2
		p1 = newPartition(t, p, []uint64{1, 2, 3})
		p2 = newPartition(t, p, []uint64{4, 5})
		p1.CanNotConnect([]string{getProxyInEndpoint(4), getProxyInEndpoint(4)})
		p2.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3)})
		p1.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3)})
		p2.CanConnect([]string{getProxyInEndpoint(4), getProxyInEndpoint(5)})

		//1,2 are partition 1
		//3,4 are partition 2
		//3 is partition 3
		p1 = newPartition(t, p, []uint64{1, 2})
		p2 = newPartition(t, p, []uint64{4, 5})
		p3 := newPartition(t, p, []uint64{3})
		p1.CanNotConnect([]string{getProxyInEndpoint(3), getProxyInEndpoint(4), getProxyInEndpoint(5)})
		p2.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3)})
		p3.CanNotConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4)})
		p1.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2)})
		p2.CanConnect([]string{getProxyInEndpoint(4), getProxyInEndpoint(5)})
		//recover
		p4 := newPartition(t, p, []uint64{1, 2, 3, 4, 5})
		p4.CanConnect([]string{getProxyInEndpoint(1), getProxyInEndpoint(2), getProxyInEndpoint(3), getProxyInEndpoint(4), getProxyInEndpoint(5)})
	})
}

func TestRaftNetwork_Dial(t *testing.T) {
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}
	t.Run("success", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))
		assert.Nil(t, p.SetPartition([]uint64{1, 2}))
		_, err := p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)
		_, err = p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)
		conns, ok := p.dialToProxyConn[1]
		assert.Equal(t, true, ok)
		connMap, ok := conns[getProxyInEndpoint(2)]
		assert.Equal(t, 2, len(connMap))
	})
	t.Run("disable proxy", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))
		assert.Nil(t, p.SetPartition([]uint64{1, 2}))
		_, err := p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)
		_, err = p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)
		assert.Nil(t, p.DisconnectProxy(2))

		conns, ok := p.dialToProxyConn[1]
		assert.Equal(t, true, ok)
		_, ok = conns[getProxyInEndpoint(2)]
		assert.Equal(t, false, ok)
	})
	t.Run("dial each other: disable proxy", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContext = mockDial
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))
		assert.Nil(t, p.SetPartition([]uint64{1, 2}))
		_, err := p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)
		_, err = p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
		assert.Nil(t, err)

		_, err = p.Dial(ibabuza.TLSConfig{}, 2, getProxyInEndpoint(1))
		assert.Nil(t, err)
		_, err = p.Dial(ibabuza.TLSConfig{}, 2, getProxyInEndpoint(1))
		assert.Nil(t, err)

		assert.Nil(t, p.DisconnectProxy(1))
		_, ok := p.dialToProxyConn[1]
		assert.Equal(t, false, ok)
		_, ok = p.dialToProxyConn[2][getProxyInEndpoint(1)]
		assert.Equal(t, false, ok)
	})
}

func TestRaftNetwork_PartitionDisconnect(t *testing.T) {
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}
	p := New()
	defer p.TeardownNetwork()
	p.dialContext = mockDial
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     1,
		InAddr: getProxyInEndpoint(1),
	}))
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     2,
		InAddr: getProxyInEndpoint(2),
	}))
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     3,
		InAddr: getProxyInEndpoint(3),
	}))
	assert.Nil(t, p.SetPartition([]uint64{1, 2, 3}))

	_, err := p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2))
	assert.Nil(t, err)
	_, err = p.Dial(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(3))
	assert.Nil(t, err)

	assert.Nil(t, p.partitionDisconnect(1, []uint64{3}))
	conns, ok := p.dialToProxyConn[1]
	assert.Equal(t, true, ok)
	_, ok = conns[getProxyInEndpoint(2)]
	assert.Equal(t, true, ok)
	_, ok = conns[getProxyInEndpoint(3)]
	assert.Equal(t, false, ok)

	assert.Nil(t, p.partitionDisconnect(1, []uint64{2, 3}))
	conns, ok = p.dialToProxyConn[1]
	assert.Equal(t, true, ok)
	assert.Equal(t, 0, len(conns))
}

func TestProxyNetwork_ConnectProxiesIds(t *testing.T) {
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}
	p := New()
	defer p.TeardownNetwork()
	p.dialContext = mockDial
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     1,
		InAddr: getProxyInEndpoint(1),
	}))
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     2,
		InAddr: getProxyInEndpoint(2),
	}))
	disconnectedIds := p.DisconnectProxiesIds()
	assert.Equal(t, 2, len(disconnectedIds))
	assert.Nil(t, p.ConnectProxy(1))
	assert.Nil(t, p.ConnectProxy(2))
	connectedIds := p.ConnectProxiesIds()
	assert.Equal(t, 2, len(connectedIds))
	for _, id := range connectedIds {
		assert.Contains(t, []uint64{1, 2}, id)
	}
}

func TestProxyNetwork_DisconnectProxiesIds(t *testing.T) {
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}
	p := New()
	defer p.TeardownNetwork()
	p.dialContext = mockDial
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     1,
		InAddr: getProxyInEndpoint(1),
	}))
	assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
		Id:     2,
		InAddr: getProxyInEndpoint(2),
	}))
	disconnectedIds := p.DisconnectProxiesIds()
	assert.Equal(t, 2, len(disconnectedIds))
	assert.Nil(t, p.ConnectProxy(1))
	assert.Nil(t, p.DisconnectProxy(1))

	disconnectedIds = p.DisconnectProxiesIds()
	assert.Equal(t, 2, len(disconnectedIds))
	for _, id := range disconnectedIds {
		assert.Contains(t, []uint64{1, 2}, id)
	}
}

func TestSaveTopologyAsSVG(t *testing.T) {
	// Setup test file name
	testFile := os.TempDir() + "test_topology.svg"
	t.Log(testFile)
	// Clean up after test
	defer os.Remove(testFile)

	// Create a new ProxyNetwork instance
	mockDial := func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
		return mockConn{}, nil
	}
	network := New()
	defer network.TeardownNetwork()
	network.dialContext = mockDial
	// Create test proxy configurations
	proxies := []ibabuza.ProxyConfig{
		{
			Id:      1,
			InAddr:  "localhost:8081",
			OutAddr: "localhost:9081",
		},
		{
			Id:      2,
			InAddr:  "localhost:8082",
			OutAddr: "localhost:9082",
		},
		{
			Id:      3,
			InAddr:  "localhost:8083",
			OutAddr: "localhost:9083",
		},
		{
			Id:      4,
			InAddr:  "localhost:8084",
			OutAddr: "localhost:9084",
		},
		{
			Id:      5,
			InAddr:  "localhost:8085",
			OutAddr: "localhost:9085",
		},
		{
			Id:      6,
			InAddr:  "localhost:8086",
			OutAddr: "localhost:9086",
		},
		{
			Id:      7,
			InAddr:  "localhost:8087",
			OutAddr: "localhost:9087",
		},
		{
			Id:      8,
			InAddr:  "localhost:8088",
			OutAddr: "localhost:9088",
		},
	}

	// Add proxies to network
	for _, cfg := range proxies {
		err := network.AddProxy(cfg)
		if err != nil {
			t.Fatalf("Failed to add proxy: %v", err)
		}

		err = network.ConnectProxy(cfg.Id)
		if err != nil {
			t.Fatalf("Failed to connect proxy %d: %v", cfg.Id, err)
		}
	}

	// Set up some test partitions
	err := network.SetPartition([]uint64{1, 2})
	if err != nil {
		t.Fatalf("Failed to set partition 1,2: %v", err)
	}

	err = network.SetPartition([]uint64{3, 4})
	if err != nil {
		t.Fatalf("Failed to set partition 3,4: %v", err)
	}

	err = network.SetPartition([]uint64{5, 6, 7, 8})
	if err != nil {
		t.Fatalf("Failed to set partition 5,6,7,8: %v", err)
	}

	// Test SaveTopologyAsSVG
	err = network.SaveTopologyAsSVG(testFile)
	if err != nil {
		t.Fatalf("SaveTopologyAsSVG failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Error("SVG file was not created")
	}

	// Read file content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read SVG file: %v", err)
	}

	// Verify basic SVG content
	tests := []struct {
		name     string
		contains string
	}{
		{"SVG Header", `<?xml version="1.0" encoding="UTF-8"`},
		{"SVG Root Element", "<svg"},
		{"Proxy1 Content", "Proxy 1"},
		{"Proxy2 Content", "Proxy 2"},
		{"Proxy3 Content", "Proxy 3"},
		{"Proxy4 Content", "Proxy 4"},
		{"Proxy5 Content", "Proxy 5"},
		{"Proxy6 Content", "Proxy 6"},
		{"Proxy7 Content", "Proxy 7"},
		{"Proxy8 Content", "Proxy 8"},
		{"Proxy Status", "enabled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(string(content), tt.contains) {
				t.Errorf("SVG file missing expected content: %s", tt.contains)
			}
		})
	}

	// Test file size is reasonable
	fileInfo, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	if fileInfo.Size() == 0 {
		t.Error("SVG file is empty")
	}
	if fileInfo.Size() > 1000000 { // 1MB
		t.Error("SVG file is unusually large")
	}
}

func TestRaftNetwork_DialWithTimeout(t *testing.T) {
	// Create a mock dial function that succeeds immediately
	mockSuccessDial := func(tlsConfig ibabuza.TLSConfig, endpoint string, timeout time.Duration) (net.Conn, error) {
		return mockConn{}, nil
	}

	// Create a mock dial function that simulates a timeout
	mockTimeoutDial := func(tlsConfig ibabuza.TLSConfig, endpoint string, timeout time.Duration) (net.Conn, error) {
		return nil, fmt.Errorf("connection timeout")
	}

	t.Run("success with timeout", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContextWithTimeout = mockSuccessDial

		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))
		assert.Nil(t, p.SetPartition([]uint64{1, 2}))

		// Test with different timeout values
		timeouts := []time.Duration{
			100 * time.Millisecond,
			1 * time.Second,
			5 * time.Second,
		}

		for _, timeout := range timeouts {
			conn, err := p.DialWithTimeout(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2), timeout)
			assert.Nil(t, err)
			assert.NotNil(t, conn)
		}

		// Verify connections were tracked correctly
		conns, ok := p.dialToProxyConn[1]
		assert.Equal(t, true, ok)
		connMap, ok := conns[getProxyInEndpoint(2)]
		assert.Equal(t, 3, len(connMap))
	})

	t.Run("connection timeout", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContextWithTimeout = mockTimeoutDial

		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))
		assert.Nil(t, p.SetPartition([]uint64{1, 2}))

		conn, err := p.DialWithTimeout(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2), 100*time.Millisecond)
		assert.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("dial from disconnected proxy", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()
		p.dialContextWithTimeout = mockSuccessDial

		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     1,
			InAddr: getProxyInEndpoint(1),
		}))
		assert.Nil(t, p.AddProxy(ibabuza.ProxyConfig{
			Id:     2,
			InAddr: getProxyInEndpoint(2),
		}))

		// Without setting partition, proxies can't connect
		conn, err := p.DialWithTimeout(ibabuza.TLSConfig{}, 1, getProxyInEndpoint(2), 100*time.Millisecond)
		assert.Error(t, err)
		assert.Equal(t, ErrDialFromDisconnectedPeer, err)
		assert.Nil(t, conn)
	})

	t.Run("nonexistent proxy", func(t *testing.T) {
		p := New()
		defer p.TeardownNetwork()

		// Try to dial from a proxy that doesn't exist
		conn, err := p.DialWithTimeout(ibabuza.TLSConfig{}, 999, getProxyInEndpoint(1), 100*time.Millisecond)
		assert.Error(t, err)
		assert.Equal(t, ErrNotExistProxy, err)
		assert.Nil(t, conn)
	})
}
