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
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
)

var (
	ErrExistProxy               = errors.New("tcp proxy network: proxy is existing")
	ErrNotExistProxy            = errors.New("tcp proxy network: proxy is not existing")
	ErrDialFromDisconnectedPeer = errors.New("tcp proxy network: can not dial from disconnected peer")
)

type DialContext func(tlsConfig ibabuza.TLSConfig, endpoint string) (net.Conn, error)
type DialContextTimeout func(tlsConfig ibabuza.TLSConfig, endpoint string, timeout time.Duration) (net.Conn, error)

type ProxyNetwork struct {
	dialContext            DialContext
	dialContextWithTimeout DialContextTimeout
	proxy                  map[uint64]*Proxy
	dialToProxyConn        map[uint64]map[string][]net.Conn
	proxyConnectMap        map[uint64]map[string]bool
	mu                     sync.Mutex
}

func New() *ProxyNetwork {
	return &ProxyNetwork{
		dialContext:            netutil.TcpDial,
		dialContextWithTimeout: netutil.TcpDialTimeout,
		proxy:                  make(map[uint64]*Proxy),
		dialToProxyConn:        make(map[uint64]map[string][]net.Conn),
		proxyConnectMap:        make(map[uint64]map[string]bool),
	}
}

func (n *ProxyNetwork) AddProxy(config ibabuza.ProxyConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.proxy[config.Id]
	if ok {
		return ErrExistProxy
	}
	p := NewProxy(config)
	addProxyId := config.Id

	n.proxyConnectMap[addProxyId] = make(map[string]bool)
	for _, otherProxy := range n.proxy {
		n.proxyConnectMap[addProxyId][otherProxy.config.InAddr] = false
	}
	for _, otherProxy := range n.proxy {
		n.proxyConnectMap[otherProxy.config.Id][p.config.InAddr] = false
	}
	n.proxy[addProxyId] = p
	return nil
}

func (n *ProxyNetwork) DeleteProxy(proxyId uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.disableProxy(proxyId); err != nil {
		return err
	}
	delete(n.proxy, proxyId)
	delete(n.proxyConnectMap, proxyId)
	return nil
}

func (n *ProxyNetwork) ConnectProxy(proxyId uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ok := n.proxy[proxyId]
	if !ok {
		return ErrNotExistProxy
	}
	return p.Enable()
}

func (n *ProxyNetwork) DisconnectProxy(proxyId uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.disableProxy(proxyId)
}

func (n *ProxyNetwork) IsProxyConnected(proxyId uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ok := n.proxy[proxyId]
	if !ok {
		return false
	}
	return p.enable
}

func (n *ProxyNetwork) SetPartition(connectProxyIds []uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var disconnectProxyIds []uint64
	for pId, _ := range n.proxy {
		exist := false
		for _, connectId := range connectProxyIds {
			if pId == connectId {
				exist = true
				break
			}
		}
		if !exist {
			disconnectProxyIds = append(disconnectProxyIds, pId)
		}
	}
	for _, connectId := range connectProxyIds {
		if err := n.partitionDisconnect(connectId, disconnectProxyIds); err != nil {
			return err
		}
	}
	for _, disconnectId := range disconnectProxyIds {
		if err := n.partitionDisconnect(disconnectId, connectProxyIds); err != nil {
			return err
		}
	}
	if err := n.partitionConnect(connectProxyIds); err != nil {
		return err
	}
	return nil
}

func (n *ProxyNetwork) TeardownNetwork() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	mErrs := multierror.New()
	for _, p := range n.proxy {
		if err := p.Disable(); err != nil {
			mErrs.Append(err)
		}
	}
	return mErrs.Get()
}

func (n *ProxyNetwork) Dial(cfg ibabuza.TLSConfig, fromProxyId uint64, toProxyInEndpoint string) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	canDialMap, ok := n.proxyConnectMap[fromProxyId]
	if !ok {
		return nil, ErrNotExistProxy
	}
	canDial, ok := canDialMap[toProxyInEndpoint]
	if !ok {
		return nil, ErrNotExistProxy
	}
	if !canDial {
		return nil, ErrDialFromDisconnectedPeer
	}
	conn, err := n.dialContext(cfg, toProxyInEndpoint)
	if err != nil {
		return nil, err
	}
	pConn := newProxyDialConn(conn)
	dialToProxyConn, ok := n.dialToProxyConn[fromProxyId]
	if !ok {
		dialToProxyConn = make(map[string][]net.Conn)
	}
	dialToProxyConn[toProxyInEndpoint] = append(dialToProxyConn[toProxyInEndpoint], pConn)
	n.dialToProxyConn[fromProxyId] = dialToProxyConn
	return pConn, nil
}

func (n *ProxyNetwork) DialWithTimeout(cfg ibabuza.TLSConfig, fromProxyId uint64, toProxyInEndpoint string, timeout time.Duration) (net.Conn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	canDialMap, ok := n.proxyConnectMap[fromProxyId]
	if !ok {
		return nil, ErrNotExistProxy
	}
	canDial, ok := canDialMap[toProxyInEndpoint]
	if !ok {
		return nil, ErrNotExistProxy
	}
	if !canDial {
		return nil, ErrDialFromDisconnectedPeer
	}
	conn, err := n.dialContextWithTimeout(cfg, toProxyInEndpoint, timeout)
	if err != nil {
		return nil, err
	}
	pConn := newProxyDialConn(conn)
	dialToProxyConn, ok := n.dialToProxyConn[fromProxyId]
	if !ok {
		dialToProxyConn = make(map[string][]net.Conn)
	}
	dialToProxyConn[toProxyInEndpoint] = append(dialToProxyConn[toProxyInEndpoint], pConn)
	n.dialToProxyConn[fromProxyId] = dialToProxyConn
	return pConn, nil
}

func (n *ProxyNetwork) Listen(tlsCfg ibabuza.TLSConfig, endpoint string) (net.Listener, error) {
	return netutil.TcpListen(tlsCfg, endpoint)
}

func (n *ProxyNetwork) disableProxy(proxyId uint64) error {
	disableProxy, ok := n.proxy[proxyId]
	if !ok {
		return ErrNotExistProxy
	}
	if err := disableProxy.Disable(); err != nil {
		return err
	}
	dialConn, ok := n.dialToProxyConn[proxyId]
	if ok {
		for _, conns := range dialConn {
			for _, conn := range conns {
				conn.Close()
			}
		}
		delete(n.dialToProxyConn, proxyId)
	}

	for _, otherProxy := range n.proxy {
		if proxyId != otherProxy.config.Id {
			dialConn, ok = n.dialToProxyConn[otherProxy.config.Id]
			if ok {
				for inEndpoint, conns := range dialConn {
					if inEndpoint == disableProxy.config.InAddr {
						for _, conn := range conns {
							conn.Close()
						}
						delete(dialConn, inEndpoint)
					}
				}
				n.dialToProxyConn[otherProxy.config.Id] = dialConn
			}
		}
	}
	return nil
}

func (n *ProxyNetwork) partitionConnect(connectPeerIds []uint64) error {
	for _, cId1 := range connectPeerIds {
		for _, cId2 := range connectPeerIds {
			if cId1 != cId2 {
				connectProxy, ok := n.proxy[cId2]
				if !ok {
					return ErrNotExistProxy
				}
				n.proxyConnectMap[cId1][connectProxy.config.InAddr] = true
			}
		}
	}
	return nil
}

func (n *ProxyNetwork) partitionDisconnect(pId uint64, disconnectIds []uint64) error {
	for _, dId := range disconnectIds {
		disconnectProxy, ok := n.proxy[dId]
		if !ok {
			return ErrNotExistProxy
		}
		n.proxyConnectMap[pId][disconnectProxy.config.InAddr] = false
		dialConn, ok := n.dialToProxyConn[pId]
		if ok {
			for proxyInEndpoint, conns := range dialConn {
				if proxyInEndpoint == disconnectProxy.config.InAddr {
					for _, conn := range conns {
						conn.Close()
					}
					delete(dialConn, proxyInEndpoint)
				}
			}
			n.dialToProxyConn[pId] = dialConn
		}
	}
	return nil
}

func (n *ProxyNetwork) ConnectProxiesIds() []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := make([]uint64, 0, len(n.proxy))
	for id, p := range n.proxy {
		if p.enable {
			result = append(result, id)
		}
	}
	return result
}

func (n *ProxyNetwork) DisconnectProxiesIds() []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := make([]uint64, 0, len(n.proxy))
	for id, p := range n.proxy {
		if !p.enable {
			result = append(result, id)
		}
	}
	return result
}

func (n *ProxyNetwork) SetProxyFault(proxyId uint64, config FaultConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ok := n.proxy[proxyId]
	if !ok {
		return ErrNotExistProxy
	}
	p.SetFault(config)
	return nil
}

func (n *ProxyNetwork) ClearProxyFault(proxyId uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ok := n.proxy[proxyId]
	if !ok {
		return ErrNotExistProxy
	}
	p.ClearFault()
	return nil
}

func (n *ProxyNetwork) SaveTopologyAsSVG(filename string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Find partitions by checking connectivity
	partitions := make(map[uint64][]uint64)
	partitionId := uint64(0)
	processedProxies := make(map[uint64]bool)

	// Helper function to find all connected proxies
	var findConnectedProxies func(startId uint64, currentPartition []uint64) []uint64
	findConnectedProxies = func(startId uint64, currentPartition []uint64) []uint64 {
		if processedProxies[startId] {
			return currentPartition
		}

		processedProxies[startId] = true
		currentPartition = append(currentPartition, startId)

		for otherId, proxy := range n.proxy {
			if !processedProxies[otherId] && n.proxyConnectMap[startId][proxy.config.InAddr] {
				currentPartition = findConnectedProxies(otherId, currentPartition)
			}
		}
		return currentPartition
	}

	// Find connected partitions
	for proxyId := range n.proxy {
		if !processedProxies[proxyId] {
			connectedGroup := findConnectedProxies(proxyId, []uint64{})
			if len(connectedGroup) > 0 {
				partitions[partitionId] = connectedGroup
				partitionId++
			}
		}
	}

	// Calculate canvas dimensions
	const (
		nodeWidth   = 220
		nodeHeight  = 120
		nodeSpacing = 40
		rowSpacing  = 50
		leftPadding = 140
		topPadding  = 80
	)

	// Find maximum number of proxies in any partition
	maxProxiesInRow := 0
	for _, proxies := range partitions {
		if len(proxies) > maxProxiesInRow {
			maxProxiesInRow = len(proxies)
		}
	}

	width := leftPadding + (nodeWidth+nodeSpacing)*maxProxiesInRow + nodeSpacing
	height := topPadding + (nodeHeight+rowSpacing)*len(partitions) + rowSpacing

	// Start SVG document
	svg := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<svg width="%d" height="%d" xmlns="http://www.w3.org/2000/svg">
    <defs>
        <filter id="dropShadow" x="-20%%" y="-20%%" width="140%%" height="140%%">
            <feGaussianBlur in="SourceAlpha" stdDeviation="3"/>
            <feOffset dx="2" dy="2"/>
            <feComponentTransfer>
                <feFuncA type="linear" slope="0.3"/>
            </feComponentTransfer>
            <feMerge>
                <feMergeNode/>
                <feMergeNode in="SourceGraphic"/>
            </feMerge>
        </filter>
    </defs>
    <rect width="100%%" height="100%%" fill="white"/>
`, width, height)

	// Calculate node positions and draw partition labels
	positions := make(map[uint64]struct{ x, y int })
	for pId, proxyIds := range partitions {
		// Calculate row y position
		rowY := int(pId)*(nodeHeight+rowSpacing) + rowSpacing + topPadding

		// Draw partition label
		svg += fmt.Sprintf(`
        <text x="%d" y="%d" font-family="Arial" font-size="16" font-weight="bold" fill="#333">
            Partition %d
        </text>
        `, 30, rowY+nodeHeight/3, pId+1)

		// Position nodes in the row
		for i, proxyId := range proxyIds {
			x := leftPadding + (nodeWidth+nodeSpacing)*i + nodeSpacing + nodeWidth/2
			y := rowY + nodeHeight/2
			positions[proxyId] = struct{ x, y int }{x, y}
		}
	}

	// Draw nodes
	for id, proxy := range n.proxy {
		pos := positions[id]
		fillColor := "#ff9999" // disabled color
		if proxy.enable {
			fillColor = "#99ff99" // enabled color
		}

		rectX := pos.x - nodeWidth/2
		rectY := pos.y - nodeHeight/2

		svg += fmt.Sprintf(`    <rect x="%d" y="%d" width="%d" height="%d" rx="10" ry="10" 
            fill="%s" stroke="#333" stroke-width="2" filter="url(#dropShadow)"/>
`, rectX, rectY, nodeWidth, nodeHeight, fillColor)

		status := "disabled"
		if proxy.enable {
			status = "enabled"
		}

		// Add node information (simplified)
		texts := []string{
			fmt.Sprintf("Proxy %d (%s)", id, status),
			"In: " + strings.TrimPrefix(proxy.config.InAddr, "localhost:"),
			"Out: " + strings.TrimPrefix(proxy.config.OutAddr, "localhost:"),
		}

		textY := pos.y - 20
		for _, text := range texts {
			svg += fmt.Sprintf(`    <text x="%d" y="%d" text-anchor="middle" font-family="Arial" font-size="12">%s</text>
`, pos.x, textY, text)
			textY += 25
		}
	}

	// Add legend (moved to top-center)
	legendX := width/2 - 90
	legendY := 20
	legendWidth := 180
	legendHeight := 80
	svg += addLegend(legendX, legendY, legendWidth, legendHeight)

	// End SVG document
	svg += "</svg>"

	// Write to file
	return os.WriteFile(filename, []byte(svg), 0644)
}

// Helper function to add legend
func addLegend(x, y, width, height int) string {
	padding := 10
	itemSpacing := 30
	squareSize := 16

	return fmt.Sprintf(`
    <rect x="%d" y="%d" width="%d" height="%d" rx="5" ry="5" 
          fill="white" stroke="#333" stroke-width="1" filter="url(#dropShadow)"/>
    
    <text x="%d" y="%d" font-family="Arial" font-size="14" font-weight="bold">Legend</text>

    <rect x="%d" y="%d" width="%d" height="%d" fill="#99ff99" stroke="#333" stroke-width="1"/>
    <text x="%d" y="%d" font-family="Arial" font-size="12">Enabled Proxy</text>

    <rect x="%d" y="%d" width="%d" height="%d" fill="#ff9999" stroke="#333" stroke-width="1"/>
    <text x="%d" y="%d" font-family="Arial" font-size="12">Disabled Proxy</text>`,
		// Legend background
		x, y, width, height,

		// Legend title
		x+padding, y+padding+14,

		// Enabled Proxy
		x+padding, y+padding+20,
		squareSize, squareSize,
		x+padding+squareSize+10, y+padding+20+12,

		// Disabled Proxy
		x+padding, y+padding+20+itemSpacing,
		squareSize, squareSize,
		x+padding+squareSize+10, y+padding+20+itemSpacing+12)
}
