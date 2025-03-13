package connpool

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool/frame"
	"net"
	"sync"
	"time"
)

var (
	ErrMaxConnectionsReached = errors.New("max connections per host reached")
	ErrConnectionClosed      = errors.New("connection is closed")
)

type Dialer interface {
	Dial(ibabuza.TLSConfig, uint64, string) (net.Conn, error)
}

type ConnectionPool struct {
	connections map[string][]*Connection
	mu          sync.RWMutex      // Protects the connections map
	dialer      Dialer            // For creating new connections
	options     Options           // Options for new connections
	tlsConfig   ibabuza.TLSConfig // TLS configuration
	stopCleaner chan struct{}     // Add this field
}

func NewConnectionPool(dialer Dialer, tlsConfig ibabuza.TLSConfig, options Options) *ConnectionPool {
	pool := &ConnectionPool{
		connections: make(map[string][]*Connection),
		dialer:      dialer,
		options:     options,
		tlsConfig:   tlsConfig,
		stopCleaner: make(chan struct{}),
	}
	go pool.cleanIdleConnections()
	return pool
}

func (p *ConnectionPool) cleanIdleConnections() {
	ticker := time.NewTicker(p.options.IdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCleaner:
			return
		case <-ticker.C:
			p.mu.Lock()
			now := time.Now()

			for addr, conns := range p.connections {
				var remaining []*Connection

				for _, conn := range conns {
					conn.mu.Lock()
					if !conn.inUse && now.Sub(conn.lastUsed) > p.options.IdleTimeout {
						_ = conn.conn.Close()
						conn.mu.Unlock()
					} else {
						conn.mu.Unlock()
						remaining = append(remaining, conn)
					}
				}
				if len(remaining) == 0 {
					delete(p.connections, addr)
				} else {
					p.connections[addr] = remaining
				}
			}
			p.mu.Unlock()
		}
	}
}

func (p *ConnectionPool) GetConnection(addr string) (*Connection, error) {
	p.mu.RLock()
	conns, exists := p.connections[addr]
	p.mu.RUnlock()

	if exists {
		for _, conn := range conns {
			conn.mu.Lock()
			if !conn.inUse {
				conn.inUse = true
				conn.lastUsed = time.Now()
				conn.mu.Unlock()
				return conn, nil
			}
			conn.mu.Unlock()
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		conns, exists = p.connections[addr]
		if exists && len(conns) >= p.options.MaxConnectionsPerHost {
			return nil, ErrMaxConnectionsReached
		}
	} else {
		p.mu.Lock()
		defer p.mu.Unlock()
	}

	// Create a new connection
	netConn, err := p.dialer.Dial(p.tlsConfig, 0, addr)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		conn:     netConn,
		reader:   frame.NewReader(netConn),
		writer:   frame.NewWriter(netConn),
		addr:     addr,
		inUse:    true,
		lastUsed: time.Now(),
		cfg:      p.options,
		pool:     p,
	}

	if !exists {
		p.connections[addr] = []*Connection{conn}
	} else {
		p.connections[addr] = append(p.connections[addr], conn)
	}

	return conn, nil
}

func (p *ConnectionPool) ReturnConnection(conn *Connection) {
	if conn == nil {
		return
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	conn.inUse = false
	conn.lastUsed = time.Now()
}

func (p *ConnectionPool) RemoveConnection(conn *Connection) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	conns, exists := p.connections[conn.addr]
	if !exists {
		_ = conn.conn.Close()
		return
	}
	for i, c := range conns {
		if c == conn {
			_ = conn.conn.Close()
			lastIdx := len(conns) - 1
			conns[i] = conns[lastIdx]
			p.connections[conn.addr] = conns[:lastIdx]
			break
		}
	}
}

func (p *ConnectionPool) Close() error {
	close(p.stopCleaner)
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conns := range p.connections {
		for _, conn := range conns {
			conn.mu.Lock()
			_ = conn.conn.Close()
			conn.mu.Unlock()
		}
		delete(p.connections, addr)
	}
	return nil
}
