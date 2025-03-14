package connpool

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrMaxConnectionsReached = errors.New("max connections per host reached")
	ErrConnectionNotFound    = errors.New("connection not found")
)

type connectionWrapper[V ComparableConnection] struct {
	conn     V
	addr     string
	inUse    bool
	lastUsed time.Time
	mu       sync.RWMutex
}

type ConnectionPool[T ComparableConnection] struct {
	mu          sync.RWMutex
	connections map[string][]*connectionWrapper[T]
	connCreator ConnectionDialer[T]
	config      Config
	stopCleaner chan struct{}
}

func NewConnectionPool[T ComparableConnection](connCreator ConnectionDialer[T], config Config) *ConnectionPool[T] {
	pool := &ConnectionPool[T]{
		connections: make(map[string][]*connectionWrapper[T]),
		connCreator: connCreator,
		config:      config,
		stopCleaner: make(chan struct{}),
	}
	go pool.startCleanup()
	return pool
}

func (p *ConnectionPool[T]) Get(address string) (T, error) {
	p.mu.RLock()
	conns, exists := p.connections[address]
	p.mu.RUnlock()

	if exists {
		for _, c := range conns {
			c.mu.Lock()
			if !c.inUse {
				c.inUse = true
				c.lastUsed = time.Now()
				c.mu.Unlock()
				return c.conn, nil
			}
			c.mu.Unlock()
		}
		if len(conns) >= p.config.MaxConnectionsPerHost {
			var zero T
			return zero, ErrMaxConnectionsReached
		}
	}

	conn, err := p.connCreator.Dial(address)
	if err != nil {
		var zero T
		return zero, err
	}

	wrapper := &connectionWrapper[T]{
		conn:     conn,
		addr:     address,
		inUse:    true,
		lastUsed: time.Now(),
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	connsList, exists := p.connections[address]
	if !exists {
		p.connections[address] = []*connectionWrapper[T]{wrapper}
	} else {
		p.connections[address] = append(connsList, wrapper)
	}

	return conn, nil
}

func (p *ConnectionPool[T]) Put(conn T) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conns := range p.connections {
		for _, c := range conns {
			c.mu.Lock()
			if c.conn == conn {
				c.inUse = false
				c.lastUsed = time.Now()
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
		}
	}
	return ErrConnectionNotFound
}

func (p *ConnectionPool[T]) Remove(conn T) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conns := range p.connections {
		for i, c := range conns {
			if c.conn == conn {
				_ = c.conn.Close()

				lastIdx := len(conns) - 1
				conns[i] = conns[lastIdx]
				p.connections[addr] = conns[:lastIdx]

				if len(p.connections[addr]) == 0 {
					delete(p.connections, addr)
				}

				return nil
			}
		}
	}
	return ErrConnectionNotFound
}

func (p *ConnectionPool[T]) Close() error {
	close(p.stopCleaner)
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conns := range p.connections {
		for _, conn := range conns {
			_ = conn.conn.Close()
		}
		delete(p.connections, addr)
	}

	return nil
}

func (p *ConnectionPool[T]) GetActiveConnectionCount(address string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conns, ok := p.connections[address]
	if !ok {
		return 0
	}

	count := 0
	for _, conn := range conns {
		conn.mu.RLock()
		if conn.inUse {
			count++
		}
		conn.mu.RUnlock()
	}

	return count
}

func (p *ConnectionPool[T]) GetIdleConnectionCount(address string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conns, ok := p.connections[address]
	if !ok {
		return 0
	}

	count := 0
	for _, conn := range conns {
		conn.mu.RLock()
		if !conn.inUse {
			count++
		}
		conn.mu.RUnlock()
	}

	return count
}

func (p *ConnectionPool[T]) startCleanup() {
	cleanupTicker := time.NewTicker(p.config.IdleTimeout / 2)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-p.stopCleaner:
			return
		case <-cleanupTicker.C:
			p.mu.Lock()
			now := time.Now()
			for addr, conns := range p.connections {
				var remaining []*connectionWrapper[T]

				for _, conn := range conns {
					if !conn.inUse && now.Sub(conn.lastUsed) > p.config.IdleTimeout {
						_ = conn.conn.Close()
					} else {
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
