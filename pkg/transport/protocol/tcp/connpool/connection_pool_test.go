package connpool

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool/frame"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

var defaultOptions = Options{
	WriteDeadline:         time.Second * 2,
	ReadDeadline:          time.Second * 2,
	IdleTimeout:           time.Second * 30,
	DialTimeout:           time.Second,
	MaxConnectionsPerHost: 5,
}

// MockNetworkIO implements the NetworkIO interface for testing
type MockNetworkIO struct {
	dialFunc   func(ibabuza.TLSConfig, string) (net.Conn, error)
	listenFunc func(ibabuza.TLSConfig, string) (net.Listener, error)
}

func (m *MockNetworkIO) Dial(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string) (net.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(config, toEndPoint)
	}
	return nil, errors.New("CreateTransportClient not implemented")
}

func (m *MockNetworkIO) DialWithTimeout(config ibabuza.TLSConfig, fromPeerId uint64, toEndPoint string, timeout time.Duration) (net.Conn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(config, toEndPoint)
	}
	return nil, errors.New("CreateTransportClient not implemented")
}

func (m *MockNetworkIO) Listen(cfg ibabuza.TLSConfig, addr string) (net.Listener, error) {
	if m.listenFunc != nil {
		return m.listenFunc(cfg, addr)
	}
	return nil, errors.New("Listen not implemented")
}

// MockMessage implements the frame.Message interface for testing
type MockMessage struct {
	data []byte
}

func (m *MockMessage) MarshalTo(dAtA []byte) (int, error) {
	copy(dAtA, m.data)
	return len(m.data), nil
}

func (m *MockMessage) Size() int {
	return len(m.data)
}

// MockConn implements net.Conn interface for testing
type MockConn struct {
	r             io.Reader
	w             io.Writer
	closed        bool
	mu            sync.Mutex
	localAddr     net.Addr
	remoteAddr    net.Addr
	readDeadline  time.Time
	writeDeadline time.Time
	readErr       error
	writeErr      error
	closeErr      error
	deadlineErr   error
}

func NewMockConn() *MockConn {
	r, w := io.Pipe()
	return &MockConn{
		r:          r,
		w:          w,
		localAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5678},
	}
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	return m.r.Read(b)
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	return m.w.Write(b)
}

func (m *MockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closeErr != nil {
		return m.closeErr
	}

	m.closed = true
	return nil
}

func (m *MockConn) LocalAddr() net.Addr {
	return m.localAddr
}

func (m *MockConn) RemoteAddr() net.Addr {
	return m.remoteAddr
}

func (m *MockConn) SetDeadline(t time.Time) error {
	if m.deadlineErr != nil {
		return m.deadlineErr
	}
	m.SetReadDeadline(t)
	m.SetWriteDeadline(t)
	return nil
}

func (m *MockConn) SetReadDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deadlineErr != nil {
		return m.deadlineErr
	}

	m.readDeadline = t
	return nil
}

func (m *MockConn) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deadlineErr != nil {
		return m.deadlineErr
	}

	m.writeDeadline = t
	return nil
}

// TestNewConnectionPool tests the creation of a new connection pool
func TestNewConnectionPool(t *testing.T) {
	mockIO := &MockNetworkIO{}
	options := defaultOptions

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)

	assert.NotNil(t, pool)
	assert.NotNil(t, pool.connections)
	assert.Equal(t, mockIO, pool.dialer)
	assert.Equal(t, options, pool.options)

	// Clean up
	pool.Close()
}

// TestGetConnection tests getting a connection from the pool
func TestGetConnection(t *testing.T) {
	mockConn := NewMockConn()
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return mockConn, nil
		},
	}

	options := defaultOptions

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)
	defer pool.Close()

	// Get a new connection
	conn, err := pool.GetConnection("test-addr")

	assert.Nil(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, "test-addr", conn.addr)
	assert.True(t, conn.inUse)
}

// TestGetConnectionMaxReached tests behavior when max connections is reached
func TestGetConnectionMaxReached(t *testing.T) {
	connCount := 0
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			connCount++
			return NewMockConn(), nil
		},
	}

	options := defaultOptions
	options.MaxConnectionsPerHost = 2

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)
	defer pool.Close()

	// Get 2 connections (max allowed)
	conn1, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)
	assert.NotNil(t, conn1)

	conn2, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)
	assert.NotNil(t, conn2)

	// Try to get a third connection, should fail with max connections error
	conn3, err := pool.GetConnection("test-addr")
	assert.Equal(t, ErrMaxConnectionsReached, err)
	assert.Nil(t, conn3)
}

// TestReturnConnection tests returning a connection to the pool
func TestReturnConnection(t *testing.T) {
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return NewMockConn(), nil
		},
	}

	options := defaultOptions

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)
	defer pool.Close()

	// Get a new connection
	conn, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)
	assert.True(t, conn.inUse)

	// Return connection to pool
	pool.ReturnConnection(conn)
	assert.False(t, conn.inUse)

	// Get another connection, should reuse the existing one
	conn2, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)
	assert.Equal(t, conn, conn2) // Should be the same connection
	assert.True(t, conn2.inUse)
}

// TestRemoveConnection tests removing a connection from the pool
func TestRemoveConnection(t *testing.T) {
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return NewMockConn(), nil
		},
	}

	options := defaultOptions
	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)

	// Get a connection
	conn, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)

	// Ensure it's in the pool
	assert.Len(t, pool.connections["test-addr"], 1)

	// Remove the connection
	pool.RemoveConnection(conn)

	// Verify it's removed from the pool
	assert.Len(t, pool.connections["test-addr"], 0)

	// Clean up
	pool.Close()
}

// TestCleanIdleConnections tests that idle connections are cleaned up
func TestCleanIdleConnections(t *testing.T) {
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return NewMockConn(), nil
		},
	}

	// Use a short idle timeout for testing
	options := defaultOptions
	options.IdleTimeout = 100 * time.Millisecond // Very short for testing

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)

	// Get a connection
	conn, err := pool.GetConnection("test-addr")
	assert.Nil(t, err)

	// Return it to the pool (mark as not in use)
	pool.ReturnConnection(conn)

	// Wait for the cleaner to run
	time.Sleep(options.IdleTimeout * 3)

	// The connection should be removed from the pool
	pool.mu.RLock()
	connections, exists := pool.connections["test-addr"]
	pool.mu.RUnlock()

	// Either the connections list should be empty or the address should not exist
	if exists {
		assert.Len(t, connections, 0)
	} else {
		assert.False(t, exists)
	}

	// Clean up
	pool.Close()
}

// TestConnectionSendReadFrame tests sending a frame through a connection then reading it
func TestConnectionSendReadFrame(t *testing.T) {
	mockConn := NewMockConn()

	options := defaultOptions

	pool := NewConnectionPool(nil, ibabuza.TLSConfig{}, options)

	conn := &Connection{
		conn:     mockConn,
		reader:   frame.NewReader(mockConn),
		writer:   frame.NewWriter(mockConn),
		addr:     "test-addr",
		inUse:    true,
		lastUsed: time.Now(),
		cfg:      options,
		pool:     pool,
	}

	// Create a test message
	msg := &MockMessage{data: []byte("test message")}

	// Send the frame
	go func() {
		err := conn.SendFrame(frame.BatchMsgType, msg)
		// Validate
		assert.Nil(t, err)
	}()
	assert.Nil(t, conn.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		assert.Equal(t, msgType, frame.BatchMsgType)
		assert.Equal(t, msg.data, msgBuf)
		return nil
	}))

	// Clean up
	pool.Close()
}

// TestConnectionPoolClose tests closing the connection pool
func TestConnectionPoolClose(t *testing.T) {
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return NewMockConn(), nil
		},
	}

	options := defaultOptions
	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)

	// Get some connections
	conn1, _ := pool.GetConnection("addr1")
	conn2, _ := pool.GetConnection("addr2")

	assert.NotNil(t, conn1)
	assert.NotNil(t, conn2)

	// Close the pool
	err := pool.Close()

	// Validate
	assert.Nil(t, err)

	// Verify connections map is empty
	assert.Empty(t, pool.connections)
}

// TestConcurrentGetConnection tests concurrent access to GetConnection
func TestConcurrentGetConnection(t *testing.T) {
	mockIO := &MockNetworkIO{
		dialFunc: func(cfg ibabuza.TLSConfig, addr string) (net.Conn, error) {
			return NewMockConn(), nil
		},
	}

	options := defaultOptions
	options.MaxConnectionsPerHost = 10

	pool := NewConnectionPool(mockIO, ibabuza.TLSConfig{}, options)
	defer pool.Close()

	var wg sync.WaitGroup
	const numGoroutines = 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.GetConnection("test-addr")
			assert.Nil(t, err)
			assert.NotNil(t, conn)

			// Hold the connection briefly then return it
			time.Sleep(10 * time.Millisecond)
			pool.ReturnConnection(conn)
		}()
	}

	wg.Wait()

	// After all routines complete, check connection pool state
	pool.mu.RLock()
	conns := pool.connections["test-addr"]
	pool.mu.RUnlock()

	// Should have some connections in the pool
	assert.True(t, len(conns) > 0)

	// All connections should be idle (not in use)
	for _, conn := range conns {
		conn.mu.Lock()
		assert.False(t, conn.inUse)
		conn.mu.Unlock()
	}
}
