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
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// faultTestConn is a mock net.Conn for fault writer testing
type faultTestConn struct {
	writeBuf bytes.Buffer
	mu       sync.Mutex
}

func (m *faultTestConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *faultTestConn) Write(b []byte) (n int, err error)  { m.mu.Lock(); defer m.mu.Unlock(); return m.writeBuf.Write(b) }
func (m *faultTestConn) Close() error                       { return nil }
func (m *faultTestConn) LocalAddr() net.Addr                { return nil }
func (m *faultTestConn) RemoteAddr() net.Addr               { return nil }
func (m *faultTestConn) SetDeadline(t time.Time) error      { return nil }
func (m *faultTestConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *faultTestConn) SetWriteDeadline(t time.Time) error { return nil }

func (m *faultTestConn) Written() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf.Bytes()
}

func TestFaultWriter_NoFault(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	// No SetFault called - should pass through

	data := []byte("hello world")
	n, err := fw.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, conn.Written())
}

func TestFaultWriter_PacketLoss_Full(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		LossRate: 1.0, // 100% loss
	})

	data := []byte("hello world")
	n, err := fw.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)  // Reports successful write
	assert.Empty(t, conn.Written()) // But nothing was written
}

func TestFaultWriter_PacketLoss_None(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		LossRate: 0.0, // No loss
	})

	data := []byte("hello world")
	n, err := fw.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, conn.Written())
}

func TestFaultWriter_PacketLoss_Statistical(t *testing.T) {
	// Test that ~50% loss rate results in approximately half the packets dropped
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		LossRate: 0.5,
	})

	iterations := 1000
	data := []byte("x")
	for i := 0; i < iterations; i++ {
		fw.Write(data)
	}

	written := len(conn.Written())
	// With 50% loss rate, expect between 40% and 60% to be written
	assert.Greater(t, written, iterations*40/100, "Too many packets dropped")
	assert.Less(t, written, iterations*60/100, "Too few packets dropped")
}

func TestFaultWriter_FixedDelay(t *testing.T) {
	conn := &faultTestConn{}
	delay := 50 * time.Millisecond
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		DelayMin: delay,
		DelayMax: delay,
	})

	data := []byte("hello")
	start := time.Now()
	n, err := fw.Write(data)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.GreaterOrEqual(t, elapsed, delay-5*time.Millisecond)
}

func TestFaultWriter_RandomDelay(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		DelayMin: 10 * time.Millisecond,
		DelayMax: 50 * time.Millisecond,
	})

	data := []byte("hello")
	start := time.Now()
	n, err := fw.Write(data)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.GreaterOrEqual(t, elapsed, 5*time.Millisecond)
	assert.LessOrEqual(t, elapsed, 60*time.Millisecond)
}

func TestFaultWriter_Reorder(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		ReorderBufferSize: 3,
	})

	// Write 3 packets (fills buffer, triggers flush)
	fw.Write([]byte("A"))
	fw.Write([]byte("B"))
	fw.Write([]byte("C"))

	// All data should be written after buffer fills
	written := conn.Written()
	assert.Len(t, written, 3)

	// Data contains A, B, C but may be in any order
	assert.Contains(t, string(written), "A")
	assert.Contains(t, string(written), "B")
	assert.Contains(t, string(written), "C")
}

func TestFaultWriter_Reorder_Buffering(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		ReorderBufferSize: 5,
	})

	// Write less than buffer size
	fw.Write([]byte("A"))
	fw.Write([]byte("B"))

	// Nothing written yet (buffered)
	assert.Empty(t, conn.Written())

	// ClearFault flushes remaining data
	fw.ClearFault()

	// Now data should be written
	written := conn.Written()
	assert.Len(t, written, 2)
}

func TestFaultWriter_Reorder_Disabled(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		ReorderBufferSize: 0, // Disabled
	})

	fw.Write([]byte("A"))
	fw.Write([]byte("B"))
	fw.Write([]byte("C"))

	// Data written in order immediately
	assert.Equal(t, "ABC", string(conn.Written()))
}

func TestFaultWriter_Close_FlushesBuffer(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		ReorderBufferSize: 5,
	})

	fw.Write([]byte("A"))
	fw.Write([]byte("B"))

	// Buffer not full, nothing written yet
	assert.Empty(t, conn.Written())

	// Close flushes remaining data
	err := fw.Close()
	assert.NoError(t, err)

	written := conn.Written()
	assert.Len(t, written, 2)
}

func TestFaultWriter_CombinedFaults(t *testing.T) {
	// With multiple fault types enabled, each write randomly selects one fault type
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		LossRate:          0.5,
		DelayMin:          5 * time.Millisecond,
		DelayMax:          10 * time.Millisecond,
		ReorderBufferSize: 3,
	})

	// Write multiple times - some may be dropped, delayed, or reordered
	for i := 0; i < 10; i++ {
		fw.Write([]byte("X"))
	}
	fw.ClearFault() // Flush reorder buffer

	// With random selection, we can't predict exact behavior,
	// but we should have some data written (not all dropped)
	written := conn.Written()
	assert.Greater(t, len(written), 0, "Some data should be written")
}

func TestFaultWriter_SetFaultAndClearFault(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)

	// Initially no fault
	assert.False(t, fw.IsFaultEnabled())

	// Set fault
	fw.SetFault(FaultConfig{
		LossRate: 1.0, // 100% loss
	})
	assert.True(t, fw.IsFaultEnabled())

	// Write should be dropped
	fw.Write([]byte("dropped"))
	assert.Empty(t, conn.Written())

	// Clear fault
	fw.ClearFault()
	assert.False(t, fw.IsFaultEnabled())

	// Write should succeed now
	fw.Write([]byte("success"))
	assert.Equal(t, "success", string(conn.Written()))
}

func TestFaultWriter_Reorder_FlushTimer(t *testing.T) {
	conn := &faultTestConn{}
	fw := newFaultWriter(conn)
	fw.SetFault(FaultConfig{
		ReorderBufferSize:    10, // Large buffer so it won't fill
		ReorderFlushInterval: 50 * time.Millisecond,
	})

	// Write data (won't fill buffer)
	fw.Write([]byte("A"))
	fw.Write([]byte("B"))

	// Nothing written yet (buffered)
	assert.Empty(t, conn.Written())

	// Wait for timer to flush
	time.Sleep(100 * time.Millisecond)

	// Data should be written after timer flush
	written := conn.Written()
	assert.Len(t, written, 2)
	assert.Contains(t, string(written), "A")
	assert.Contains(t, string(written), "B")
}
