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
	"math/rand"
	"net"
	"sync"
	"time"
)

// Fault type constants
const (
	faultLoss = iota
	faultDelay
	faultReorder
)

type FaultConfig struct {
	// LossRate is the probability of dropping a packet (0.0-1.0).
	LossRate float64
	// DelayMin is the minimum delay to add before forwarding.
	DelayMin time.Duration
	// DelayMax is the maximum delay to add before forwarding.
	DelayMax time.Duration
	// ReorderBufferSize is the buffer size for out-of-order delivery.
	// Set to 0 to disable reordering.
	ReorderBufferSize int
	// ReorderFlushInterval is the interval to flush the reorder buffer.
	// Set to 0 to disable time-based flushing (only flush when buffer is full).
	ReorderFlushInterval time.Duration
}

type faultWriter struct {
	conn net.Conn

	mu           sync.RWMutex
	faultEnabled bool
	config       FaultConfig
	rng          *rand.Rand
	faultTypes   []int

	// reorder buffer
	reorderBuf  [][]byte
	flushTimer  *time.Timer
	stopFlushCh chan struct{}
}

func newFaultWriter(conn net.Conn) *faultWriter {
	return &faultWriter{
		conn: conn,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (fw *faultWriter) SetFault(config FaultConfig) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Stop any existing flush timer
	fw.stopFlushTimer()

	fw.config = config
	fw.faultEnabled = true
	fw.reorderBuf = make([][]byte, 0, config.ReorderBufferSize)

	// Compute enabled fault types
	fw.faultTypes = nil
	if config.LossRate > 0 {
		fw.faultTypes = append(fw.faultTypes, faultLoss)
	}
	if config.DelayMax > 0 {
		fw.faultTypes = append(fw.faultTypes, faultDelay)
	}
	if config.ReorderBufferSize > 0 {
		fw.faultTypes = append(fw.faultTypes, faultReorder)
	}

	// Start flush timer if interval is set and reorder is enabled
	if config.ReorderFlushInterval > 0 && config.ReorderBufferSize > 0 {
		fw.stopFlushCh = make(chan struct{})
		fw.startFlushTimer()
	}
}

func (fw *faultWriter) stopFlushTimer() {
	if fw.flushTimer != nil {
		fw.flushTimer.Stop()
		fw.flushTimer = nil
	}
	if fw.stopFlushCh != nil {
		close(fw.stopFlushCh)
		fw.stopFlushCh = nil
	}
}

func (fw *faultWriter) startFlushTimer() {
	fw.flushTimer = time.AfterFunc(fw.config.ReorderFlushInterval, func() {
		fw.mu.Lock()
		defer fw.mu.Unlock()

		// Check if still enabled
		select {
		case <-fw.stopFlushCh:
			return
		default:
		}
		// Flush the reorder buffer
		if len(fw.reorderBuf) > 0 {
			_ = fw.flushReorderBufferOnly()
		}
		// Restart the timer if still enabled
		if fw.faultEnabled && fw.config.ReorderFlushInterval > 0 {
			fw.flushTimer.Reset(fw.config.ReorderFlushInterval)
		}
	})
}

func (fw *faultWriter) ClearFault() {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Stop the flush timer
	fw.stopFlushTimer()

	// Flush any remaining reorder buffer
	if len(fw.reorderBuf) > 0 {
		for _, buf := range fw.reorderBuf {
			_, _ = fw.conn.Write(buf)
		}
		fw.reorderBuf = nil
	}
	fw.faultEnabled = false
	fw.config = FaultConfig{}
	fw.faultTypes = nil
}

func (fw *faultWriter) IsFaultEnabled() bool {
	fw.mu.RLock()
	defer fw.mu.RUnlock()
	return fw.faultEnabled
}

func (fw *faultWriter) Read(b []byte) (n int, err error) {
	return fw.conn.Read(b)
}

func (fw *faultWriter) Write(b []byte) (n int, err error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if !fw.faultEnabled {
		return fw.conn.Write(b)
	}

	return fw.writeWithFault(b)
}

func (fw *faultWriter) writeWithFault(data []byte) (int, error) {
	// First, check if reorder buffer needs flushing
	if fw.config.ReorderBufferSize > 0 && len(fw.reorderBuf) >= fw.config.ReorderBufferSize {
		if err := fw.flushReorderBufferOnly(); err != nil {
			return 0, err
		}
	}

	// No faults enabled
	if len(fw.faultTypes) == 0 {
		return fw.conn.Write(data)
	}

	// Randomly select one fault type to apply
	selectedFault := fw.faultTypes[fw.rng.Intn(len(fw.faultTypes))]

	switch selectedFault {
	case faultLoss:
		if fw.rng.Float64() < fw.config.LossRate {
			// Drop the packet
			return len(data), nil
		}
	case faultDelay:
		delay := fw.config.DelayMin
		if fw.config.DelayMax > fw.config.DelayMin {
			jitter := fw.rng.Int63n(int64(fw.config.DelayMax - fw.config.DelayMin))
			delay += time.Duration(jitter)
		}
		time.Sleep(delay)
	case faultReorder:
		// Add to reorder buffer
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		fw.reorderBuf = append(fw.reorderBuf, dataCopy)
		// Flush when buffer reaches capacity
		if len(fw.reorderBuf) >= fw.config.ReorderBufferSize {
			if err := fw.flushReorderBufferOnly(); err != nil {
				return 0, err
			}
		}
		return len(data), nil
	}

	return fw.conn.Write(data)
}

func (fw *faultWriter) flushReorderBufferOnly() error {
	if len(fw.reorderBuf) == 0 {
		return nil
	}

	for i := len(fw.reorderBuf) - 1; i > 0; i-- {
		j := fw.rng.Intn(i + 1)
		fw.reorderBuf[i], fw.reorderBuf[j] = fw.reorderBuf[j], fw.reorderBuf[i]
	}

	// Write all buffered data
	for _, buf := range fw.reorderBuf {
		if _, err := fw.conn.Write(buf); err != nil {
			fw.reorderBuf = fw.reorderBuf[:0]
			return err
		}
	}
	fw.reorderBuf = fw.reorderBuf[:0]
	return nil
}

func (fw *faultWriter) Close() error {
	fw.mu.Lock()
	fw.stopFlushTimer()
	// Flush any remaining reorder buffer
	if len(fw.reorderBuf) > 0 {
		for _, buf := range fw.reorderBuf {
			_, _ = fw.conn.Write(buf)
		}
		fw.reorderBuf = nil
	}
	fw.mu.Unlock()
	return fw.conn.Close()
}

func (fw *faultWriter) LocalAddr() net.Addr {
	return fw.conn.LocalAddr()
}

func (fw *faultWriter) RemoteAddr() net.Addr {
	return fw.conn.RemoteAddr()
}

func (fw *faultWriter) SetDeadline(t time.Time) error {
	return fw.conn.SetDeadline(t)
}

func (fw *faultWriter) SetReadDeadline(t time.Time) error {
	return fw.conn.SetReadDeadline(t)
}

func (fw *faultWriter) SetWriteDeadline(t time.Time) error {
	return fw.conn.SetWriteDeadline(t)
}
