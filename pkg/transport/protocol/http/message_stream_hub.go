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

package http

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
)

var (
	errMessageStreamUnavailable  = errors.New("http message stream unavailable")
	errMessageStreamBackpressure = errors.New("http message stream backpressure")
)

const defaultMessageStreamBuffer = 256

type MessageStreamHub struct {
	mu      sync.RWMutex
	streams map[uint64]*outboundMessageStream
	buffer  int
}

type outboundMessageStream struct {
	peerID uint64
	frames chan []byte
	done   chan struct{}
	once   sync.Once
}

func NewMessageStreamHub() *MessageStreamHub {
	return newMessageStreamHub(defaultMessageStreamBuffer)
}

func newMessageStreamHub(buffer int) *MessageStreamHub {
	if buffer <= 0 {
		buffer = defaultMessageStreamBuffer
	}
	return &MessageStreamHub{
		streams: make(map[uint64]*outboundMessageStream),
		buffer:  buffer,
	}
}

func (h *MessageStreamHub) register(peerID uint64) *outboundMessageStream {
	stream := &outboundMessageStream{
		peerID: peerID,
		frames: make(chan []byte, h.buffer),
		done:   make(chan struct{}),
	}

	h.mu.Lock()
	old := h.streams[peerID]
	h.streams[peerID] = stream
	h.mu.Unlock()

	if old != nil {
		old.close()
	}
	return stream
}

func (h *MessageStreamHub) unregister(peerID uint64, stream *outboundMessageStream) {
	h.mu.Lock()
	if h.streams[peerID] == stream {
		delete(h.streams, peerID)
	}
	h.mu.Unlock()
	stream.close()
}

func (h *MessageStreamHub) closeAll() {
	h.mu.Lock()
	streams := h.streams
	h.streams = make(map[uint64]*outboundMessageStream)
	h.mu.Unlock()
	for _, stream := range streams {
		stream.close()
	}
}

func (h *MessageStreamHub) send(peerID uint64, batchMsg babuzapb.BatchMessage) error {
	h.mu.RLock()
	stream := h.streams[peerID]
	h.mu.RUnlock()
	if stream == nil {
		return errMessageStreamUnavailable
	}

	frameBytes, err := encodeMessageStreamFrame(frame.BatchMsgType, &batchMsg)
	if err != nil {
		return err
	}

	select {
	case <-stream.done:
		return errMessageStreamUnavailable
	default:
	}

	select {
	case <-stream.done:
		return errMessageStreamUnavailable
	case stream.frames <- frameBytes:
		return nil
	default:
		return errMessageStreamBackpressure
	}
}

func (s *outboundMessageStream) close() {
	s.once.Do(func() {
		close(s.done)
	})
}

func encodeMessageStreamFrame(msgType frame.MessageType, msg frame.Message) ([]byte, error) {
	bufSize := frame.EncodeSize(msg.Size())
	buf := make([]byte, bufSize)
	out := bytes.NewBuffer(make([]byte, 0, bufSize))
	if err := frame.NewWriter(out).Encode(buf, msgType, msg); err != nil {
		return nil, fmt.Errorf("failed to encode message stream frame: %w", err)
	}
	return out.Bytes(), nil
}
