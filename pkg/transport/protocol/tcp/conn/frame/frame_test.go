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

package frame

import (
	"bytes"
	"io"
	"testing"

	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/stretchr/testify/assert"
)

// MockMessage implements the Message interface for testing
type MockMessage struct {
	Data []byte
}

func (m *MockMessage) MarshalTo(data []byte) (int, error) {
	copy(data, m.Data)
	return len(m.Data), nil
}

func (m *MockMessage) Size() int {
	return len(m.Data)
}

func (m *MockMessage) Unmarshal(data []byte) error {
	m.Data = make([]byte, len(data))
	copy(m.Data, data)
	return nil
}

func TestWriterAndReader(t *testing.T) {
	for _, tt := range []struct {
		name     string
		msgType  MessageType
		msgData  []byte
		bufSize  int
		wantErr  bool
		errorMsg string
	}{
		{
			name:    "small message",
			msgType: BatchMsgType,
			msgData: []byte("hello world"),
			bufSize: 100,
		},
		{
			name:    "zero length message",
			msgType: SnapshotMsgReqType,
			msgData: []byte{},
			bufSize: 100,
		},
		{
			name:    "message size equals buffer size",
			msgType: ClusterPeersReqType,
			msgData: bytes.Repeat([]byte("a"), 92),
			bufSize: 100,
		},
		{
			name:     "message size exceeds buffer size",
			msgType:  ClusterPeersResType,
			msgData:  bytes.Repeat([]byte("a"), 129), // must be greater than 128 to exceed allocator s
			bufSize:  100,                            // allocator size limit is 128
			wantErr:  true,
			errorMsg: "buffer size 128 is not enough for frame size",
		},
		{
			name:     "message size exceeds max message size",
			msgType:  PubAppServiceReqType,
			msgData:  bytes.Repeat([]byte("a"), MaxMessageSize+1),
			bufSize:  MaxMessageSize + HeaderSize + 10,
			wantErr:  true,
			errorMsg: "message size",
		},
	} {
		// Create a buffer for communication
		buf := new(bytes.Buffer)

		// Create writer and message
		writer := NewWriter(buf)
		msg := &MockMessage{Data: tt.msgData}

		// Write to buffer
		writeBuf := allocator.Acquire(tt.bufSize).Buffer
		err := writer.Encode(writeBuf, tt.msgType, msg)

		// Check write errors
		if tt.wantErr {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorMsg)
			return
		}

		assert.NoError(t, err)

		// Create reader
		reader := NewReader(buf)

		// Read back
		var readMsgType MessageType
		var readData []byte

		err = reader.ReadFrame(func(msgType MessageType, msgBuf []byte) error {
			readMsgType = msgType
			readData = make([]byte, len(msgBuf))
			copy(readData, msgBuf)
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, tt.msgType, readMsgType)
		assert.Equal(t, tt.msgData, readData)

	}
}

func TestCorruptedMessage(t *testing.T) {
	// Create a buffer for communication
	buf := new(bytes.Buffer)

	// Create writer and message
	writer := NewWriter(buf)
	msg := &MockMessage{Data: []byte("test data")}

	// Write to buffer
	writeBuf := allocator.Acquire(100).Buffer
	err := writer.Encode(writeBuf, BatchMsgType, msg)
	assert.NoError(t, err)

	// Corrupt the data by modifying the buffer
	bufData := buf.Bytes()
	bufData[HeaderSize] = bufData[HeaderSize] ^ 0xFF // Flip bits in first byte of message

	// Create reader with corrupted buffer
	reader := NewReader(bytes.NewBuffer(bufData))

	// Try to read the corrupted message
	err = reader.ReadFrame(func(msgType MessageType, msgBuf []byte) error {
		return nil
	})

	// Should get CRC error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crc does not match")
}

func TestMessageTypes(t *testing.T) {
	messageTypes := []MessageType{
		BatchMsgType,
		SnapshotMsgReqType,
		ClusterPeersReqType,
		ClusterPeersResType,
		PubAppServiceReqType,
		PubAppServiceResType,
	}

	for _, msgType := range messageTypes {
		t.Run(string(msgType), func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewWriter(buf)
			msg := &MockMessage{Data: []byte("type test")}

			writeBuf := allocator.Acquire(100).Buffer
			err := writer.Encode(writeBuf, msgType, msg)
			assert.NoError(t, err)
			reader := NewReader(buf)
			var readMsgType MessageType

			err = reader.ReadFrame(func(mType MessageType, msgBuf []byte) error {
				readMsgType = mType
				return nil
			})

			assert.NoError(t, err)
			assert.Equal(t, msgType, readMsgType)
		})
	}
}

func TestIncompleteRead(t *testing.T) {
	// Create a buffer with incomplete data
	buf := bytes.NewBuffer([]byte{1, 2, 3}) // Not enough bytes for a header

	reader := NewReader(buf)

	// Try to read
	err := reader.ReadFrame(func(msgType MessageType, msgBuf []byte) error {
		return nil
	})

	assert.Error(t, err)
}

func TestReadFrameOrEOFDistinguishesCleanAndPartialEOF(t *testing.T) {
	reader := NewReader(bytes.NewReader(nil))
	eof, err := reader.ReadFrameOrEOF(func(msgType MessageType, msgBuf []byte) error {
		return nil
	})
	assert.True(t, eof)
	assert.NoError(t, err)

	reader = NewReader(bytes.NewReader([]byte{1, 2, 3}))
	eof, err = reader.ReadFrameOrEOF(func(msgType MessageType, msgBuf []byte) error {
		return nil
	})
	assert.False(t, eof)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
