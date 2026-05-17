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
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"hash/crc32"
	"io"
	"math/rand"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	defaultServerCfg = ServerConfig{
		WriteDeadline:   time.Second * 2,
		ReadDeadline:    time.Second * 2,
		ShutdownTimeout: time.Second * 2,
	}
)

type testMsg struct {
	batchMsg *babuzapb.BatchMessage
	snapMsg  *babuzapb.SnapshotMessage
}

type nodeMsg struct {
	nodeId          uint64
	batchMsg        map[uint64]raftpb.Message
	snapshotMsg     map[uint64]babuzapb.SnapshotMessage
	receiveMsgCount int
	totalMsgCount   int
}

func (m *nodeMsg) matchBatchMessage(matchMsgs []raftpb.Message) bool {
	for _, msg := range matchMsgs {
		_, ok := m.batchMsg[msg.Index]
		if !ok {
			return false
		}
	}
	return true
}

func (m *nodeMsg) check(t *testing.T, identify string, tms []*testMsg) {
	for _, tm := range tms {
		if tm.batchMsg != nil {
			for _, msg := range tm.batchMsg.Messages {
				rm, ok := m.batchMsg[msg.Index]
				assert.Equal(t, true, ok, identify)
				assert.EqualValues(t, msg, rm, identify)
			}
		} else if tm.snapMsg != nil {
			snapMsg, ok := m.snapshotMsg[tm.snapMsg.Index]
			assert.Equal(t, true, ok, identify)
			assert.EqualValues(t, *tm.snapMsg, snapMsg, identify)
		}
	}
}

type mockTransportRaft struct {
	nodesMsg         map[uint64]*nodeMsg
	notifyNodeDoneCh chan *nodeMsg
	clusterRes       babuzapb.GetClusterPeersResponse
	publishRes       babuzapb.PublishApplicationServiceResponse
	mu               sync.Mutex
}

func (m *mockTransportRaft) ProcessMultiRaftMessage(message babuzapb.MultiRaftBatchMessage) {
	// not supported
}

func newMockTransportRaft(nodes int) *mockTransportRaft {
	return &mockTransportRaft{
		nodesMsg:         make(map[uint64]*nodeMsg),
		notifyNodeDoneCh: make(chan *nodeMsg, nodes),
	}
}
func (m *mockTransportRaft) setupMsgCount(node uint64, msgCount int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodesMsg[node] = &nodeMsg{
		nodeId:        node,
		batchMsg:      make(map[uint64]raftpb.Message),
		snapshotMsg:   make(map[uint64]babuzapb.SnapshotMessage),
		totalMsgCount: msgCount,
	}
}

func (m *mockTransportRaft) ProcessBatchMessage(message babuzapb.BatchMessage) {
	m.mu.Lock()
	nodeId := message.Messages[0].From
	n := m.nodesMsg[nodeId]
	for _, msg := range message.Messages {
		n.batchMsg[msg.Index] = msg
	}
	n.receiveMsgCount++
	if n.receiveMsgCount == n.totalMsgCount {
		delete(m.nodesMsg, nodeId)
		m.notifyNodeDoneCh <- n
	}
	m.mu.Unlock()

}

func (m *mockTransportRaft) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	m.mu.Lock()
	n := m.nodesMsg[message.From]
	n.snapshotMsg[message.Index] = message
	n.receiveMsgCount++
	if n.receiveMsgCount == n.totalMsgCount {
		delete(m.nodesMsg, message.From)
		m.notifyNodeDoneCh <- n
	}
	m.mu.Unlock()
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
	}

}
func (m *mockTransportRaft) GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return m.clusterRes
}

func (m *mockTransportRaft) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return m.publishRes
}

func dial(cfg ibabuza.TLSConfig, endpoint string) (net.Conn, error) {
	tlsCfg, err := netutil.GetClientTlsConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return net.Dial("tcp", endpoint)
	}
	return tls.Dial("tcp", endpoint, tlsCfg)
}

// MockTransportResolver is a simple implementation of ibabuza.TransportResolver for testing
type MockTransportResolver struct {
	addressMap map[uint64]string
}

func NewMockTransportResolver() *MockTransportResolver {
	return &MockTransportResolver{
		addressMap: make(map[uint64]string),
	}
}

func (m *MockTransportResolver) ResolvePeerAddress(peerID uint64) (string, error) {
	if addr, ok := m.addressMap[peerID]; ok {
		return addr, nil
	}
	return "localhost:14200", nil // Default for testing
}

type streamTestRaft struct {
	processed chan babuzapb.BatchMessage
	count     atomic.Int64
}

func newStreamTestRaft(buffer int) *streamTestRaft {
	return &streamTestRaft{
		processed: make(chan babuzapb.BatchMessage, buffer),
	}
}

func (m *streamTestRaft) ProcessBatchMessage(message babuzapb.BatchMessage) {
	m.count.Add(1)
	m.processed <- message
}

func (m *streamTestRaft) ProcessSnapshotMessage(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	return babuzapb.SnapshotMessageResponse{}
}

func (m *streamTestRaft) GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (m *streamTestRaft) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

type snapshotStreamTestRaft struct {
	processed  chan babuzapb.SnapshotMessage
	count      atomic.Int64
	responseFn func(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse
}

func newSnapshotStreamTestRaft(buffer int) *snapshotStreamTestRaft {
	return &snapshotStreamTestRaft{
		processed: make(chan babuzapb.SnapshotMessage, buffer),
	}
}

func (m *snapshotStreamTestRaft) ProcessBatchMessage(babuzapb.BatchMessage) {
}

func (m *snapshotStreamTestRaft) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	m.count.Add(1)
	m.processed <- message
	if m.responseFn != nil {
		return m.responseFn(message)
	}
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: message.Type.String(),
	}
}

func (m *snapshotStreamTestRaft) GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (m *snapshotStreamTestRaft) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

type benchmarkRaft struct {
	count atomic.Int64
}

func (m *benchmarkRaft) ProcessBatchMessage(message babuzapb.BatchMessage) {
	m.count.Add(1)
}

func (m *benchmarkRaft) ProcessSnapshotMessage(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	m.count.Add(1)
	return babuzapb.SnapshotMessageResponse{Status: babuzapb.SUCCESS}
}

func (m *benchmarkRaft) GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (m *benchmarkRaft) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func testBatchMessage(from, to, index uint64) babuzapb.BatchMessage {
	return babuzapb.BatchMessage{
		Messages: []raftpb.Message{
			{
				From:  from,
				To:    to,
				Index: index,
			},
		},
	}
}

func testSnapshotMessage(from, to, index uint64, typ babuzapb.SnapshotMessageType) babuzapb.SnapshotMessage {
	return babuzapb.SnapshotMessage{
		From:  from,
		To:    to,
		Index: index,
		Type:  typ,
		ChunkMessage: babuzapb.SnapshotChunkMessage{
			FileTag: "default",
			Data:    []byte("chunk"),
		},
	}
}

func startHTTPMessageTestServer(t testing.TB, streamEnabled bool, tlsEnabled bool, raft ibabuza.RaftMessageHandler) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	shortCount := &atomic.Int64{}
	streamCount := &atomic.Int64{}
	h := &handler{
		raft: raft,
		config: ServerConfig{
			MessageStreamEnabled: streamEnabled,
			StreamIdleTimeout:    time.Second,
		},
	}
	mux := stdhttp.NewServeMux()
	mux.HandleFunc(raftBatchMsgPrefix, func(w stdhttp.ResponseWriter, req *stdhttp.Request) {
		shortCount.Add(1)
		h.batchMessageFunc(w, req)
	})
	mux.HandleFunc(raftBatchMsgStreamPrefix, func(w stdhttp.ResponseWriter, req *stdhttp.Request) {
		streamCount.Add(1)
		h.batchMessageStreamFunc(w, req)
	})
	srv := httptest.NewUnstartedServer(mux)
	if tlsEnabled {
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv, shortCount, streamCount
}

func startHTTPSnapshotTestServer(t testing.TB, streamEnabled bool, raft ibabuza.RaftMessageHandler) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	shortCount := &atomic.Int64{}
	streamCount := &atomic.Int64{}
	h := &handler{
		raft: raft,
		config: ServerConfig{
			MessageStreamEnabled:      streamEnabled,
			StreamIdleTimeout:         time.Second,
			SnapshotStreamIdleTimeout: time.Second,
		},
	}
	mux := stdhttp.NewServeMux()
	mux.HandleFunc(raftSnapshotMsgPrefix, func(w stdhttp.ResponseWriter, req *stdhttp.Request) {
		shortCount.Add(1)
		h.snapshotMessageFunc(w, req)
	})
	mux.HandleFunc(raftSnapshotStreamPrefix, func(w stdhttp.ResponseWriter, req *stdhttp.Request) {
		streamCount.Add(1)
		h.snapshotMessageStreamFunc(w, req)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, shortCount, streamCount
}

func TestHTTPBatchMessageStreamDisabledUsesShortRequest(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, shortCount, streamCount := startHTTPMessageTestServer(t, false, false, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false)
	defer client.Close()

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	assert.EqualValues(t, 1, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())

	select {
	case msg := <-raft.processed:
		assert.EqualValues(t, 10, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for short request batch")
	}
}

func TestHTTPBatchMessageStreamEnabledSendsMultipleFrames(t *testing.T) {
	raft := newStreamTestRaft(2)
	srv, shortCount, streamCount := startHTTPMessageTestServer(t, true, false, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 11)))
	assert.NoError(t, client.Close())

	got := make(map[uint64]bool)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-raft.processed:
			got[msg.Messages[0].Index] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for streamed batch")
		}
	}
	assert.True(t, got[10])
	assert.True(t, got[11])
	assert.EqualValues(t, 0, shortCount.Load())
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPBatchMessageStreamTLS(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, _, streamCount := startHTTPMessageTestServer(t, true, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, true, ServerConfig{MessageStreamEnabled: true})

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	assert.NoError(t, client.Close())
	select {
	case <-raft.processed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TLS streamed batch")
	}
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPBatchMessageStreamRecreatesAfterResponseFailure(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, _, streamCount := startHTTPMessageTestServer(t, true, false, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	_, staleBody := io.Pipe()
	defer staleBody.Close()
	staleStream := &messageStream{
		body:   staleBody,
		doneCh: make(chan error, 1),
	}
	staleStream.doneCh <- fmt.Errorf("previous stream failed")
	client.stream = staleStream

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 11)))
	assert.NoError(t, client.Close())
	select {
	case msg := <-raft.processed:
		assert.EqualValues(t, 11, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recreated stream batch")
	}
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPBatchMessageStreamRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		body func(t *testing.T) io.Reader
	}{
		{
			name: "unsupported type",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader(encodeFrameForTest(t, frame.SnapshotMsgReqType, &babuzapb.SnapshotMessage{}))
			},
		},
		{
			name: "crc mismatch",
			body: func(t *testing.T) io.Reader {
				msg := testBatchMessage(1, 2, 10)
				buf := encodeFrameForTest(t, frame.BatchMsgType, &msg)
				buf[len(buf)-1] ^= 0xff
				return bytes.NewReader(buf)
			},
		},
		{
			name: "empty batch",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader(encodeFrameForTest(t, frame.BatchMsgType, &babuzapb.BatchMessage{}))
			},
		},
		{
			name: "partial header",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader([]byte{1, 2, 3})
			},
		},
		{
			name: "partial body",
			body: func(t *testing.T) io.Reader {
				msg := testBatchMessage(1, 2, 10)
				buf := encodeFrameForTest(t, frame.BatchMsgType, &msg)
				return bytes.NewReader(buf[:len(buf)-1])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raft := newStreamTestRaft(1)
			srv, _, _ := startHTTPMessageTestServer(t, true, false, raft)
			res, err := srv.Client().Post(srv.URL+raftBatchMsgStreamPrefix, "application/octet-stream", tt.body(t))
			assert.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, stdhttp.StatusBadRequest, res.StatusCode)
			assert.EqualValues(t, 0, raft.count.Load())
		})
	}
}

func TestHTTPSnapshotStreamDisabledUsesShortRequest(t *testing.T) {
	raft := newSnapshotStreamTestRaft(3)
	srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, false, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false)
	defer client.Close()

	msgs := []babuzapb.SnapshotMessage{
		testSnapshotMessage(1, 2, 10, babuzapb.SnapshotMessageType_Metadata),
		testSnapshotMessage(1, 2, 10, babuzapb.SnapshotMessageType_Chunk),
		testSnapshotMessage(1, 2, 10, babuzapb.SnapshotMessageType_Finish),
	}
	for _, msg := range msgs {
		res, err := client.SendSnapshotMessage(msg)
		assert.NoError(t, err)
		assert.Equal(t, babuzapb.SUCCESS, res.Status)
	}
	assert.EqualValues(t, 3, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())
	assert.EqualValues(t, 3, raft.count.Load())
}

func TestHTTPSnapshotStreamEnabledSendsTransferOnOneStream(t *testing.T) {
	raft := newSnapshotStreamTestRaft(3)
	srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	res, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Metadata))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.SUCCESS, res.Status)

	res, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Chunk))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.SUCCESS, res.Status)

	res, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Finish))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.SUCCESS, res.Status)
	assert.Equal(t, babuzapb.SnapshotMessageType_Finish.String(), res.Message)

	got := make(map[babuzapb.SnapshotMessageType]bool)
	for i := 0; i < 3; i++ {
		select {
		case msg := <-raft.processed:
			got[msg.Type] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for streamed snapshot")
		}
	}
	assert.True(t, got[babuzapb.SnapshotMessageType_Metadata])
	assert.True(t, got[babuzapb.SnapshotMessageType_Chunk])
	assert.True(t, got[babuzapb.SnapshotMessageType_Finish])
	assert.EqualValues(t, 0, shortCount.Load())
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPSnapshotStreamReturnsProcessorFailureOnFinish(t *testing.T) {
	raft := newSnapshotStreamTestRaft(3)
	raft.responseFn = func(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
		if message.Type == babuzapb.SnapshotMessageType_Finish {
			return babuzapb.SnapshotMessageResponse{Status: babuzapb.FAILED, Message: "finish failed"}
		}
		return babuzapb.SnapshotMessageResponse{Status: babuzapb.SUCCESS}
	}
	srv, _, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	res, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 12, babuzapb.SnapshotMessageType_Metadata))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.SUCCESS, res.Status)
	res, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 12, babuzapb.SnapshotMessageType_Chunk))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.SUCCESS, res.Status)
	res, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 12, babuzapb.SnapshotMessageType_Finish))
	assert.NoError(t, err)
	assert.Equal(t, babuzapb.FAILED, res.Status)
	assert.Equal(t, "finish failed", res.Message)
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPSnapshotStreamRejectsConcurrentMetadata(t *testing.T) {
	raft := newSnapshotStreamTestRaft(2)
	srv, _, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	_, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 13, babuzapb.SnapshotMessageType_Metadata))
	assert.NoError(t, err)
	_, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 14, babuzapb.SnapshotMessageType_Metadata))
	assert.Error(t, err)
	_, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 13, babuzapb.SnapshotMessageType_Finish))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPSnapshotStreamRejectsMalformedFrames(t *testing.T) {
	tests := []struct {
		name string
		body func(t *testing.T) io.Reader
	}{
		{
			name: "unsupported type",
			body: func(t *testing.T) io.Reader {
				msg := testBatchMessage(1, 2, 10)
				return bytes.NewReader(encodeFrameForTest(t, frame.BatchMsgType, &msg))
			},
		},
		{
			name: "crc mismatch",
			body: func(t *testing.T) io.Reader {
				msg := testSnapshotMessage(1, 2, 10, babuzapb.SnapshotMessageType_Metadata)
				buf := encodeFrameForTest(t, frame.SnapshotMsgReqType, &msg)
				buf[len(buf)-1] ^= 0xff
				return bytes.NewReader(buf)
			},
		},
		{
			name: "malformed protobuf",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader(encodeRawFrameForTest(frame.SnapshotMsgReqType, []byte{0xff}))
			},
		},
		{
			name: "empty stream",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader(nil)
			},
		},
		{
			name: "partial header",
			body: func(t *testing.T) io.Reader {
				return bytes.NewReader([]byte{1, 2, 3})
			},
		},
		{
			name: "partial body",
			body: func(t *testing.T) io.Reader {
				msg := testSnapshotMessage(1, 2, 10, babuzapb.SnapshotMessageType_Metadata)
				buf := encodeFrameForTest(t, frame.SnapshotMsgReqType, &msg)
				return bytes.NewReader(buf[:len(buf)-1])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raft := newSnapshotStreamTestRaft(1)
			srv, _, _ := startHTTPSnapshotTestServer(t, true, raft)
			res, err := srv.Client().Post(srv.URL+raftSnapshotStreamPrefix, "application/octet-stream", tt.body(t))
			assert.NoError(t, err)
			defer res.Body.Close()
			assert.Equal(t, stdhttp.StatusBadRequest, res.StatusCode)
			assert.EqualValues(t, 0, raft.count.Load())
		})
	}
}

func TestNewSnapshotStreamClientLimitsConnectionsPerHost(t *testing.T) {
	client, err := NewSnapshotStreamClient(ibabuza.TLSConfig{}, ServerConfig{})
	assert.NoError(t, err)
	transport, ok := client.Transport.(*stdhttp.Transport)
	assert.True(t, ok)
	assert.Equal(t, 1, transport.MaxConnsPerHost)
	assert.Equal(t, 1, transport.MaxIdleConnsPerHost)
	transport.CloseIdleConnections()
}

func TestHandlerRequestReadTimeoutSelection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	cfg := ServerConfig{
		ReadDeadline:              time.Second,
		StreamIdleTimeout:         2 * time.Second,
		SnapshotStreamIdleTimeout: 3 * time.Second,
	}
	conn := newConnection(serverConn, cfg.ReadDeadline, cfg.WriteDeadline)
	req := httptest.NewRequest(stdhttp.MethodPost, raftSnapshotStreamPrefix, nil)
	req = req.WithContext(context.WithValue(req.Context(), serverConnectionContextKey{}, conn))

	h := &handler{config: cfg}
	restore := h.withRequestReadTimeout(req, cfg.SnapshotStreamIdleTimeout)
	assert.Equal(t, cfg.SnapshotStreamIdleTimeout, conn.ReadTimeout())
	restore()
	assert.Equal(t, cfg.ReadDeadline, conn.ReadTimeout())

	restore = h.withRequestReadTimeout(req, cfg.StreamIdleTimeout)
	assert.Equal(t, cfg.StreamIdleTimeout, conn.ReadTimeout())
	restore()
	assert.Equal(t, cfg.ReadDeadline, conn.ReadTimeout())

	restore = h.withRequestReadTimeout(req, 0)
	assert.Equal(t, cfg.ReadDeadline, conn.ReadTimeout())
	restore()
	assert.Equal(t, cfg.ReadDeadline, conn.ReadTimeout())
}

func encodeFrameForTest(t *testing.T, msgType frame.MessageType, msg frame.Message) []byte {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	writer := frame.NewWriter(buf)
	byteSlice := allocator.Acquire(frame.EncodeSize(msg.Size()))
	defer allocator.Release(byteSlice)
	assert.NoError(t, writer.Encode(byteSlice.Buffer, msgType, msg))
	return append([]byte(nil), buf.Bytes()...)
}

func encodeRawFrameForTest(msgType frame.MessageType, payload []byte) []byte {
	buf := make([]byte, frame.HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:frame.CrcOffset], uint32((len(payload)<<frame.MsgSizeShift)|(int(msgType&frame.MsgTypeMask))))
	binary.LittleEndian.PutUint32(buf[frame.CrcOffset:frame.HeaderSize], crc32.Checksum(payload, frame.Crc32Table))
	copy(buf[frame.HeaderSize:], payload)
	return buf
}

func BenchmarkHTTPBatchMessageShortRequest(b *testing.B) {
	raft := &benchmarkRaft{}
	srv, _, _ := startHTTPMessageTestServer(b, false, false, raft)
	defer srv.Close()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false)
	defer client.Close()
	msg := testBatchMessage(1, 2, 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Messages[0].Index = uint64(i + 1)
		if err := client.SendBatchMessage(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTPBatchMessageStream(b *testing.B) {
	raft := &benchmarkRaft{}
	srv, _, _ := startHTTPMessageTestServer(b, true, false, raft)
	defer srv.Close()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	msg := testBatchMessage(1, 2, 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Messages[0].Index = uint64(i + 1)
		if err := client.SendBatchMessage(msg); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := client.Close(); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for raft.count.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if raft.count.Load() != int64(b.N) {
		b.Fatalf("processed %d messages, want %d", raft.count.Load(), b.N)
	}
}

func BenchmarkHTTPSnapshotShortRequestSmallChunks(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, false, 32, 256)
}

func BenchmarkHTTPSnapshotStreamSmallChunks(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, true, 32, 256)
}

func BenchmarkHTTPSnapshotShortRequestLargeChunks(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, false, 4, 8192)
}

func BenchmarkHTTPSnapshotStreamLargeChunks(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, true, 4, 8192)
}

func benchmarkHTTPSnapshotTransfer(b *testing.B, streamEnabled bool, chunkCount int, chunkSize int) {
	raft := &benchmarkRaft{}
	srv, _, _ := startHTTPSnapshotTestServer(b, streamEnabled, raft)
	defer srv.Close()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: streamEnabled})
	defer client.Close()

	chunkData := bytes.Repeat([]byte("x"), chunkSize)
	metadata := testSnapshotMessage(1, 2, 1, babuzapb.SnapshotMessageType_Metadata)
	chunk := testSnapshotMessage(1, 2, 1, babuzapb.SnapshotMessageType_Chunk)
	chunk.ChunkMessage.Data = chunkData
	finish := testSnapshotMessage(1, 2, 1, babuzapb.SnapshotMessageType_Finish)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := uint64(i + 1)
		metadata.Index = index
		chunk.Index = index
		finish.Index = index
		if _, err := client.SendSnapshotMessage(metadata); err != nil {
			b.Fatal(err)
		}
		for chunkID := 0; chunkID < chunkCount; chunkID++ {
			chunk.ChunkMessage.Id = int64(chunkID)
			chunk.ChunkMessage.LastChunk = chunkID == chunkCount-1
			if _, err := client.SendSnapshotMessage(chunk); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := client.SendSnapshotMessage(finish); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSingleServerClient_SendAndReceive(t *testing.T) {
	type testCase struct {
		ibabuza.TransportConfig
		clientTls         ibabuza.TLSConfig
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/server.pem",
					TLSKey:    "../../../../test/fixtures/server-key.pem",
					TLSRootCA: "../../../../test/fixtures/root.pem",
				},
			},
			clientTls: ibabuza.TLSConfig{
				EnableTLS: true,
				MutualTLS: false,
				TLSCert:   "../../../../test/fixtures/client.pem",
				TLSKey:    "../../../../test/fixtures/client-key.pem",
				TLSRootCA: "../../../../test/fixtures/root.pem",
			},
			totalMsgCount:     256,
			batchRaftMsgCount: 64,
		},
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: true,
					TLSCert:   "../../../../test/fixtures/server.pem",
					TLSKey:    "../../../../test/fixtures/server-key.pem",
					TLSRootCA: "../../../../test/fixtures/root.pem",
				},
			},
			clientTls: ibabuza.TLSConfig{
				EnableTLS: true,
				MutualTLS: true,
				TLSCert:   "../../../../test/fixtures/client.pem",
				TLSKey:    "../../../../test/fixtures/client-key.pem",
				TLSRootCA: "../../../../test/fixtures/root.pem",
			},
			totalMsgCount:     512,
			batchRaftMsgCount: 64,
		},
	}
	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		mr := newMockTransportRaft(1)
		srv := NewRaftMsgServer(c.TransportConfig, defaultServerCfg, mr, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)
		httpClient, err := NewClient(c.clientTls, defaultServerCfg)
		assert.Nil(t, err, identify)

		resolver := NewMockTransportResolver()
		client := NewRaftMsgClient(httpClient, resolver, c.EnableTLS)

		tms := genTestMsg(c.totalMsgCount, c.batchRaftMsgCount, 1)
		mr.setupMsgCount(1, len(tms))
		for index, tm := range tms {
			if tm.batchMsg != nil {
				assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
			} else if tm.snapMsg != nil {
				_, err = client.SendSnapshotMessage(*tm.snapMsg)
				assert.Nil(t, err, identify)
			}

			res := babuzapb.GetClusterPeersResponse{
				Peers: []babuzapb.Peer{
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							PeerID:         uint64(index),
							RaftListenAddr: "localhost:14200",
							IsLearner:      false,
						},
					},
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							PeerID:         uint64(index + 1),
							RaftListenAddr: "localhost:14201",
							IsLearner:      true,
						},
					},
				},
			}
			mr.clusterRes = res
			getRes, _ := client.GetClusterPeers(babuzapb.GetClusterPeersRequest{ClusterID: 100})
			assert.Equal(t, res, getRes)
		}
		nodeDoneMsg := <-mr.notifyNodeDoneCh
		nodeDoneMsg.check(t, identify, tms)
		client.Close()
		assert.Nil(t, srv.Stop())
	}
}

func TestSingleServerMultiClient_SendAndReceive(t *testing.T) {
	type testCase struct {
		ibabuza.TransportConfig
		clientTls         ibabuza.TLSConfig
		clients           int
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			clients:           8,
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/server.pem",
					TLSKey:    "../../../../test/fixtures/server-key.pem",
					TLSRootCA: "../../../../test/fixtures/root.pem",
				},
			},
			clientTls: ibabuza.TLSConfig{
				EnableTLS: true,
				MutualTLS: false,
				TLSCert:   "../../../../test/fixtures/client.pem",
				TLSKey:    "../../../../test/fixtures/client-key.pem",
				TLSRootCA: "../../../../test/fixtures/root.pem",
			},
			clients:           16,
			totalMsgCount:     256,
			batchRaftMsgCount: 64,
		},
		{
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/server.pem",
					TLSKey:    "../../../../test/fixtures/server-key.pem",
					TLSRootCA: "../../../../test/fixtures/root.pem",
				},
			},
			clientTls: ibabuza.TLSConfig{
				EnableTLS: true,
				MutualTLS: true,
				TLSCert:   "../../../../test/fixtures/client.pem",
				TLSKey:    "../../../../test/fixtures/client-key.pem",
				TLSRootCA: "../../../../test/fixtures/root.pem",
			},
			clients:           32,
			totalMsgCount:     512,
			batchRaftMsgCount: 64,
		},
	}
	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		mr := newMockTransportRaft(c.clients)
		srv := NewRaftMsgServer(c.TransportConfig, defaultServerCfg, mr, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)
		allTms := make(map[int][]*testMsg)
		wg := new(sync.WaitGroup)
		for n := 1; n <= c.clients; n++ {
			allTms[n] = genTestMsg(c.totalMsgCount, c.batchRaftMsgCount, uint64(n))
			mr.setupMsgCount(uint64(n), len(allTms[n]))
		}

		for n := 1; n <= c.clients; n++ {
			wg.Add(1)
			go func(tms []*testMsg) {
				defer wg.Done()
				httpClient, err := NewClient(c.clientTls, defaultServerCfg)
				assert.Nil(t, err, identify)
				resolver := NewMockTransportResolver()
				client := NewRaftMsgClient(httpClient, resolver, c.EnableTLS)
				defer client.Close()
				for _, tm := range tms {
					if tm.batchMsg != nil {
						assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
					} else if tm.snapMsg != nil {
						_, err = client.SendSnapshotMessage(*tm.snapMsg)
						assert.Nil(t, err, identify)
					}
				}
			}(allTms[n])
		}
		wg.Wait()
		for n := 1; n <= c.clients; n++ {
			nodeDoneMsg := <-mr.notifyNodeDoneCh
			nodeDoneMsg.check(t, identify, allTms[int(nodeDoneMsg.nodeId)])
		}
		assert.Nil(t, srv.Stop())
	}
}

func genTestMsg(totalMsgs, maxRaftMsgs int, fromNode uint64) []*testMsg {
	r := make([]*testMsg, totalMsgs)
	var startIndex uint64 = 1
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < totalMsgs; i++ {
		isBatch := rand.Intn(100)%2 == 0
		if isBatch {
			msgs := genRaftMsg(maxRaftMsgs, startIndex, fromNode)
			startIndex = msgs[len(msgs)-1].Index + 1
			r[i] = &testMsg{
				batchMsg: &babuzapb.BatchMessage{
					Messages: msgs,
				},
			}
		} else {
			startIndex += uint64(i)
			r[i] = &testMsg{
				snapMsg: &babuzapb.SnapshotMessage{
					From:  fromNode,
					Index: startIndex,
				},
			}
		}
	}
	return r
}
func genRaftMsg(maxMsgs int, startIndex, fromNode uint64) []raftpb.Message {
	r := make([]raftpb.Message, maxMsgs)
	for i := 0; i < maxMsgs; i++ {
		r[i] = raftpb.Message{
			From:  fromNode,
			Index: startIndex + uint64(i),
		}
	}
	return r
}
