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
	"github.com/fanaujie/babuza/pkg/transport/internal/testutil"
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

type blockingFinishStreamRaft struct {
	batchProcessed    chan babuzapb.BatchMessage
	snapshotProcessed chan babuzapb.SnapshotMessage
	finishStarted     chan struct{}
	releaseFinish     chan struct{}
	finishOnce        sync.Once
}

func newBlockingFinishStreamRaft() *blockingFinishStreamRaft {
	return &blockingFinishStreamRaft{
		batchProcessed:    make(chan babuzapb.BatchMessage, 1),
		snapshotProcessed: make(chan babuzapb.SnapshotMessage, 3),
		finishStarted:     make(chan struct{}),
		releaseFinish:     make(chan struct{}),
	}
}

func (m *blockingFinishStreamRaft) ProcessBatchMessage(message babuzapb.BatchMessage) {
	m.batchProcessed <- message
}

func (m *blockingFinishStreamRaft) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	m.snapshotProcessed <- message
	if message.Type == babuzapb.SnapshotMessageType_Finish {
		m.finishOnce.Do(func() {
			close(m.finishStarted)
		})
		<-m.releaseFinish
	}
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: message.Type.String(),
	}
}

func (m *blockingFinishStreamRaft) GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (m *blockingFinishStreamRaft) PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
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

func openBatchGETStreamForTest(t testing.TB, client *stdhttp.Client, baseURL string, from uint64) (<-chan babuzapb.BatchMessage, func()) {
	t.Helper()
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, fmt.Sprintf("%s%s?from=%d", baseURL, raftBatchMsgStreamPrefix, from), nil)
	assert.NoError(t, err)
	res, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, stdhttp.StatusOK, res.StatusCode)
	msgs := make(chan babuzapb.BatchMessage, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(msgs)
		reader := frame.NewReader(res.Body)
		for {
			eof, err := reader.ReadFrameOrEOF(func(msgType frame.MessageType, msgBuf []byte) error {
				if msgType != frame.BatchMsgType {
					return fmt.Errorf("unexpected message type: %d", msgType)
				}
				var msg babuzapb.BatchMessage
				if err := msg.Unmarshal(msgBuf); err != nil {
					return err
				}
				msgs <- msg
				return nil
			})
			if eof || err != nil {
				return
			}
		}
	}()
	return msgs, func() {
		_ = res.Body.Close()
		<-done
	}
}

func startHTTPMessageTestServer(t testing.TB, streamEnabled bool, tlsEnabled bool, raft ibabuza.RaftMessageHandler) (*httptest.Server, *atomic.Int64, *atomic.Int64, *MessageStreamHub) {
	t.Helper()
	shortCount := &atomic.Int64{}
	streamCount := &atomic.Int64{}
	hub := NewMessageStreamHub()
	h := &handler{
		raft: raft,
		config: ServerConfig{
			MessageStreamEnabled: streamEnabled,
			StreamIdleTimeout:    time.Second,
		},
		messageStreamHub: hub,
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
	return srv, shortCount, streamCount, hub
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

func startHTTPStreamTestServer(t testing.TB, raft ibabuza.RaftMessageHandler) *httptest.Server {
	t.Helper()
	h := &handler{
		raft: raft,
		config: ServerConfig{
			MessageStreamEnabled:      true,
			StreamIdleTimeout:         time.Second,
			SnapshotStreamIdleTimeout: time.Second,
		},
		messageStreamHub: NewMessageStreamHub(),
	}
	mux := stdhttp.NewServeMux()
	mux.HandleFunc(raftBatchMsgStreamPrefix, h.batchMessageStreamFunc)
	mux.HandleFunc(raftSnapshotStreamPrefix, h.snapshotMessageStreamFunc)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPBatchMessageStreamDisabledUsesShortRequest(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, shortCount, streamCount, _ := startHTTPMessageTestServer(t, false, false, raft)

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
	raft := newStreamTestRaft(1)
	srv, shortCount, streamCount, hub := startHTTPMessageTestServer(t, true, false, raft)
	streamMsgs, closeStream := openBatchGETStreamForTest(t, srv.Client(), srv.URL, 2)
	defer closeStream()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClientWithMessageStreamHub(srv.Client(), resolver, false,
		ServerConfig{MessageStreamEnabled: true}, hub, 1, raft)
	defer client.Close()

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 11)))

	got := make(map[uint64]bool)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-streamMsgs:
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

func TestHTTPBatchMessageStreamSwitchesPeer(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, shortCount, streamCount, hub := startHTTPMessageTestServer(t, true, false, raft)
	peer2Msgs, closePeer2 := openBatchGETStreamForTest(t, srv.Client(), srv.URL, 2)
	defer closePeer2()
	peer3Msgs, closePeer3 := openBatchGETStreamForTest(t, srv.Client(), srv.URL, 3)
	defer closePeer3()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	resolver.addressMap[3] = srv.Listener.Addr().String()
	client := NewRaftMsgClientWithMessageStreamHub(srv.Client(), resolver, false,
		ServerConfig{MessageStreamEnabled: true}, hub, 1, raft)
	defer client.Close()

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 3, 20)))

	select {
	case msg := <-peer2Msgs:
		assert.EqualValues(t, 2, msg.Messages[0].To)
		assert.EqualValues(t, 10, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for peer 2 streamed batch")
	}
	select {
	case msg := <-peer3Msgs:
		assert.EqualValues(t, 3, msg.Messages[0].To)
		assert.EqualValues(t, 20, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for peer 3 streamed batch")
	}
	assert.EqualValues(t, 0, shortCount.Load())
	assert.EqualValues(t, 2, streamCount.Load())
}

func TestHTTPBatchMessageStreamTLS(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, _, streamCount, hub := startHTTPMessageTestServer(t, true, true, raft)
	streamMsgs, closeStream := openBatchGETStreamForTest(t, srv.Client(), srv.URL, 2)
	defer closeStream()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClientWithMessageStreamHub(srv.Client(), resolver, true,
		ServerConfig{MessageStreamEnabled: true}, hub, 1, raft)
	defer client.Close()

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 10)))
	select {
	case msg := <-streamMsgs:
		assert.EqualValues(t, 10, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for TLS streamed batch")
	}
	assert.EqualValues(t, 1, streamCount.Load())
}

func TestHTTPBatchMessageStreamFallsBackToShortRequestWhenNoGETStream(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, shortCount, streamCount, hub := startHTTPMessageTestServer(t, true, false, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClientWithMessageStreamHub(srv.Client(), resolver, false,
		ServerConfig{MessageStreamEnabled: true}, hub, 1, raft)
	defer client.Close()

	assert.NoError(t, client.SendBatchMessage(testBatchMessage(1, 2, 11)))
	select {
	case msg := <-raft.processed:
		assert.EqualValues(t, 11, msg.Messages[0].Index)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback batch")
	}
	assert.EqualValues(t, 1, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())
}

func TestHTTPBatchMessageStreamRejectsInvalidRequests(t *testing.T) {
	raft := newStreamTestRaft(1)
	srv, _, _, _ := startHTTPMessageTestServer(t, true, false, raft)

	res, err := srv.Client().Post(srv.URL+raftBatchMsgStreamPrefix, "application/octet-stream", nil)
	assert.NoError(t, err)
	assert.Equal(t, stdhttp.StatusMethodNotAllowed, res.StatusCode)
	_ = res.Body.Close()

	res, err = srv.Client().Get(srv.URL + raftBatchMsgStreamPrefix)
	assert.NoError(t, err)
	assert.Equal(t, stdhttp.StatusBadRequest, res.StatusCode)
	_ = res.Body.Close()

	disabledSrv, _, _, _ := startHTTPMessageTestServer(t, false, false, raft)
	res, err = disabledSrv.Client().Get(disabledSrv.URL + raftBatchMsgStreamPrefix + "?from=2")
	assert.NoError(t, err)
	assert.Equal(t, stdhttp.StatusNotFound, res.StatusCode)
	_ = res.Body.Close()
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

func TestHTTPSnapshotStreamEnabledUsesShortRequests(t *testing.T) {
	raft := newSnapshotStreamTestRaft(3)
	srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	msgs := []babuzapb.SnapshotMessage{
		testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Metadata),
		testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Chunk),
		testSnapshotMessage(1, 2, 11, babuzapb.SnapshotMessageType_Finish),
	}
	for _, msg := range msgs {
		res, err := client.SendSnapshotMessage(msg)
		assert.NoError(t, err)
		assert.Equal(t, babuzapb.SUCCESS, res.Status)
		assert.Equal(t, msg.Type.String(), res.Message)
	}

	got := make(map[babuzapb.SnapshotMessageType]bool)
	for i := 0; i < 3; i++ {
		select {
		case msg := <-raft.processed:
			got[msg.Type] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for snapshot short request")
		}
	}
	assert.True(t, got[babuzapb.SnapshotMessageType_Metadata])
	assert.True(t, got[babuzapb.SnapshotMessageType_Chunk])
	assert.True(t, got[babuzapb.SnapshotMessageType_Finish])
	assert.EqualValues(t, 3, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())
}

func TestHTTPSnapshotShortRequestsWaitForProcessorResponse(t *testing.T) {
	raft := newSnapshotStreamTestRaft(1)
	releaseMetadata := make(chan struct{})
	var metadataOnce sync.Once
	raft.responseFn = func(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
		if message.Type == babuzapb.SnapshotMessageType_Metadata {
			metadataOnce.Do(func() {
				<-releaseMetadata
			})
		}
		return babuzapb.SnapshotMessageResponse{Status: babuzapb.SUCCESS, Message: message.Type.String()}
	}
	srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	metadataDone := make(chan snapshotStreamResult, 1)
	go func() {
		res, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 19, babuzapb.SnapshotMessageType_Metadata))
		metadataDone <- snapshotStreamResult{resp: res, err: err}
	}()
	select {
	case <-metadataDone:
		t.Fatal("snapshot short request returned before processor response")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseMetadata)
	select {
	case result := <-metadataDone:
		assert.NoError(t, result.err)
		assert.Equal(t, babuzapb.SUCCESS, result.resp.Status)
		assert.Equal(t, babuzapb.SnapshotMessageType_Metadata.String(), result.resp.Message)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metadata response")
	}
	assert.EqualValues(t, 1, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())
}

func TestHTTPSnapshotShortRequestsReturnProcessorFailure(t *testing.T) {
	tests := []struct {
		name        string
		failureType babuzapb.SnapshotMessageType
		message     string
	}{
		{name: "metadata", failureType: babuzapb.SnapshotMessageType_Metadata, message: "metadata failed"},
		{name: "chunk", failureType: babuzapb.SnapshotMessageType_Chunk, message: "chunk failed"},
		{name: "finish", failureType: babuzapb.SnapshotMessageType_Finish, message: "finish failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raft := newSnapshotStreamTestRaft(3)
			raft.responseFn = func(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
				if message.Type == tt.failureType {
					return babuzapb.SnapshotMessageResponse{Status: babuzapb.FAILED, Message: tt.message}
				}
				return babuzapb.SnapshotMessageResponse{Status: babuzapb.SUCCESS, Message: message.Type.String()}
			}
			srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, true, raft)

			resolver := NewMockTransportResolver()
			resolver.addressMap[2] = srv.Listener.Addr().String()
			client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
			defer client.Close()

			res, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 12, tt.failureType))
			assert.NoError(t, err)
			assert.Equal(t, babuzapb.FAILED, res.Status)
			assert.Equal(t, tt.message, res.Message)
			assert.EqualValues(t, 1, shortCount.Load())
			assert.EqualValues(t, 0, streamCount.Load())
		})
	}
}

func TestHTTPSnapshotShortRequestsDoNotKeepActiveStreamState(t *testing.T) {
	raft := newSnapshotStreamTestRaft(3)
	srv, shortCount, streamCount := startHTTPSnapshotTestServer(t, true, raft)

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClient(srv.Client(), resolver, false, ServerConfig{MessageStreamEnabled: true})
	defer client.Close()

	_, err := client.SendSnapshotMessage(testSnapshotMessage(1, 2, 13, babuzapb.SnapshotMessageType_Metadata))
	assert.NoError(t, err)
	_, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 14, babuzapb.SnapshotMessageType_Metadata))
	assert.NoError(t, err)
	_, err = client.SendSnapshotMessage(testSnapshotMessage(1, 2, 13, babuzapb.SnapshotMessageType_Finish))
	assert.NoError(t, err)
	assert.EqualValues(t, 3, shortCount.Load())
	assert.EqualValues(t, 0, streamCount.Load())
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

func waitSnapshotStreamDone(t *testing.T, client *RaftMsgClient) {
	t.Helper()
	assert.Eventually(t, func() bool {
		client.snapshotStreamMu.Lock()
		stream := client.snapshotStream
		client.snapshotStreamMu.Unlock()
		if stream == nil {
			return false
		}
		select {
		case <-stream.doneCh:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
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
	srv, _, _, _ := startHTTPMessageTestServer(b, false, false, raft)
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
	srv, _, _, hub := startHTTPMessageTestServer(b, true, false, raft)
	defer srv.Close()

	streamMsgs, closeStream := openBatchGETStreamForTest(b, srv.Client(), srv.URL, 2)
	defer closeStream()
	go func() {
		for msg := range streamMsgs {
			raft.ProcessBatchMessage(msg)
		}
	}()

	resolver := NewMockTransportResolver()
	resolver.addressMap[2] = srv.Listener.Addr().String()
	client := NewRaftMsgClientWithMessageStreamHub(srv.Client(), resolver, false,
		ServerConfig{MessageStreamEnabled: true}, hub, 1, raft)
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

func BenchmarkHTTPSnapshotShortRequestSmallChunksMessageStreamEnabled(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, true, 32, 256)
}

func BenchmarkHTTPSnapshotShortRequestLargeChunks(b *testing.B) {
	benchmarkHTTPSnapshotTransfer(b, false, 4, 8192)
}

func BenchmarkHTTPSnapshotShortRequestLargeChunksMessageStreamEnabled(b *testing.B) {
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
		c.PeerAddress = testutil.FreeTCPAddr(t, "localhost")
		mr := newMockTransportRaft(1)
		srv := NewRaftMsgServer(c.TransportConfig, defaultServerCfg, mr, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)
		httpClient, err := NewClient(c.clientTls, defaultServerCfg)
		assert.Nil(t, err, identify)

		resolver := NewMockTransportResolver()
		resolver.addressMap[0] = c.PeerAddress
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
		c.PeerAddress = testutil.FreeTCPAddr(t, "localhost")
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
				resolver.addressMap[0] = c.PeerAddress
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
