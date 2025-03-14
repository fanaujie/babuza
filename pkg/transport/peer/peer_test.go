package peer

import (
	"bytes"
	"context"
	"errors"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// MockMemoryLimiter implements limiter.ResourceLimiter for testing
type MockMemoryLimiter struct {
	limiter.ResourceLimiter
}

func NewMockMemoryLimiter(limit int64) limiter.ResourceLimiter {
	return &MockMemoryLimiter{
		limiter.NewMemorySizeLimiter(limit),
	}
}

// MockRateLimiter implements limiter.RateLimiter for testing
type MockRateLimiter struct {
	limiter.RateLimiter
}

func NewMockRateLimiter() limiter.RateLimiter {
	return &MockRateLimiter{}
}

func (m *MockRateLimiter) Wait(ctx context.Context) error { return nil }

// MockBreaker implements breaker.Breaker for testing
type MockBreaker struct {
	mu           sync.Mutex
	isSuccessful bool
}

func NewMockBreaker() *MockBreaker {
	return &MockBreaker{
		isSuccessful: true,
	}
}

func (b *MockBreaker) Reset() {
	b.isSuccessful = false
}

func (b *MockBreaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isSuccessful
}

func (b *MockBreaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isSuccessful = true
}

func (b *MockBreaker) Fail() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isSuccessful = false
}

// MockPeerRaftReport implements ibabuza.RaftStatusReporter for testing
type MockPeerRaftReport struct {
	unreachable map[uint64]struct{}
	snapshots   map[uint64]raft.SnapshotStatus
	mu          sync.Mutex
}

func NewMockPeerRaftReport() *MockPeerRaftReport {
	return &MockPeerRaftReport{
		unreachable: make(map[uint64]struct{}),
		snapshots:   make(map[uint64]raft.SnapshotStatus),
	}
}

func (r *MockPeerRaftReport) ReportUnreachable(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unreachable[id] = struct{}{}
}

func (r *MockPeerRaftReport) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[id] = status
}

func (r *MockPeerRaftReport) IsUnreachableReported(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.unreachable[id]
	return exists
}

func (r *MockPeerRaftReport) GetSnapshotStatus(id uint64) (raft.SnapshotStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, exists := r.snapshots[id]
	return status, exists
}

func (r *MockPeerRaftReport) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unreachable = make(map[uint64]struct{})
	r.snapshots = make(map[uint64]raft.SnapshotStatus)
}

// MockFailedClient implements ibabuza.TransportClient for testing failure scenarios
type MockFailedClient struct{}

func (c *MockFailedClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (c *MockFailedClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (c *MockFailedClient) SendBatchMessage(batch babuzapb.BatchMessage) error {
	return errors.New("failed to send batch message")
}

func (c *MockFailedClient) SendSnapshotMessage(msg babuzapb.SnapshotMessage) error {
	return errors.New("failed to send snapshot message")
}

func (c *MockFailedClient) Close() error {
	return nil
}

// MockSuccessClient implements ibabuza.TransportClient for testing success scenarios
type MockSuccessClient struct {
	sentBatchMessages []babuzapb.BatchMessage
	sentSnapMessages  []babuzapb.SnapshotMessage
	mu                sync.Mutex
}

func NewMockSuccessClient() *MockSuccessClient {
	return &MockSuccessClient{
		sentBatchMessages: make([]babuzapb.BatchMessage, 0),
		sentSnapMessages:  make([]babuzapb.SnapshotMessage, 0),
	}
}

func (c *MockSuccessClient) SendBatchMessage(batch babuzapb.BatchMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sentBatchMessages = append(c.sentBatchMessages, batch)
	return nil
}

func (c *MockSuccessClient) SendSnapshotMessage(msg babuzapb.SnapshotMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sentSnapMessages = append(c.sentSnapMessages, msg)
	return nil
}

func (c *MockSuccessClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (c *MockSuccessClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (c *MockSuccessClient) Close() error {
	return nil
}

func (c *MockSuccessClient) GetSentBatchMessages() []babuzapb.BatchMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sentBatchMessages
}

func (c *MockSuccessClient) GetSentSnapMessages() []babuzapb.SnapshotMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sentSnapMessages
}

// MockTransportClientFactory implements TransportClientFactory for testing
type MockTransportClientFactory struct {
	client     ibabuza.TransportClient
	shouldFail bool
}

func NewMockTransportClientFactory(shouldFail bool) *MockTransportClientFactory {
	return &MockTransportClientFactory{
		shouldFail: shouldFail,
	}
}

func (d *MockTransportClientFactory) CreateTransportClient() (ibabuza.TransportClient, error) {
	if d.shouldFail {
		d.client = &MockFailedClient{}
	} else {
		d.client = NewMockSuccessClient()
	}
	return d.client, nil
}

func (d *MockTransportClientFactory) GetClient() (ibabuza.TransportClient, bool) {
	if d.client == nil {
		return nil, false
	}
	return d.client, true
}

// MockSnapshotFileReader implements transport.SnapshotFileReader for testing
type MockSnapshotFileReader struct {
	metadata babuzapb.SnapshotMetadata
	fileData map[string][]byte
}

func NewMockSnapshotFileReader(term, index uint64) *MockSnapshotFileReader {
	files := map[string]babuzapb.SnapshotFileDesc{
		"test1": {
			Tag:      "test1",
			FileSize: 10,
			FileType: babuzapb.SnapshotFileType_StateMachine,
		},
		"test2": {
			Tag:      "test2",
			FileSize: 20,
			FileType: babuzapb.SnapshotFileType_Cluster,
		},
	}

	snapMetadata := babuzapb.SnapshotMetadata{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: index,
				Term:  term,
			},
		},
		Files: files,
	}

	fileData := map[string][]byte{
		"test1": []byte("1234567890"),
		"test2": []byte("12345678901234567890"),
	}

	return &MockSnapshotFileReader{
		metadata: snapMetadata,
		fileData: fileData,
	}
}

func (r *MockSnapshotFileReader) Metadata() babuzapb.SnapshotMetadata {
	return r.metadata
}

func (r *MockSnapshotFileReader) ForEachFile(visitor func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error) error {
	for tag, data := range r.fileData {
		fileDesc := r.metadata.Files[tag]
		err := visitor(bytes.NewReader(data), fileDesc)
		if err != nil {
			return err
		}
	}
	return nil
}

// Test cases for RaftPeer
func TestRaftPeerNew(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 1024,
		SnapshotChunkSize:          256,
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  100 * time.Millisecond,
	}

	report := NewMockPeerRaftReport()
	memLimiter := NewMockMemoryLimiter(4096)
	chunkLimiter := NewMockRateLimiter()
	breaker := NewMockBreaker()
	dialer := NewMockTransportClientFactory(false)

	peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})
	assert.NotNil(t, peer, "Peer should not be nil")
	// Sleep briefly to allow goroutine to start
	time.Sleep(100 * time.Millisecond)

	// Clean up
	peer.Stop()
}

func TestRaftPeerSendRaftMessage(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 1024,
		SnapshotChunkSize:          256,
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  100 * time.Millisecond,
	}

	t.Run("success case", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		clientFactory := NewMockTransportClientFactory(false)
		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, clientFactory, &logger.Mock{})

		defer peer.Stop()

		// Allow time for goroutine to start
		time.Sleep(100 * time.Millisecond)

		// Send a message
		msg := &raftpb.Message{
			Type:  raftpb.MsgApp,
			To:    peerId,
			From:  1,
			Index: 10,
		}

		err := peer.SendRaftMessage(msg)
		assert.NoError(t, err, "Should send message without error")

		// Allow time for message to be processed
		time.Sleep(200 * time.Millisecond)

		// Check if message was sent to the client
		client, exists := clientFactory.GetClient()
		assert.True(t, exists, "Client should exist")
		mockClient := client.(*MockSuccessClient)
		sentMessages := mockClient.GetSentBatchMessages()
		assert.GreaterOrEqual(t, len(sentMessages), 1, "Should have sent at least one batch message")

		// Verify message content in the first batch
		if len(sentMessages) > 0 && len(sentMessages[0].Messages) > 0 {
			sentMsg := sentMessages[0].Messages[0]
			assert.Equal(t, msg.Type, sentMsg.Type)
			assert.Equal(t, msg.To, sentMsg.To)
			assert.Equal(t, msg.From, sentMsg.From)
			assert.Equal(t, msg.Index, sentMsg.Index)
		}
	})

	t.Run("breaker not ready", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		dialer := NewMockTransportClientFactory(false)

		// Set breaker to not ready
		breaker.Fail()

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})

		defer peer.Stop()

		// Send a message
		msg := &raftpb.Message{
			Type:  raftpb.MsgApp,
			To:    peerId,
			From:  1,
			Index: 10,
		}

		err := peer.SendRaftMessage(msg)
		assert.ErrorIs(t, err, ErrPeerBreakerNotReady, "Should return breaker not ready error")
		assert.True(t, report.IsUnreachableReported(peerId), "Should report peer as unreachable")
	})

	t.Run("memory limit exceeded", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(10) // Very small limit
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		dialer := NewMockTransportClientFactory(false)

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})

		defer peer.Stop()

		// Create a message that will exceed the memory limit
		msg := &raftpb.Message{
			Type:    raftpb.MsgApp,
			To:      peerId,
			From:    1,
			Index:   10,
			Entries: make([]raftpb.Entry, 100), // Make it large
		}

		err := peer.SendRaftMessage(msg)
		assert.ErrorIs(t, err, ErrPeerReachMaxTotalSendMsgSize, "Should return memory limit error")
		assert.True(t, report.IsUnreachableReported(peerId), "Should report peer as unreachable")
	})

	t.Run("peer stopped", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		dialer := NewMockTransportClientFactory(false)

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})

		// Stop the peer
		peer.Stop()

		// Send a message
		msg := &raftpb.Message{
			Type:  raftpb.MsgApp,
			To:    peerId,
			From:  1,
			Index: 10,
		}

		err := peer.SendRaftMessage(msg)
		assert.ErrorIs(t, err, ErrPeerStopped, "Should return peer stopped error")
	})

	t.Run("queue full", func(t *testing.T) {
		cfg = RaftPeerConfig{
			LimiterMaxBatchMessageSize: 1024,
			SnapshotChunkSize:          256,
			RaftMsgQueueSize:           1, // Very small queue size
			DialTimeout:                time.Second,
			SendSnapshotChunkInterval:  100 * time.Millisecond,
		}
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		dialer := NewMockTransportClientFactory(false)

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})

		defer peer.Stop()

		// Send first message to create the queue
		msg1 := &raftpb.Message{
			Type:  raftpb.MsgApp,
			To:    peerId,
			From:  1,
			Index: 10,
		}

		err := peer.SendRaftMessage(msg1)
		assert.NoError(t, err, "First message should be sent successfully")

		// Give it a small moment to process but not long enough to dequeue
		time.Sleep(10 * time.Millisecond)

		// Fill the queue completely
		for i := 0; i < 10; i++ {
			msg2 := &raftpb.Message{
				Type:  raftpb.MsgApp,
				To:    peerId,
				From:  1,
				Index: 11 + uint64(i),
			}
			err = peer.SendRaftMessage(msg2)
			if errors.Is(err, ErrPeerQueueFull) {
				break
			}
		}
		assert.ErrorIs(t, err, ErrPeerQueueFull, "Should return queue full error")
	})
}

func TestRaftPeerMessageBatching(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 50, // Small batch size to force batching
		SnapshotChunkSize:          256,
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  100 * time.Millisecond,
	}

	report := NewMockPeerRaftReport()
	memLimiter := NewMockMemoryLimiter(4096)
	chunkLimiter := NewMockRateLimiter()
	breaker := NewMockBreaker()
	dialer := NewMockTransportClientFactory(false)

	peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, dialer, &logger.Mock{})

	defer peer.Stop()

	// Send multiple messages that will exceed batch size
	for i := 0; i < 5; i++ {
		msg := &raftpb.Message{
			Type:  raftpb.MsgApp,
			To:    peerId,
			From:  1,
			Index: 10 + uint64(i),
			Entries: []raftpb.Entry{
				{Data: make([]byte, 20)}, // Large enough to trigger batching
			},
		}

		err := peer.SendRaftMessage(msg)
		assert.NoError(t, err, "Should send message without error")
	}
	// Allow time for message to be processed
	time.Sleep(time.Second)

	// Check if messages were properly batched
	client, exists := dialer.GetClient()
	assert.True(t, exists, "Client should exist")
	mockClient := client.(*MockSuccessClient)
	sentBatches := mockClient.GetSentBatchMessages()
	assert.GreaterOrEqual(t, len(sentBatches), 2, "Should have sent at least two batches")
}
func TestRaftPeerSendSnapshot(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 1024,
		SnapshotChunkSize:          4, // Small chunk size for testing
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  10 * time.Millisecond, // Fast for testing
	}

	t.Run("success case", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		clientFactory := NewMockTransportClientFactory(false)

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, clientFactory, &logger.Mock{})

		defer peer.Stop()

		// Create snapshot message and reader
		snapReader := NewMockSnapshotFileReader(1, 100)
		snapMsg := &raftpb.Message{
			Type:  raftpb.MsgSnap,
			To:    peerId,
			From:  1,
			Index: 100,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 100,
					Term:  1,
				},
			},
		}

		peer.SendSnapshot(snapMsg, snapReader)
		peer.closer.Wait()
		// Check snapshot status
		status, exists := report.GetSnapshotStatus(peerId)
		assert.True(t, exists, "Snapshot report should exist")
		assert.Equal(t, raft.SnapshotFinish, status, "Should report snapshot finished")

		// Check if snapshot messages were sent
		client, exists := clientFactory.GetClient()
		assert.True(t, exists, "Client should exist")
		mockClient := client.(*MockSuccessClient)

		sentSnapMessages := mockClient.GetSentSnapMessages()
		assert.Greater(t, len(sentSnapMessages), 0, "Should have sent snapshot messages")

		// Verify metadata message was sent
		metadataSent := false
		finishSent := false
		for _, msg := range sentSnapMessages {
			if msg.Metadata != nil {
				metadataSent = true
			}
			if msg.FinishMessage != nil {
				finishSent = true
			}
		}

		assert.True(t, metadataSent, "Should have sent metadata message")
		assert.True(t, finishSent, "Should have sent finish message")
	})

	t.Run("failed client", func(t *testing.T) {
		report := NewMockPeerRaftReport()
		memLimiter := NewMockMemoryLimiter(4096)
		chunkLimiter := NewMockRateLimiter()
		breaker := NewMockBreaker()
		clientFactory := NewMockTransportClientFactory(true) // Use failing clientFactory

		peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, clientFactory, &logger.Mock{})

		defer peer.Stop()

		// Create snapshot message and reader
		snapReader := NewMockSnapshotFileReader(1, 100)
		snapMsg := &raftpb.Message{
			Type:  raftpb.MsgSnap,
			To:    peerId,
			From:  1,
			Index: 100,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 100,
					Term:  1,
				},
			},
		}

		peer.SendSnapshot(snapMsg, snapReader)
		peer.closer.Wait()
		// Check snapshot status
		status, exists := report.GetSnapshotStatus(peerId)
		assert.True(t, exists, "Snapshot report should exist")
		assert.Equal(t, raft.SnapshotFailure, status, "Should report snapshot failure")

		// Check if unreachable was reported
		assert.True(t, report.IsUnreachableReported(peerId), "Should report peer as unreachable")

		// Check if breaker was marked as failed
		assert.False(t, breaker.Ready(), "Breaker should be marked as not ready")
	})
}

func TestRaftPeerUpdateRaftReport(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 1024,
		SnapshotChunkSize:          256,
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  100 * time.Millisecond,
	}

	report := NewMockPeerRaftReport()
	memLimiter := NewMockMemoryLimiter(4096)
	chunkLimiter := NewMockRateLimiter()
	breaker := NewMockBreaker()
	clientFactory := NewMockTransportClientFactory(false)

	peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, clientFactory, &logger.Mock{})

	defer peer.Stop()

	// Create a new report
	newReport := NewMockPeerRaftReport()

	// Update the report
	peer.UpdateRaftReport(newReport)

	// Test that the new report is used
	newReport.ReportUnreachable(peerId)
	assert.True(t, newReport.IsUnreachableReported(peerId), "New report should be used")
	assert.False(t, report.IsUnreachableReported(peerId), "Old report should not be affected")
}

func TestRaftPeerStop(t *testing.T) {
	peerId := uint64(2)
	cfg := RaftPeerConfig{
		LimiterMaxBatchMessageSize: 1024,
		SnapshotChunkSize:          256,
		RaftMsgQueueSize:           128,
		DialTimeout:                time.Second,
		SendSnapshotChunkInterval:  100 * time.Millisecond,
	}

	report := NewMockPeerRaftReport()
	memLimiter := NewMockMemoryLimiter(4096)
	chunkLimiter := NewMockRateLimiter()
	breaker := NewMockBreaker()
	clientFactory := NewMockTransportClientFactory(false)

	peer := New(100, peerId, cfg, report, memLimiter, chunkLimiter, breaker, clientFactory, &logger.Mock{})

	// Stop the peer
	peer.Stop()

	// Try to send a message after stopping
	msg := &raftpb.Message{
		Type:  raftpb.MsgApp,
		To:    peerId,
		From:  1,
		Index: 10,
	}

	err := peer.SendRaftMessage(msg)
	assert.ErrorIs(t, err, ErrPeerStopped, "Should return peer stopped error")
}
