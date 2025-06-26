package transport

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"sync"
	"testing"
	"time"
)

// mockMultiRaftProcessor implements the ibabuza.MultiRaftStoreHandler interface for testing
type mockMultiRaftProcessor struct {
	receivedMsg      map[uint64][]raftpb.Message
	receivedSnapMsg  map[uint64]babuzapb.SnapshotMessage
	unreachable      map[uint64]struct{}
	snapshotStatus   map[uint64]raft.SnapshotStatus
	snapshotReader   ibabuza.SnapshotReader
	metadata         babuzapb.SnapshotMetadata
	snapshotFileData map[string][]byte
	finishMsg        raftpb.Message
	mu               sync.Mutex
}

func newMockMultiRaftProcessor() *mockMultiRaftProcessor {
	return &mockMultiRaftProcessor{
		receivedMsg:      make(map[uint64][]raftpb.Message),
		receivedSnapMsg:  make(map[uint64]babuzapb.SnapshotMessage),
		unreachable:      make(map[uint64]struct{}),
		snapshotStatus:   make(map[uint64]raft.SnapshotStatus),
		snapshotFileData: make(map[string][]byte),
	}
}

func (mr *mockMultiRaftProcessor) ProcessMultiRaftMessage(message babuzapb.MultiRaftBatchMessage) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for _, msg := range message.Messages {
		mr.receivedMsg[msg.GroupID] = append(mr.receivedMsg[msg.GroupID], msg.Message)
	}
}

func (mr *mockMultiRaftProcessor) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.receivedSnapMsg[message.Index] = message

	// Store snapshot data for validation
	if message.Type == babuzapb.SnapshotMessageType_Finish {
		mr.finishMsg = raftpb.Message{
			Type:     raftpb.MsgSnap,
			From:     message.From,
			To:       message.To,
			Snapshot: message.FinishMessage.Snapshot,
		}
	} else if message.Type == babuzapb.SnapshotMessageType_Chunk {
		if _, ok := mr.snapshotFileData[message.ChunkMessage.FileTag]; !ok {
			mr.snapshotFileData[message.ChunkMessage.FileTag] = make([]byte, 0)
		}
		mr.snapshotFileData[message.ChunkMessage.FileTag] = append(
			mr.snapshotFileData[message.ChunkMessage.FileTag],
			message.ChunkMessage.Data...,
		)
	} else if message.Type == babuzapb.SnapshotMessageType_Metadata {
		mr.metadata = message.Metadata
	}
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: "Success",
	}
}

func (mr *mockMultiRaftProcessor) ReportUnreachable(groupID ibabuza.RaftGroupID, nodeID uint64) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.unreachable[nodeID] = struct{}{}
}

func (mr *mockMultiRaftProcessor) ReportSnapshot(groupID ibabuza.RaftGroupID, nodeID uint64, status raft.SnapshotStatus) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.snapshotStatus[nodeID] = status
}

func (mr *mockMultiRaftProcessor) CreateSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	return mr.snapshotReader, nil
}

func (mr *mockMultiRaftProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{
		Peers: []babuzapb.Peer{
			{
				RaftPeerAttr: babuzapb.RaftPeerAttribute{
					PeerID:         1,
					RaftListenAddr: "localhost:14200",
				},
			},
			{
				RaftPeerAttr: babuzapb.RaftPeerAttribute{
					PeerID:         2,
					RaftListenAddr: "localhost:14201",
				},
			},
		},
	}
}

func (mr *mockMultiRaftProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{
		Status:  babuzapb.SUCCESS,
		Message: "Success"}
}

// mockSnapshotReader implements the ibabuza.SnapshotReader interface for testing
type mockMultiRaftSnapshotReader struct {
	metadata         babuzapb.SnapshotMetadata
	snapshotFileData map[string][]byte
}

func newMockMultiRaftSnapshotReader(term, index uint64, files map[string]babuzapb.SnapshotFileDesc) *mockMultiRaftSnapshotReader {
	snapMetadata := babuzapb.SnapshotMetadata{
		Version: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: index,
				Term:  term,
			},
		},
		Files: files,
	}
	reader := &mockMultiRaftSnapshotReader{
		metadata:         snapMetadata,
		snapshotFileData: make(map[string][]byte),
	}
	for tag, fsm := range snapMetadata.Files {
		d := make([]byte, fsm.FileSize)
		_, err := rand.Read(d)
		if err != nil {
			return nil
		}
		reader.snapshotFileData[tag] = d
	}
	return reader
}

func (mr *mockMultiRaftSnapshotReader) Open(fileTag string) (io.Reader, ibabuza.StateMachineFileDesc, error) {
	return nil, ibabuza.StateMachineFileDesc{}, nil
}

func (mr *mockMultiRaftSnapshotReader) Close() error {
	return nil
}

func (mr *mockMultiRaftSnapshotReader) Cluster() (io.Reader, error) {
	return nil, nil
}

func (mr *mockMultiRaftSnapshotReader) Session() (io.Reader, error) {
	return nil, nil
}

func (mr *mockMultiRaftSnapshotReader) Metadata() babuzapb.SnapshotMetadata {
	return mr.metadata
}

func (mr *mockMultiRaftSnapshotReader) ForEachFile(visitor func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error) error {
	for tag, d := range mr.snapshotFileData {
		if err := visitor(bufio.NewReader(bytes.NewReader(d)), mr.metadata.Files[tag]); err != nil {
			return err
		}
	}
	return nil
}

func (mr *mockMultiRaftSnapshotReader) CreateTarArchiveReader() (io.ReadCloser, error) {
	return nil, nil
}

// Helper function to create a new MultiRaftTransport for testing
func newTestMultiRaftTransport(t *testing.T, nodeId uint64, listenAddress string) (*MultiRaftTransport, *mockMultiRaftProcessor) {
	var tranProtocol ibabuza.MultiRaftTransportProtocol
	mockProc := newMockMultiRaftProcessor()

	mockProc.snapshotReader = newMockMultiRaftSnapshotReader(1, 1, map[string]babuzapb.SnapshotFileDesc{
		"one": {
			Tag:      "one",
			FileSize: 8,
		},
		"two": {
			Tag:      "two",
			FileSize: 24,
		},
	})

	// Using GrpcMultiRaft protocol
	tranProtocol = protocol.NewGrpcMultiRaft(&logger.Mock{},
		protocol.SetGrpcMultiRaftOptsWithDialTimeout(time.Second),
		protocol.SetGrpcMultiRaftOptsWithIdleTimeout(time.Second))

	peerManager := NewPeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter]()
	trans := NewMultiRaftTransport(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), tranProtocol, &logger.Mock{}, SetTransportOptionsWithPeerSnapshotChunkSize(8))

	err := trans.SetupTransportConfig(ibabuza.TransportConfig{
		LocalNodeID: nodeId,
		PeerAddress: listenAddress,
	})
	assert.NoError(t, err)

	assert.Nil(t, trans.SetupTransportRaft(mockProc))
	assert.Nil(t, trans.Start())

	return trans, mockProc
}

// Test MultiRaftTransport creation
func TestMultiRaftTransport_Create(t *testing.T) {
	peerManager := NewPeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter]()
	grpcMultiRaftProtocol := protocol.NewGrpcMultiRaft(&logger.Mock{})
	grpcMultiRaftTrans := NewMultiRaftTransport(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), grpcMultiRaftProtocol, &logger.Mock{})
	assert.NotNil(t, grpcMultiRaftTrans)
}

// Test sending messages between MultiRaftTransports
func TestMultiRaftTransport_Send(t *testing.T) {
	node1ListenAddr := "localhost:24200"
	node2ListenAddr := "localhost:24201"
	trans1, mockRaft1 := newTestMultiRaftTransport(t, 1, node1ListenAddr)
	defer trans1.Stop()
	trans2, mockRaft2 := newTestMultiRaftTransport(t, 2, node2ListenAddr)
	defer trans2.Stop()

	groupID := ibabuza.RaftGroupID(101)
	// Test sending from node 1 to node 2
	trans1.AddPeer(groupID, 2, node2ListenAddr)

	// Create a MultiRaftMessage to send
	msg1To2 := raftpb.Message{
		Type:  raftpb.MsgApp,
		To:    2,
		From:  1,
		Term:  1,
		Index: 1,
	}
	trans1.Send(groupID, msg1To2)
	time.Sleep(time.Second) // Allow message to be delivered

	// Verify message was received
	mockRaft2.mu.Lock()
	trans2RecMsg, ok := mockRaft2.receivedMsg[uint64(groupID)]
	mockRaft2.mu.Unlock()
	assert.True(t, ok, "Message should be received")
	assert.Equal(t, msg1To2, trans2RecMsg[0])

	// Test sending from node 2 to node 1
	groupID = ibabuza.RaftGroupID(102)
	trans2.AddPeer(groupID, 1, node1ListenAddr)
	msg2To1 := raftpb.Message{
		Type: raftpb.MsgApp,
		To:   1,
		From: 2,
		Term: 2,
	}
	trans2.Send(groupID, msg2To1)
	time.Sleep(time.Second) // Allow message to be delivered

	// Verify message was received
	mockRaft1.mu.Lock()
	trans1RecMsg, ok := mockRaft1.receivedMsg[uint64(groupID)]
	mockRaft1.mu.Unlock()
	assert.True(t, ok, "Message should be received")
	assert.Equal(t, msg2To1, trans1RecMsg[0])
}

// Test sending a snapshot between MultiRaftTransports
func TestMultiRaftTransport_SendSnapshot(t *testing.T) {
	node1ListenAddr := "localhost:24200"
	node2ListenAddr := "localhost:24201"
	trans1, mockRaft1 := newTestMultiRaftTransport(t, 1, node1ListenAddr)
	defer trans1.Stop()

	trans2, mockRaft2 := newTestMultiRaftTransport(t, 2, node2ListenAddr)
	defer trans2.Stop()
	groupID := ibabuza.RaftGroupID(201)
	// Add peer and prepare for snapshot
	trans1.AddPeer(groupID, 2, node2ListenAddr)

	// Create and send a snapshot message
	snapMsg := raftpb.Message{
		Type: raftpb.MsgSnap,
		To:   2,
		From: 1,
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 1,
				Term:  1,
			},
		},
	}
	trans1.SendSnapshot(groupID, snapMsg)

	// Allow time for snapshot to be processed
	time.Sleep(time.Second * 2)

	// Verify snapshot was received correctly
	assert.Equal(t, mockRaft1.snapshotReader.Metadata(), mockRaft2.metadata, "Snapshot metadata should match")
	for tag, data := range mockRaft1.snapshotFileData {
		assert.Equal(t, data, mockRaft2.snapshotFileData[tag], "Snapshot file data should match")
	}
	assert.Equal(t, snapMsg.From, mockRaft2.finishMsg.From, "Snapshot finish message From field should match")
	assert.Equal(t, snapMsg.To, mockRaft2.finishMsg.To, "Snapshot finish message To field should match")
	assert.Equal(t, snapMsg.Type, mockRaft2.finishMsg.Type, "Snapshot finish message Type field should match")
}

// Test peer management operations for MultiRaftTransport
func TestMultiRaftTransport_PeerManagement(t *testing.T) {
	node1ListenAddr := "localhost:24200"
	node2ListenAddr := "localhost:24201"
	node3ListenAddr := "localhost:24202"
	trans, _ := newTestMultiRaftTransport(t, 1, node1ListenAddr)
	defer trans.Stop()

	// Test adding peers
	trans.AddPeer(1, 2, node2ListenAddr)
	trans.AddPeer(1, 3, node3ListenAddr)

	// Verify peer was added correctly
	addr, err := trans.peerMgr.ResolvePeerAddress(1, 2)
	assert.Nil(t, err, "Should be able to get peer address")
	assert.Equal(t, node2ListenAddr, addr)

	// Test updating peer
	trans.UpdatePeer(1, 2, "localhost:14203")

	// Verify peer was updated
	addr, err = trans.peerMgr.ResolvePeerAddress(1, 2)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:14203", addr)

	// Test removing a specific peer
	trans.RemovePeer(1, 3)

	// Verify peer was removed
	_, err = trans.peerMgr.ResolvePeerAddress(1, 3)
	assert.NotNil(t, err, "Peer should be removed")

	// Test removing all peers
	trans.RemovePeers()

	// Verify all peers were removed
	_, err = trans.peerMgr.ResolvePeerAddress(1, 2)
	assert.NotNil(t, err, "All peers should be removed")
}

// Test unreachable peer reporting for MultiRaftTransport
func TestMultiRaftTransport_UnreachablePeer(t *testing.T) {
	trans1, mockRaft1 := newTestMultiRaftTransport(t, 1, "localhost:14200")
	defer trans1.Stop()
	groupID := ibabuza.RaftGroupID(301)

	// Add a non-existent peer to test unreachable reporting
	trans1.AddPeer(groupID, 99, "localhost:99999")
	// Send a message to the unreachable peer
	msg := raftpb.Message{
		Type:  raftpb.MsgApp,
		To:    99,
		From:  1,
		Term:  1,
		Index: 1,
	}
	trans1.Send(groupID, msg)

	// Allow time for unreachable report
	time.Sleep(time.Second * 2)

	// Verify the peer was reported as unreachable
	mockRaft1.mu.Lock()
	_, unreachable := mockRaft1.unreachable[99]
	mockRaft1.mu.Unlock()
	assert.True(t, unreachable, "Peer should be reported as unreachable")
}

// Test updating a peer's address and continuing to communicate for MultiRaftTransport
func TestMultiRaftTransport_UpdatePeerAndCommunicate(t *testing.T) {
	trans1, _ := newTestMultiRaftTransport(t, 1, "localhost:14200")
	defer trans1.Stop()

	// Start the second transport at a different address
	trans2, mockRaft2 := newTestMultiRaftTransport(t, 2, "localhost:14201")
	defer trans2.Stop()

	groupID := ibabuza.RaftGroupID(401)
	// First add the peer with a wrong address
	trans1.AddPeer(groupID, 2, "localhost:12345")

	// Update the peer with the correct address
	trans1.UpdatePeer(groupID, 2, "localhost:14201")

	// Send a message to the updated peer
	msg := raftpb.Message{
		Type:  raftpb.MsgApp,
		To:    2,
		From:  1,
		Term:  1,
		Index: 1,
	}
	trans1.Send(groupID, msg)

	// Allow time for message to be delivered
	time.Sleep(time.Second)

	// Verify message was received after the peer update
	mockRaft2.mu.Lock()
	trans2RecMsg, ok := mockRaft2.receivedMsg[uint64(groupID)]
	mockRaft2.mu.Unlock()

	assert.True(t, ok, "Message should be received after peer update")
	assert.Equal(t, msg, trans2RecMsg[0])
}
