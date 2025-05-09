package transport

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
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

const (
	transportTypeTcp  = 1
	transportTypeHttp = 2
	transportTypeGrpc = 3
)

// mockRaftProcessor implements the ibabuza.RaftNodeHandler interface for testing
type mockRaftProcessor struct {
	receivedMsg      map[uint64]raftpb.Message
	receivedSnapMsg  map[uint64]babuzapb.SnapshotMessage
	unreachable      map[uint64]struct{}
	snapshotStatus   map[uint64]raft.SnapshotStatus
	snapshotReader   ibabuza.SnapshotReader
	metadata         babuzapb.SnapshotMetadata
	snapshotFileData map[string][]byte
	finishMsg        raftpb.Message
	mu               sync.Mutex
}

func (mr *mockRaftProcessor) ProcessMultiRaftMessage(message babuzapb.MultiRaftBatchMessage) {
	// not implemented
}

func newMockRaftProcessor() *mockRaftProcessor {
	return &mockRaftProcessor{
		receivedMsg:      make(map[uint64]raftpb.Message),
		receivedSnapMsg:  make(map[uint64]babuzapb.SnapshotMessage),
		unreachable:      make(map[uint64]struct{}),
		snapshotStatus:   make(map[uint64]raft.SnapshotStatus),
		snapshotFileData: make(map[string][]byte),
	}
}

func (mr *mockRaftProcessor) ProcessBatchMessage(message babuzapb.BatchMessage) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for _, msg := range message.Messages {
		mr.receivedMsg[msg.Index] = msg
	}
}

func (mr *mockRaftProcessor) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
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

func (mr *mockRaftProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{
		Peers: []babuzapb.Peer{
			{
				RaftPeerAttr: babuzapb.RaftPeerAttribute{
					Id:             1,
					RaftListenAddr: "localhost:14200",
				},
			},
			{
				RaftPeerAttr: babuzapb.RaftPeerAttribute{
					Id:             2,
					RaftListenAddr: "localhost:14201",
				},
			},
		},
	}
}

func (mr *mockRaftProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{
		Status:  babuzapb.SUCCESS,
		Message: "Success"}
}

func (mr *mockRaftProcessor) ReportUnreachable(id uint64) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.unreachable[id] = struct{}{}
}

func (mr *mockRaftProcessor) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.snapshotStatus[id] = status
}

func (mr *mockRaftProcessor) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	return mr.snapshotReader, nil
}

// mockSnapshotReader implements the ibabuza.SnapshotReader interface for testing
type mockSnapshotReader struct {
	metadata         babuzapb.SnapshotMetadata
	snapshotFileData map[string][]byte
}

func newMockSnapshotReader(term, index uint64, files map[string]babuzapb.SnapshotFileDesc) *mockSnapshotReader {
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
	reader := &mockSnapshotReader{
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

func (mr *mockSnapshotReader) Open(fileTag string) (io.Reader, ibabuza.StateMachineFileDesc, error) {
	return nil, ibabuza.StateMachineFileDesc{}, nil
}

func (mr *mockSnapshotReader) Close() error {
	return nil
}

func (mr *mockSnapshotReader) Cluster() (io.Reader, error) {
	return nil, nil
}

func (mr *mockSnapshotReader) Session() (io.Reader, error) {
	return nil, nil
}

func (mr *mockSnapshotReader) Metadata() babuzapb.SnapshotMetadata {
	return mr.metadata
}

func (mr *mockSnapshotReader) ForEachFile(visitor func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error) error {
	for tag, d := range mr.snapshotFileData {
		if err := visitor(bufio.NewReader(bytes.NewReader(d)), mr.metadata.Files[tag]); err != nil {
			return err
		}
	}
	return nil
}

func (mr *mockSnapshotReader) CreateTarArchiveReader() (io.ReadCloser, error) {
	return nil, nil
}

// Test transport creation with different protocols
func TestTransport_Create(t *testing.T) {
	// Test TCP protocol
	peerManager := NewPeerManager()
	tcpProtocol := protocol.NewTcp(networkio.NewTcpPhysicalIO(), &logger.Mock{})
	tcpTrans := New(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), tcpProtocol, &logger.Mock{})
	assert.NotNil(t, tcpTrans)

	// Test HTTP protocol
	peerManager = NewPeerManager()
	httpProtocol := protocol.NewHttp(&logger.Mock{})
	httpTrans := New(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), httpProtocol, &logger.Mock{})
	assert.NotNil(t, httpTrans)

	// Test GRPC protocol
	peerManager = NewPeerManager()
	grpcProtocol := protocol.NewGrpc(&logger.Mock{})
	grpcTrans := New(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), grpcProtocol, &logger.Mock{})
	assert.NotNil(t, grpcTrans)
}

// Helper function to create a new transport for testing
func newTestTransport(t *testing.T, transType int, nodeId uint64, listenAddress string) (*Transport, *mockRaftProcessor) {
	var tranProtocol ibabuza.TransportProtocol
	mockProc := newMockRaftProcessor()

	mockProc.snapshotReader = newMockSnapshotReader(1, 1, map[string]babuzapb.SnapshotFileDesc{
		"one": {
			Tag:      "one",
			FileSize: 8,
		},
		"two": {
			Tag:      "two",
			FileSize: 24,
		},
	})

	switch transType {
	case transportTypeTcp:
		tranProtocol = protocol.NewTcp(networkio.NewTcpPhysicalIO(), &logger.Mock{},
			protocol.SetTcpOptsWithReadDeadline(time.Second),
			protocol.SetTcpOptsWithWriteDeadline(time.Second))
	case transportTypeHttp:
		tranProtocol = protocol.NewHttp(&logger.Mock{},
			protocol.SetHttpOptsWithReadDeadline(time.Second),
			protocol.SetHttpOptsWithWriteDeadline(time.Second))
	case transportTypeGrpc:
		tranProtocol = protocol.NewGrpc(&logger.Mock{},
			protocol.SetGrpcOptsWithDialTimeout(time.Second),
			protocol.SetGrpcOptsWithIdleTimeout(time.Second))
	default:
		assert.Fail(t, "unknown transport type")
	}

	peerManager := NewPeerManager()
	trans := New(1, peerManager, limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), tranProtocol, &logger.Mock{}, SetTransportOptionsWithPeerSnapshotChunkSize(8))

	err := trans.SetupTransportConfig(ibabuza.TransportConfig{
		PeerId:      nodeId,
		PeerAddress: listenAddress,
	})
	assert.NoError(t, err)

	assert.Nil(t, trans.SetupTransportRaft(mockProc))
	assert.Nil(t, trans.Start())

	return trans, mockProc
}

// Test sending messages between transports
func TestTransport_Send(t *testing.T) {
	for _, transportType := range []int{transportTypeTcp, transportTypeHttp, transportTypeGrpc} {
		transportName := "TCP"
		if transportType == transportTypeHttp {
			transportName = "HTTP"
		} else if transportType == transportTypeGrpc {
			transportName = "GRPC"
		}

		t.Run(transportName, func(t *testing.T) {
			trans1, mockRaft1 := newTestTransport(t, transportType, 1, "localhost:14200")
			defer trans1.Stop()
			trans2, mockRaft2 := newTestTransport(t, transportType, 2, "localhost:14201")
			defer trans2.Stop()

			// Test sending from node 1 to node 2
			trans1.AddPeer(2, "localhost:14201")
			msg1To2 := raftpb.Message{
				Type:  raftpb.MsgApp,
				To:    2,
				From:  1,
				Term:  1,
				Index: 1,
			}
			trans1.Send(msg1To2)
			time.Sleep(time.Second) // Allow message to be delivered

			// Verify message was received
			mockRaft2.mu.Lock()
			trans2RecMsg, ok := mockRaft2.receivedMsg[1]
			mockRaft2.mu.Unlock()
			assert.True(t, ok, "Message should be received")
			assert.Equal(t, msg1To2, trans2RecMsg)

			// Test sending from node 2 to node 1
			trans2.AddPeer(1, "localhost:14200")
			msg2To1 := raftpb.Message{
				Type:  raftpb.MsgApp,
				To:    1,
				From:  2,
				Term:  2,
				Index: 2,
			}
			trans2.Send(msg2To1)
			time.Sleep(time.Second) // Allow message to be delivered

			// Verify message was received
			mockRaft1.mu.Lock()
			trans1RecMsg, ok := mockRaft1.receivedMsg[2]
			mockRaft1.mu.Unlock()
			assert.True(t, ok, "Message should be received")
			assert.Equal(t, msg2To1, trans1RecMsg)
		})
	}
}

// Test sending a snapshot between transports
func TestTransport_SendSnapshot(t *testing.T) {
	for _, transportType := range []int{transportTypeTcp, transportTypeHttp, transportTypeGrpc} {
		transportName := "TCP"
		if transportType == transportTypeHttp {
			transportName = "HTTP"
		} else if transportType == transportTypeGrpc {
			transportName = "GRPC"
		}
		t.Run(transportName, func(t *testing.T) {
			trans1, mockRaft1 := newTestTransport(t, transportType, 1, "localhost:14200")
			defer trans1.Stop()

			trans2, mockRaft2 := newTestTransport(t, transportType, 2, "localhost:14201")
			defer trans2.Stop()

			// Add peer and prepare for snapshot
			trans1.AddPeer(2, "localhost:14201")

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
			trans1.SendSnapshot(snapMsg)

			// Allow time for snapshot to be processed
			time.Sleep(time.Second * 2)

			// Verify snapshot was received correctly
			assert.Equal(t, mockRaft1.snapshotReader.Metadata(), mockRaft2.metadata, "Snapshot metadata should match")
			for tag, data := range mockRaft1.snapshotFileData {
				assert.Equal(t, data, mockRaft2.snapshotFileData[tag], "Snapshot file data should match")
			}
			assert.Equal(t, snapMsg, mockRaft2.finishMsg, "Snapshot finish message should match")
		})
	}
}

// Test peer management operations
func TestTransport_PeerManagement(t *testing.T) {
	for _, transportType := range []int{transportTypeTcp, transportTypeHttp, transportTypeGrpc} {
		transportName := "TCP"
		if transportType == transportTypeHttp {
			transportName = "HTTP"
		} else if transportType == transportTypeGrpc {
			transportName = "GRPC"
		}

		t.Run(transportName, func(t *testing.T) {
			trans, _ := newTestTransport(t, transportType, 1, "localhost:14200")
			defer trans.Stop()

			// Test adding peers
			trans.AddPeer(2, "localhost:14201")
			trans.AddPeer(3, "localhost:14202")

			// Verify peer was added correctly
			addr, err := trans.peerMgr.ResolvePeerAddress(2)
			assert.Nil(t, err, "Should be able to get peer address")
			assert.Equal(t, "localhost:14201", addr)

			// Test updating peer
			trans.UpdatePeer(2, "localhost:14203")

			// Verify peer was updated
			addr, err = trans.peerMgr.ResolvePeerAddress(2)
			assert.Nil(t, err)
			assert.Equal(t, "localhost:14203", addr)

			// Test removing a specific peer
			trans.RemovePeer(3)

			// Verify peer was removed
			_, err = trans.peerMgr.ResolvePeerAddress(3)
			assert.NotNil(t, err, "Peer should be removed")

			// Test removing all peers
			trans.RemovePeers()

			// Verify all peers were removed
			_, err = trans.peerMgr.ResolvePeerAddress(2)
			assert.NotNil(t, err, "All peers should be removed")
		})
	}
}

// Test unreachable peer reporting
func TestTransport_UnreachablePeer(t *testing.T) {
	for _, transportType := range []int{transportTypeTcp, transportTypeHttp, transportTypeGrpc} {
		transportName := "TCP"
		if transportType == transportTypeHttp {
			transportName = "HTTP"
		} else if transportType == transportTypeGrpc {
			transportName = "GRPC"
		}

		t.Run(transportName, func(t *testing.T) {
			trans1, mockRaft1 := newTestTransport(t, transportType, 1, "localhost:14200")
			defer trans1.Stop()

			// Add a non-existent peer to test unreachable reporting
			trans1.AddPeer(99, "localhost:99999")

			// Send a message to the unreachable peer
			msg := raftpb.Message{
				Type:  raftpb.MsgApp,
				To:    99,
				From:  1,
				Term:  1,
				Index: 1,
			}
			trans1.Send(msg)

			// Allow time for unreachable report
			time.Sleep(time.Second * 2)

			// Verify the peer was reported as unreachable
			mockRaft1.mu.Lock()
			_, unreachable := mockRaft1.unreachable[99]
			mockRaft1.mu.Unlock()
			assert.True(t, unreachable, "Peer should be reported as unreachable")
		})
	}
}

// Test updating a peer's address and continuing to communicate
func TestTransport_UpdatePeerAndCommunicate(t *testing.T) {
	for _, transportType := range []int{transportTypeTcp, transportTypeHttp, transportTypeGrpc} {
		transportName := "TCP"
		if transportType == transportTypeHttp {
			transportName = "HTTP"
		} else if transportType == transportTypeGrpc {
			transportName = "GRPC"
		}

		t.Run(transportName, func(t *testing.T) {
			trans1, _ := newTestTransport(t, transportType, 1, "localhost:14200")
			defer trans1.Stop()

			// Start the second transport at a different address
			trans2, mockRaft2 := newTestTransport(t, transportType, 2, "localhost:14201")
			defer trans2.Stop()

			// First add the peer with a wrong address
			trans1.AddPeer(2, "localhost:12345")

			// Update the peer with the correct address
			trans1.UpdatePeer(2, "localhost:14201")
			time.Sleep(time.Second) // Allow update to take effect

			// Send a message to the updated peer
			msg := raftpb.Message{
				Type:  raftpb.MsgApp,
				To:    2,
				From:  1,
				Term:  1,
				Index: 1,
			}
			trans1.Send(msg)

			// Allow time for message to be delivered
			time.Sleep(time.Second)

			// Verify message was received after the peer update
			mockRaft2.mu.Lock()
			trans2RecMsg, ok := mockRaft2.receivedMsg[1]
			mockRaft2.mu.Unlock()

			assert.True(t, ok, "Message should be received after peer update")
			assert.Equal(t, msg, trans2RecMsg)
		})
	}
}
