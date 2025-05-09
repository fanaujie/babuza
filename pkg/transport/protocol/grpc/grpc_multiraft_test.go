package grpc

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/networkio"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/pb"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type multiRaftResolver string

func (r multiRaftResolver) ResolvePeerAddress(peerID uint64) (string, error) {
	return string(r), nil
}

type mockMultiRaftNodeHandler struct {
	nodesMsg         map[uint64]*nodeMultiMsg
	notifyNodeDoneCh chan *nodeMultiMsg
	clusterRes       babuzapb.GetClusterPeersResponse
	mu               sync.Mutex
}

func newMockMultiRaftNodeHandler(nodes int) *mockMultiRaftNodeHandler {
	return &mockMultiRaftNodeHandler{
		nodesMsg:         make(map[uint64]*nodeMultiMsg),
		notifyNodeDoneCh: make(chan *nodeMultiMsg, nodes),
	}
}

func (m *mockMultiRaftNodeHandler) setupMsgCount(node uint64, msgCount int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodesMsg[node] = &nodeMultiMsg{
		nodeId:        node,
		batchMsg:      []babuzapb.MultiRaftBatchMessage{},
		snapshotMsg:   make(map[uint64]babuzapb.SnapshotMessage),
		totalMsgCount: msgCount,
	}
}

func (m *mockMultiRaftNodeHandler) ProcessBatchMessage(message babuzapb.BatchMessage) {

}

func (m *mockMultiRaftNodeHandler) ProcessMultiRaftMessage(message babuzapb.MultiRaftBatchMessage) {
	m.mu.Lock()
	nodeId := message.Messages[0].Message.From
	n := m.nodesMsg[nodeId]
	n.batchMsg = append(n.batchMsg, message)
	n.receiveMsgCount++
	if n.receiveMsgCount == n.totalMsgCount {
		delete(m.nodesMsg, nodeId)
		m.notifyNodeDoneCh <- n
	}
	m.mu.Unlock()
}

func (m *mockMultiRaftNodeHandler) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
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

func (m *mockMultiRaftNodeHandler) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return m.clusterRes
}

func (m *mockMultiRaftNodeHandler) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func TestMultiRaftNewServerClient(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
		clientTls ibabuza.TLSConfig
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15200",
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
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15200",
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
		},
	}

	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		srv := NewMultiRaftMsgServer(c.TransportConfig, c.NetworkIO, nil, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
		pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
		client := NewMultiRaftMsgClient(pool, multiRaftResolver(c.PeerAddress), defaultGrpcCfg, &logger.Mock{})
		_, err := client.getConnection(0)
		assert.Nil(t, err, identify)
		pool.Close()
		srv.Stop()
	}
}

func TestMultiRaftServer_StartAndStop(t *testing.T) {
	local := "localhost:15200"
	n := networkio.NewGrpcNetworkIO()
	srv := NewMultiRaftMsgServer(ibabuza.TransportConfig{
		PeerAddress: local}, n, nil, &logger.Mock{})
	assert.Nil(t, srv.Start())
	srv.Stop()
}

func TestMultiRaftSingleServerClient_SendAndReceive(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
		clientTls         ibabuza.TLSConfig
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15201",
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
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15202",
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
		mockTransport := newMockMultiRaftNodeHandler(1)
		srv := NewMultiRaftMsgServer(c.TransportConfig, c.NetworkIO, mockTransport, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
		pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
		client := NewMultiRaftMsgClient(pool, multiRaftResolver(c.PeerAddress), defaultGrpcCfg, &logger.Mock{})

		tms := genTestMultiMsg(c.totalMsgCount, c.batchRaftMsgCount, 1)
		mockTransport.setupMsgCount(1, len(tms))

		for index, tm := range tms {
			if tm.batchMsg != nil {
				assert.Nil(t, client.SendMultiRaftMessage(*tm.batchMsg), identify)
			} else if tm.snapMsg != nil {
				_, err := client.SendSnapshotMessage(*tm.snapMsg)
				assert.Nil(t, err, identify)
			}

			res := babuzapb.GetClusterPeersResponse{
				Status:  babuzapb.SUCCESS,
				Message: "success",
				Peers: []babuzapb.Peer{
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							Id:             uint64(index),
							RaftListenAddr: c.PeerAddress,
							IsLearner:      false,
						},
					},
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							Id:             uint64(index + 1),
							RaftListenAddr: "localhost:15299",
							IsLearner:      true,
						},
					},
				},
			}
			mockTransport.clusterRes = res
			getRes, _ := client.GetClusterPeers(babuzapb.GetClusterPeersRequest{ClusterID: 100, To: 1})
			assert.Equal(t, res, getRes, identify)
		}

		nodeDoneMsg := <-mockTransport.notifyNodeDoneCh
		nodeDoneMsg.check(t, identify, tms)
		client.Close()
		srv.Stop()
	}
}

func TestMultiRaftSingleServerMultiClient_SendAndReceive(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
		clientTls         ibabuza.TLSConfig
		clients           int
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			clients:           8,
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15201",
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
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:15202",
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
			clients:           32,
			totalMsgCount:     512,
			batchRaftMsgCount: 64,
		},
	}

	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		mockTransport := newMockMultiRaftNodeHandler(c.clients)
		srv := NewMultiRaftMsgServer(c.TransportConfig, c.NetworkIO, mockTransport, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		allTms := make(map[int][]*testMultiMsg)
		wg := new(sync.WaitGroup)

		for n := 1; n <= c.clients; n++ {
			allTms[n] = genTestMultiMsg(c.totalMsgCount, c.batchRaftMsgCount, uint64(n))
			mockTransport.setupMsgCount(uint64(n), len(allTms[n]))
		}

		for n := 1; n <= c.clients; n++ {
			wg.Add(1)
			go func(n int, tms []*testMultiMsg) {
				defer wg.Done()
				creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
				pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
				client := NewMultiRaftMsgClient(pool, multiRaftResolver(c.PeerAddress), defaultGrpcCfg, &logger.Mock{})
				defer client.Close()

				for _, tm := range tms {
					if tm.batchMsg != nil {
						assert.Nil(t, client.SendMultiRaftMessage(*tm.batchMsg), identify)
					} else if tm.snapMsg != nil {
						_, err := client.SendSnapshotMessage(*tm.snapMsg)
						assert.Nil(t, err, identify)
					}
				}
			}(n, allTms[n])
		}

		wg.Wait()

		for n := 1; n <= c.clients; n++ {
			nodeDoneMsg := <-mockTransport.notifyNodeDoneCh
			nodeDoneMsg.check(t, identify, allTms[int(nodeDoneMsg.nodeId)])
		}

		srv.Stop()
	}
}

func TestMultiRaftMessageStream(t *testing.T) {
	local := "localhost:15203"
	n := networkio.NewGrpcNetworkIO()
	mockTransport := newMockMultiRaftNodeHandler(1)

	srv := NewMultiRaftMsgServer(ibabuza.TransportConfig{
		PeerAddress: local,
		PeerId:      1,
	}, n, mockTransport, &logger.Mock{})

	assert.Nil(t, srv.Start())
	defer srv.Stop()

	creator := NewConnectionCreator(n, ibabuza.TLSConfig{}, defaultPoolCfg)
	pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
	client := NewMultiRaftMsgClient(pool, multiRaftResolver(local), defaultGrpcCfg, &logger.Mock{})
	defer client.Close()

	stream1, err := client.GetStream(2)
	assert.Nil(t, err)
	assert.NotNil(t, stream1)

	stream2, err := client.GetStream(2)
	assert.Nil(t, err)
	assert.Equal(t, stream1, stream2)

	client.closeStream(2)

	stream3, err := client.GetStream(2)
	assert.Nil(t, err)
	assert.NotNil(t, stream3)
	assert.NotEqual(t, stream1, stream3)
}

type failConnCreator struct {
	NetworkIO
	shouldFail bool
	failCount  int
	mu         sync.Mutex
}

func (f *failConnCreator) Dial(target string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.shouldFail {
		f.failCount++
		return nil, fmt.Errorf("simulated connection failure")
	}
	return f.NetworkIO.Dial(ibabuza.TLSConfig{}, 0, target)
}

type mockServerBehavior struct {
	pb.MultiRaftTransportServer
	terminateStreams bool
	streamCount      int
	streamMu         sync.Mutex
}

func (m *mockServerBehavior) SendMultiRaftMessage(stream pb.MultiRaftTransport_SendMultiRaftMessageServer) error {
	m.streamMu.Lock()
	m.streamCount++
	m.streamMu.Unlock()

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if m.terminateStreams {
			return fmt.Errorf("stream terminated by server")
		}

		if err = stream.Send(&emptypb.Empty{}); err != nil {
			return err
		}
	}
}

func TestMultiRaftClient_ErrorHandling(t *testing.T) {
	local := "localhost:15204"
	networkIO := networkio.NewGrpcNetworkIO()

	t.Run("ServerDisconnection", func(t *testing.T) {
		mockHandler := &mockServerBehavior{}

		srv := NewMultiRaftMsgServer(ibabuza.TransportConfig{
			PeerAddress: local,
			PeerId:      1,
		}, networkIO, nil, &logger.Mock{})

		var err error
		// start
		srv.server, err = srv.grpcNetwork.NewServer(srv.cfg.TLSConfig)
		assert.Nil(t, err)
		pb.RegisterMultiRaftTransportServer(srv.server, mockHandler)

		srv.listener, err = srv.grpcNetwork.Listen(srv.cfg.PeerAddress)
		assert.Nil(t, err)
		go func() {
			srv.server.Serve(srv.listener)
		}()

		creator := NewConnectionCreator(networkIO, ibabuza.TLSConfig{}, defaultPoolCfg)
		pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
		client := NewMultiRaftMsgClient(pool, multiRaftResolver(local), defaultGrpcCfg, &logger.Mock{})

		msg := babuzapb.MultiRaftBatchMessage{
			ClusterID: 1,
			Messages: []babuzapb.MultiRaftMessage{
				{
					GroupID: 1,
					Message: raftpb.Message{
						Type: raftpb.MsgApp,
						To:   2,
						From: 1,
					},
				},
			},
		}

		assert.Nil(t, client.SendMultiRaftMessage(msg))
		mockHandler.terminateStreams = true
		// The client will not receive any response
		// because the stream is terminated by the server
		// The goroutine handling the stream will receive an error and clean up resources
		assert.Nil(t, client.SendMultiRaftMessage(msg))

		//wait for the stream to be removed
		time.Sleep(time.Second)

		client.streamMu.RLock()
		_, streamExists := client.streamCache[2]
		_, connExists := client.streamConn[2]
		client.streamMu.RUnlock()

		assert.False(t, streamExists, "Stream should be removed after failure")
		assert.False(t, connExists, "Connection should be removed after failure")

		client.Close()
		srv.Stop()
	})

	t.Run("ConnectionFailure", func(t *testing.T) {
		failCreator := &failConnCreator{
			NetworkIO: networkIO,
		}

		pool := connpool.NewConnectionPool(failCreator, defaultPoolCfg)
		client := NewMultiRaftMsgClient(pool, multiRaftResolver(local), defaultGrpcCfg, &logger.Mock{})

		msg := babuzapb.MultiRaftBatchMessage{
			ClusterID: 1,
			Messages: []babuzapb.MultiRaftMessage{
				{
					GroupID: 1,
					Message: raftpb.Message{
						Type: raftpb.MsgApp,
						To:   3,
						From: 1,
					},
				},
			},
		}

		failCreator.shouldFail = true

		err := client.SendMultiRaftMessage(msg)
		assert.NotNil(t, err)
		assert.Equal(t, 1, failCreator.failCount, "Connection creation should fail exactly once")

		client.streamMu.RLock()
		streamCacheSize := len(client.streamCache)
		client.streamMu.RUnlock()

		assert.Equal(t, 0, streamCacheSize, "Stream cache should be empty after connection failure")

		client.Close()
	})
}

type nodeMultiMsg struct {
	nodeId          uint64
	batchMsg        []babuzapb.MultiRaftBatchMessage
	snapshotMsg     map[uint64]babuzapb.SnapshotMessage
	receiveMsgCount int
	totalMsgCount   int
}

func (m *nodeMultiMsg) check(t *testing.T, identify string, tms []*testMultiMsg) {
	nextIndex := uint64(0)
	for _, tm := range tms {
		if tm.batchMsg != nil {
			assert.EqualValues(t, *tm.batchMsg, m.batchMsg[nextIndex], identify)
			nextIndex++
		} else if tm.snapMsg != nil {
			snapMsg, ok := m.snapshotMsg[tm.snapMsg.Index]
			assert.Equal(t, true, ok, identify)
			assert.EqualValues(t, *tm.snapMsg, snapMsg, identify)
		}
	}
}

type testMultiMsg struct {
	batchMsg *babuzapb.MultiRaftBatchMessage
	snapMsg  *babuzapb.SnapshotMessage
}

func genTestMultiMsg(totalMsgs, maxRaftMsgs int, fromNode uint64) []*testMultiMsg {
	r := make([]*testMultiMsg, totalMsgs)
	var startIndex uint64 = 1
	for i := 0; i < totalMsgs; i++ {
		isBatch := rand.Intn(100)%2 == 0
		if isBatch {
			msgs := genMultiRaftMsg(maxRaftMsgs, startIndex, fromNode)
			startIndex = msgs[len(msgs)-1].Message.Index + 1
			r[i] = &testMultiMsg{
				batchMsg: &babuzapb.MultiRaftBatchMessage{
					ClusterID: 1,
					Messages:  msgs,
				},
			}
		} else {
			startIndex += uint64(i)
			r[i] = &testMultiMsg{
				snapMsg: &babuzapb.SnapshotMessage{
					From:  fromNode,
					To:    1,
					Index: startIndex,
				},
			}
		}
	}
	return r
}

func genMultiRaftMsg(maxMsgs int, startIndex, fromNode uint64) []babuzapb.MultiRaftMessage {
	r := make([]babuzapb.MultiRaftMessage, maxMsgs)
	for i := 0; i < maxMsgs; i++ {
		r[i] = babuzapb.MultiRaftMessage{
			GroupID: 1,
			Message: raftpb.Message{
				From:  fromNode,
				To:    1,
				Index: startIndex + uint64(i),
			},
		}
	}
	return r
}
