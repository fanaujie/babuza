package grpc

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/networkio"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var (
	defaultPoolCfg = connpool.Config{
		MaxConnectionsPerHost: 1024,
		DialTimeout:           2 * time.Second,
		IdleTimeout:           5 * time.Minute,
	}
	defaultGrpcCfg = ClientConfig{
		GrpcDeadline: 3 * time.Second,
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

type peerAddressResolver string

func (r peerAddressResolver) ResolvePeerAddress(peerID uint64) (string, error) {
	return string(r), nil
}

type mockTransportRaft struct {
	nodesMsg         map[uint64]*nodeMsg
	notifyNodeDoneCh chan *nodeMsg
	clusterRes       babuzapb.GetClusterPeersResponse
	publishRes       babuzapb.PublishApplicationServiceResponse
	mu               sync.Mutex
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

func (m *mockTransportRaft) ProcessMultiRaftMessage(message babuzapb.MultiRaftBatchMessage) {
	// not supported
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

type ConnectionCreator struct {
	dialer    Dialer
	tlsConfig ibabuza.TLSConfig
	options   connpool.Config
}

func NewConnectionCreator(dialer Dialer, tlsConfig ibabuza.TLSConfig, options connpool.Config) *ConnectionCreator {
	return &ConnectionCreator{
		dialer:    dialer,
		tlsConfig: tlsConfig,
		options:   options,
	}
}

func (c *ConnectionCreator) Dial(address string) (*grpc.ClientConn, error) {
	grpcConn, err := c.dialer.DialWithTimeout(c.tlsConfig, 0, address, c.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return grpcConn, nil
}

func TestNewServerClient(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
		clientTls ibabuza.TLSConfig
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
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
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
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
		},
	}

	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		srv := NewRaftMsgServer(c.TransportConfig, c.NetworkIO, nil, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
		pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
		client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress), defaultGrpcCfg)
		_, err := client.getConnection(0)
		assert.Nil(t, err, identify)
		pool.Close()
		srv.Stop()
	}
}

func TestServer_StartAndStop(t *testing.T) {
	local := "localhost:14200"
	n := networkio.NewGrpcNetworkIO()
	srv := NewRaftMsgServer(ibabuza.TransportConfig{
		PeerAddress: local}, n, nil, &logger.Mock{})
	assert.Nil(t, srv.Start())
	srv.Stop()
}

func TestSingleServerClient_SendAndReceive(t *testing.T) {
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
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14201",
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
				PeerAddress: "localhost:14202",
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
		mockTransport := newMockTransportRaft(1)
		srv := NewRaftMsgServer(c.TransportConfig, c.NetworkIO, mockTransport, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
		pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
		client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress), defaultGrpcCfg)

		tms := genTestMsg(c.totalMsgCount, c.batchRaftMsgCount, 1)
		mockTransport.setupMsgCount(1, len(tms))

		for index, tm := range tms {
			if tm.batchMsg != nil {
				assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
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
							RaftListenAddr: "localhost:14299",
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

func TestSingleServerMultiClient_SendAndReceive(t *testing.T) {
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
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			clients:           8,
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewGrpcNetworkIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14201",
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
				PeerAddress: "localhost:14202",
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
		mockTransport := newMockTransportRaft(c.clients)
		srv := NewRaftMsgServer(c.TransportConfig, c.NetworkIO, mockTransport, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		allTms := make(map[int][]*testMsg)
		wg := new(sync.WaitGroup)

		for n := 1; n <= c.clients; n++ {
			allTms[n] = genTestMsg(c.totalMsgCount, c.batchRaftMsgCount, uint64(n))
			mockTransport.setupMsgCount(uint64(n), len(allTms[n]))
		}

		for n := 1; n <= c.clients; n++ {
			wg.Add(1)
			go func(n int, tms []*testMsg) {
				defer wg.Done()
				creator := NewConnectionCreator(c.NetworkIO, c.clientTls, defaultPoolCfg)
				pool := connpool.NewConnectionPool(creator, defaultPoolCfg)
				client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress), defaultGrpcCfg)
				defer client.Close()

				for _, tm := range tms {
					if tm.batchMsg != nil {
						assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
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

func genTestMsg(totalMsgs, maxRaftMsgs int, fromNode uint64) []*testMsg {
	r := make([]*testMsg, totalMsgs)
	var startIndex uint64 = 1
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
					To:    1,
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
			To:    1,
			Index: startIndex + uint64(i),
		}
	}
	return r
}
