package tcp

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/transport/protocol/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	defaultOpts = connpool.Options{
		WriteDeadline:         time.Second * 2,
		ReadDeadline:          time.Second * 2,
		MaxConnectionsPerHost: 5,
		DialTimeout:           1 * time.Second,
		IdleTimeout:           5 * time.Minute,
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

type byteSliceEncode struct {
	data []byte
}

func (m *byteSliceEncode) MarshalTo(dAtA []byte) (int, error) {
	copy(dAtA, m.data)
	return len(dAtA), nil
}

func (m *byteSliceEncode) Size() int {
	return len(m.data)
}

// 實現 ibabuza.TransportResolver 接口的類型
type peerAddressResolver string

func (r peerAddressResolver) ResolvePeerAddress(peerId uint64) (string, error) {
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

func (m *mockTransportRaft) ProcessSnapshotMessage(message babuzapb.SnapshotMessage) {
	m.mu.Lock()
	n := m.nodesMsg[message.From]
	n.snapshotMsg[message.Index] = message
	n.receiveMsgCount++
	if n.receiveMsgCount == n.totalMsgCount {
		delete(m.nodesMsg, message.From)
		m.notifyNodeDoneCh <- n
	}
	m.mu.Unlock()

}
func (m *mockTransportRaft) GetClusterPeersRequest(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return m.clusterRes
}

func (m *mockTransportRaft) PublishApplicationServiceRequest(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return m.publishRes
}

type ConnectionCreator struct {
	dialer    Dialer
	tlsConfig ibabuza.TLSConfig
	options   connpool.Options
}

func NewConnectionCreator(dialer Dialer, tlsConfig ibabuza.TLSConfig, options connpool.Options) *ConnectionCreator {
	return &ConnectionCreator{
		dialer:    dialer,
		tlsConfig: tlsConfig,
		options:   options,
	}
}

func (c *ConnectionCreator) Create(address string) (connpool.Connection, error) {
	netConn, err := c.dialer.DialWithTimeout(c.tlsConfig, 0, address, c.options.DialTimeout)
	if err != nil {
		return nil, err
	}
	return conn.NewConnection(netConn, c.options), nil
}

func TestNewServerClient(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewTcpMemoryIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: true,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
		},
	}

	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		srv := NewRaftMsgServer(c.TransportConfig, defaultOpts, c.NetworkIO, nil, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.TLSConfig, defaultOpts)
		pool := connpool.NewConnectionPool(creator, defaultOpts)
		client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress))
		_, err := client.getConnection(0)
		assert.Nil(t, err, identify)
		pool.Close()
		srv.Stop()
	}
}

func TestServer_StartAndStop(t *testing.T) {
	local := "localhost:14200"
	n := networkio.NewTcpPhysicalIO()
	srv := NewRaftMsgServer(ibabuza.TransportConfig{
		PeerAddress: local}, defaultOpts, n, nil, &logger.Mock{})
	assert.Nil(t, srv.Start())
	conns := make(map[int]net.Conn)
	for i := 0; i < 8; i++ {
		conn, err := n.DialWithTimeout(ibabuza.TLSConfig{}, 0, local, defaultOpts.DialTimeout)
		assert.Nil(t, err)
		conns[i] = conn
	}
	defer func() {
		for _, con := range conns {
			con.Close()
		}
	}()
	time.Sleep(time.Second) // trigger go routine
	assert.Equal(t, uint64(1+8), srv.closer.Count())
	assert.Nil(t, srv.Stop())
	assert.Equal(t, uint64(0), srv.closer.Count())
}

func TestServer_ServingReceiveMessage(t *testing.T) {
	wConn, rConn := net.Pipe()
	defer wConn.Close()
	defer rConn.Close()
	clientWriter := frame.NewWriter(wConn)
	raft := newMockTransportRaft(1)
	closer := syncutil.NewCloser()
	defer closer.Close()

	frameConn := conn.NewConnection(rConn, defaultOpts)
	s := &session{
		options:   defaultOpts,
		conn:      rConn,
		frameConn: frameConn,
		raft:      raft,
		closeCh:   closer.CloseCh(),
	}

	raft.setupMsgCount(1, 2)
	batchMsg := babuzapb.BatchMessage{
		Messages: []raftpb.Message{
			{
				From:  1,
				To:    1,
				Index: 1,
			},
		},
	}
	snapshotMsg := babuzapb.SnapshotMessage{
		From:  1,
		To:    1,
		Index: 1,
	}
	fakeMsg := byteSliceEncode{data: []byte{1, 2, 3, 4}}
	go func() {
		byteSlice := allocator.Acquire(batchMsg.Size() + snapshotMsg.Size() + fakeMsg.Size())
		defer allocator.Release(byteSlice)
		assert.Nil(t, clientWriter.Encode(byteSlice.Buffer, frame.BatchMsgType, &batchMsg))
		assert.Nil(t, clientWriter.Encode(byteSlice.Buffer, frame.SnapshotMsgType, &snapshotMsg))
		assert.Nil(t, clientWriter.Encode(byteSlice.Buffer, 3, &fakeMsg))
		// Close the closer to terminate the session's start method gracefully
		time.Sleep(500 * time.Millisecond)
		closer.Close()
	}()
	assert.Error(t, s.start())
	nodeDoneMsg := <-raft.notifyNodeDoneCh
	_, ok := nodeDoneMsg.batchMsg[1]
	assert.Equal(t, true, ok)
	_, ok = nodeDoneMsg.snapshotMsg[1]
	assert.Equal(t, true, ok)
}

func TestSingleServerClient_SendAndReceive(t *testing.T) {
	type testCase struct {
		NetworkIO
		ibabuza.TransportConfig
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewTcpMemoryIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
			totalMsgCount:     256,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: true,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
			totalMsgCount:     512,
			batchRaftMsgCount: 64,
		},
	}
	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		mockTransport := newMockTransportRaft(1)
		srv := NewRaftMsgServer(c.TransportConfig, defaultOpts, c.NetworkIO, mockTransport, &logger.Mock{})
		assert.Nil(t, srv.Start(), identify)

		creator := NewConnectionCreator(c.NetworkIO, c.TLSConfig, defaultOpts)
		pool := connpool.NewConnectionPool(creator, defaultOpts)
		client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress))
		tms := genTestMsg(c.totalMsgCount, c.batchRaftMsgCount, 1)
		mockTransport.setupMsgCount(1, len(tms))
		for index, tm := range tms {
			if tm.batchMsg != nil {
				assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
			} else if tm.snapMsg != nil {
				assert.Nil(t, client.SendSnapshotMessage(*tm.snapMsg), identify)
			}
			res := babuzapb.GetClusterPeersResponse{
				Status:  babuzapb.SUCCESS,
				Message: "success",
				Peers: []babuzapb.Peer{
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							Id:             uint64(index),
							RaftListenAddr: "localhost:14200",
							IsLearner:      false,
						},
					},
					{
						RaftPeerAttr: babuzapb.RaftPeerAttribute{
							Id:             uint64(index + 1),
							RaftListenAddr: "localhost:14201",
							IsLearner:      true,
						},
					},
				},
			}
			mockTransport.clusterRes = res
			getRes := client.GetClusterPeers(babuzapb.GetClusterPeersRequest{ClusterId: 100, ToId: 1})
			assert.Equal(t, res, getRes)
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
		clients           int
		totalMsgCount     int
		batchRaftMsgCount int
	}
	var tc = []testCase{
		{
			NetworkIO: networkio.NewTcpMemoryIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			clients:           8,
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig:   ibabuza.TLSConfig{},
			},
			clients:           8,
			totalMsgCount:     128,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
			clients:           16,
			totalMsgCount:     256,
			batchRaftMsgCount: 64,
		},
		{
			NetworkIO: networkio.NewTcpPhysicalIO(),
			TransportConfig: ibabuza.TransportConfig{
				PeerAddress: "localhost:14200",
				TLSConfig: ibabuza.TLSConfig{
					EnableTLS: true,
					MutualTLS: false,
					TLSCert:   "../../../../test/fixtures/babuza.pem",
					TLSKey:    "../../../../test/fixtures/babuza-key.pem",
					TLSRootCA: "../../../../test/fixtures/ca.pem",
				},
			},
			clients:           32,
			totalMsgCount:     512,
			batchRaftMsgCount: 64,
		},
	}
	for i, c := range tc {
		identify := fmt.Sprintf("case(%d)", i)
		mockTransport := newMockTransportRaft(c.clients)
		srv := NewRaftMsgServer(c.TransportConfig, defaultOpts, c.NetworkIO, mockTransport, &logger.Mock{})
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
				creator := NewConnectionCreator(c.NetworkIO, c.TLSConfig, defaultOpts)
				pool := connpool.NewConnectionPool(creator, defaultOpts)
				client := NewRaftMsgClient(pool, peerAddressResolver(c.PeerAddress))
				defer client.Close()
				for _, tm := range tms {
					if tm.batchMsg != nil {
						assert.Nil(t, client.SendBatchMessage(*tm.batchMsg), identify)
					} else if tm.snapMsg != nil {
						assert.Nil(t, client.SendSnapshotMessage(*tm.snapMsg), identify)
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
					To:    1, // 假設目標節點 ID 為 1
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
			To:    1, // 假設目標節點 ID 為 1
			Index: startIndex + uint64(i),
		}
	}
	return r
}
