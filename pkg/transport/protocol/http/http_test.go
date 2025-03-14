package http

import (
	"crypto/tls"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"math/rand"
	"net"
	"sync"
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

func (m *MockTransportResolver) ResolvePeerAddress(peerId uint64) (string, error) {
	if addr, ok := m.addressMap[peerId]; ok {
		return addr, nil
	}
	return "localhost:14200", nil // Default for testing
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
				assert.Nil(t, client.SendSnapshotMessage(*tm.snapMsg), identify)
			}

			res := babuzapb.GetClusterPeersResponse{
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
			mr.clusterRes = res
			getRes := client.GetClusterPeers(babuzapb.GetClusterPeersRequest{ClusterId: 100})
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
						assert.Nil(t, client.SendSnapshotMessage(*tm.snapMsg), identify)
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
