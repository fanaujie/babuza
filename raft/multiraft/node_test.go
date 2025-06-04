package multiraft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type CounterOperationType string

const (
	ClusterID          uint64               = 10000
	CounterSnapshotTag                      = "counter-state-machine"
	Increment          CounterOperationType = "increment"
	Decrement          CounterOperationType = "decrement"
	Reset              CounterOperationType = "reset"
	GetValue           CounterOperationType = "get"

	//session type
	NoOPSessionType        int = 0
	LruSessionType         int = 1
	TimeExpiredSessionType int = 2
)

type CounterCommand struct {
	Operation CounterOperationType `json:"operation"`
	Value     int64                `json:"value,omitempty"`
}

type CounterResult struct {
	Operation CounterOperationType `json:"operation"`
	Success   bool                 `json:"success"`
	Value     int64                `json:"value"`
}

type ResultSerializer struct {
	buf []byte
}

func NewResultSerializer() *ResultSerializer {
	return &ResultSerializer{buf: make([]byte, 8)}
}

func (s *ResultSerializer) Serialize(w io.Writer, res any) error {
	result, ok := res.(*CounterResult)
	if !ok {
		return errors.New("can not cast res to a pointer to valid response")
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(s.buf, uint64(len(data)))
	if _, err = w.Write(s.buf); err != nil {
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return nil
}

func (s *ResultSerializer) Deserialize(r io.Reader) (any, error) {
	if _, err := io.ReadFull(r, s.buf); err != nil {
		return nil, err
	}
	var res any
	dataLen := binary.LittleEndian.Uint64(s.buf)
	buf := make([]byte, dataLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, res); err != nil {
		return nil, err
	}
	return res, nil
}

type SimpleStateMachine struct {
	counter int64
	mu      sync.RWMutex
}

func NewSimpleStateMachine() *SimpleStateMachine {
	return &SimpleStateMachine{
		counter: 0,
	}
}

func (s *SimpleStateMachine) Apply(entry ibabuza.Entry) ibabuza.ApplyResult {
	var cmd CounterCommand

	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		return ibabuza.ApplyResult{
			LogIndex: entry.Index,
			Error:    err,
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result CounterResult
	result.Operation = cmd.Operation
	result.Success = true

	switch cmd.Operation {
	case Increment:
		s.counter += cmd.Value
		result.Value = s.counter
	case Decrement:
		s.counter -= cmd.Value
		result.Value = s.counter
	case Reset:
		s.counter = cmd.Value
		result.Value = s.counter
	case GetValue:
		result.Value = s.counter
	default:
		return ibabuza.ApplyResult{
			LogIndex: entry.Index,
			Error:    errors.New("unknown counter operation"),
		}
	}

	return ibabuza.ApplyResult{
		LogIndex: entry.Index,
		Response: &result,
	}
}

func (s *SimpleStateMachine) SaveSnapshot(ctx ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	wc, err := writer.CreateStateMachineFile(CounterSnapshotTag, babuzapb.SnapshotFileCompression_None)
	if err != nil {
		return err
	}
	defer wc.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(s.counter))

	_, err = wc.Write(buf)
	return err
}

func (s *SimpleStateMachine) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, _, err := reader.Open(CounterSnapshotTag)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return err
	}

	s.counter = int64(binary.LittleEndian.Uint64(buf))
	return nil
}

func (s *SimpleStateMachine) Query(key any) (any, error) {
	return nil, nil
}

func (s *SimpleStateMachine) Close() error {
	return nil
}

func (s *SimpleStateMachine) Counter() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counter
}

func (s *SimpleStateMachine) GetResponseSerializer() ibabuza.ResponseSerializer {
	return NewResultSerializer()
}

type componentFactory struct {
	logger      ibabuza.Logger
	sessionType int
}

func newComponentFactory(sessionType int) *componentFactory {
	z, _ := zap.NewProduction(zap.AddCallerSkip(1))
	log := logger.NewRaftLogger(z.Sugar())
	return &componentFactory{
		logger:      log,
		sessionType: sessionType,
	}
}

func (c *componentFactory) CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error) {
	return NewSimpleStateMachine(), nil
}

func (c *componentFactory) CreateCluster() ibabuza.Cluster {
	return cluster.NewCluster(c.logger)
}

func (c *componentFactory) CreateSessionManager() ibabuza.SessionManager {
	switch c.sessionType {
	case NoOPSessionType:
		return session.NewNoOpManager(c.logger)
	case LruSessionType:
		return session.NewLruManager(c.logger)
	case TimeExpiredSessionType:
		return session.NewExpiredManager(c.logger)
	default:
		panic("unknown session type")
	}
}

func (c *componentFactory) GetLogger() ibabuza.Logger {
	return c.logger
}

type customConfig struct {
	EnableWalNoSync              bool
	SnapshotCount                uint64
	RaftConfig                   babuza.RaftConfig
	LearnerReadyPercent          float64
	CoalescedHeartbeatQueueSize  uint64
	TransportPeerQueueSize       int64
	TransportHeartbeatBufferSize int
	// setup raftScheduler
	SchedulerShardNum       int
	SchedulerShardWorkerNum int
	SchedulerQueueSize      uint64
}

func defaultCustomConfig() customConfig {
	return customConfig{
		EnableWalNoSync:              false,
		SnapshotCount:                10000,
		RaftConfig:                   babuza.DefaultRaftConfig(),
		LearnerReadyPercent:          0.95,
		CoalescedHeartbeatQueueSize:  512,
		TransportPeerQueueSize:       512,
		TransportHeartbeatBufferSize: 512,
		SchedulerShardNum:            2,
		SchedulerShardWorkerNum:      3,
		SchedulerQueueSize:           64,
	}
}

type nodeConfig struct {
	nodeID         uint64
	raftListenAddr string
}

func createNodeManager(nodeConfigs []nodeConfig, cuConfig customConfig, rootDir string,
	factory ComponentsFactory) (*NodeManager, error) {

	nm := NewNodeManager()
	for _, config := range nodeConfigs {
		node, err := createNode(ClusterID, config.nodeID, cuConfig, config.raftListenAddr,
			filepath.Join(rootDir, fmt.Sprintf("%d", config.nodeID)), factory)
		if err != nil {
			return nil, err
		}
		if err = nm.Add(node); err != nil {
			return nil, err
		}
	}
	return nm, nil
}

func createNode(clusterID uint64, nodeID uint64, cuConfig customConfig, nodeRaftListenAddr string,
	rootDir string, factory ComponentsFactory) (*Node, error) {
	config := DefaultNodeConfig(clusterID, nodeID, rootDir, nodeRaftListenAddr)
	config.EnableWalNoSync = cuConfig.EnableWalNoSync
	config.SnapshotCount = cuConfig.SnapshotCount
	config.RaftConfig = cuConfig.RaftConfig
	config.LearnerReadyPercent = cuConfig.LearnerReadyPercent
	config.CoalescedHeartbeatQueueSize = cuConfig.CoalescedHeartbeatQueueSize
	config.SchedulerShardNum = cuConfig.SchedulerShardNum
	config.SchedulerShardWorkerNum = cuConfig.SchedulerShardWorkerNum
	config.SchedulerQueueSize = cuConfig.SchedulerQueueSize
	log := factory.GetLogger()
	walMgr := lsmtwal.NewMultiRaftBadgerWalManager(lsmtwal.MultiRaftConfig{
		InMemory:           false,
		WalDir:             filepath.Join(rootDir, "wal"),
		KeyPrefixCacheSize: 1024,
	}, log)
	snapshotMgr := snapshot.NewMultiRaftSnapshotManager(snapshot.Config{
		SnapshotVersion: 1,
		MaxSnapFiles:    3,
		SnapshotDir:     filepath.Join(rootDir, "snapshot"),
	}, durable.NewSnapshotFS(), log)
	trans := transport.NewMultiRaftTransport(clusterID,
		transport.NewPeerManager[peer.MultiRaftPeer](), limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), protocol.NewGrpcMultiRaft(log), log,
		transport.SetTransportOptionsWithPeerQueueSize(cuConfig.TransportPeerQueueSize),
		transport.SetTransportOptionsWithHeartbeatBufferSize(cuConfig.TransportHeartbeatBufferSize))

	return BootstrapOrRecoverNode(config, factory, trans, walMgr, snapshotMgr, nil)
}

func proposeCommand(node *Node, groupID ibabuza.RaftGroupID, cmd CounterCommand) (*CounterResult, error) {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	res := node.Propose(context.Background(), groupID, babuza.ClientSession{}, cmdBytes)
	ar := res.WaitForApplyResult()
	defer res.Release()
	if ar.Error != nil {
		return nil, ar.Error
	}
	result, ok := ar.Response.(*CounterResult)
	if !ok {
		return nil, errors.New("invalid response type")
	}
	return result, nil
}

func verifyCounterValue(nm *NodeManager, groupID ibabuza.RaftGroupID, expectedValue int64) error {
	c := int64(0)
	init := false

	for _, nodeID := range nm.GetNodeIDsByGroupID(groupID) {
		node, err := nm.GetNode(nodeID)
		if err != nil {
			return err
		}
		stateMachine, err := node.StateMachine(groupID)
		if err != nil {
			return err
		}
		counterStateMachine, ok := stateMachine.(*SimpleStateMachine)
		if !ok {
			return errors.New("invalid state machine type")
		}
		if !init {
			c = counterStateMachine.Counter()
			init = true
		} else {
			if counterStateMachine.Counter() != c {
				return errors.New("counter value mismatch")
			}
		}
	}
	if c != expectedValue {
		return errors.New("counter value mismatch")
	}
	return nil
}

func TestBootstrap(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	raftGroup2 := ibabuza.RaftGroupID(11)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	group1PeersConfig.RemovePeer(3)
	group2PeersConfig := peersConfig.Clone()
	group2PeersConfig.SetGroupID(raftGroup2)
	group2PeersConfig.RemovePeer(1)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group2PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(group2PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 2, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	assert.Equal(t, raftGroup2, groupIDs[1])
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup2, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 5)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)
	group2LeaderID, err := nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	t.Logf("group2 leader: %d", group2LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(10), result.Value)

	result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 5})
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result.Value)
	// wait for the command to be applied
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 15))

	group2LeaderNode, err := nm.GetNode(group2LeaderID)
	assert.NoError(t, err)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Reset, Value: 20})
	assert.NoError(t, err)
	assert.Equal(t, int64(20), result.Value)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Decrement, Value: 8})
	assert.NoError(t, err)
	assert.Equal(t, int64(12), result.Value)
	// wait for the command to be applied
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup2, 12))
}

func TestRecover(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	for _, n := range nm.GetAllNodes() {
		assert.NoError(t, n.Start())
	}
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	defer func() {
		for _, n := range nm.GetAllNodes() {
			n.Stop()
		}
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	raftGroup2 := ibabuza.RaftGroupID(11)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	group1PeersConfig.RemovePeer(3)
	group2PeersConfig := peersConfig.Clone()
	group2PeersConfig.SetGroupID(raftGroup2)
	group2PeersConfig.RemovePeer(1)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group2PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(group2PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 2, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	assert.Equal(t, raftGroup2, groupIDs[1])
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup2, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 3)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)
	group2LeaderID, err := nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	t.Logf("group2 leader: %d", group2LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(10), result.Value)

	result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 5})
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result.Value)

	group2LeaderNode, err := nm.GetNode(group2LeaderID)
	assert.NoError(t, err)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Reset, Value: 20})
	assert.NoError(t, err)
	assert.Equal(t, int64(20), result.Value)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Decrement, Value: 8})
	assert.NoError(t, err)
	assert.Equal(t, int64(12), result.Value)
	// wait for the command to be applied
	time.Sleep(time.Second)

	// remove all nodes
	for _, n := range nm.GetAllNodes() {
		n.Stop()
	}
	nm.Clear()

	nm, err = createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	for _, n := range nm.GetAllNodes() {
		assert.NoError(t, n.Start())
	}
	// wait for leader election
	time.Sleep(time.Second * 3)
	group1LeaderID, err = nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("rastart group1 leader: %d", group1LeaderID)
	group2LeaderID, err = nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	t.Logf("rastart group2 leader: %d", group2LeaderID)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 15))
	assert.NoError(t, verifyCounterValue(nm, raftGroup2, 12))
}

func TestJoinVotingGroup(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	group1PeersConfig.RemovePeer(3)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 3)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 50})
	assert.NoError(t, err)
	assert.Equal(t, int64(50), result.Value)

	result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(60), result.Value)
	// wait for the command to be applied
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 60))

	//node3 join group1
	peer3, _ := peersConfig.GetPeer(3)
	res := group1LeaderNode.AddVotingPeer(context.Background(), raftGroup1, babuza.ClientSession{}, peer3)
	ar := res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)
	assert.NoError(t, group1PeersConfig.AddPeer(3, "localhost:14203", false))
	assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, true))

	//wait for node3 to join group1 and apply the command
	time.Sleep(time.Second * 5)
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	lastGroup1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1LeaderID, lastGroup1LeaderID)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 60))
}

func TestRemoveVotingGroup(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 5)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	removeID := (group1LeaderID % 3) + 1
	remainIDs := make([]uint64, 0)
	for i := uint64(1); i <= 3; i++ {
		if i != removeID {
			remainIDs = append(remainIDs, i)
		}
	}
	//remove group1
	res := group1LeaderNode.RemovePeer(context.Background(), raftGroup1, babuza.ClientSession{}, removeID)
	ar := res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)

	//wait for removeNode to leave group1
	time.Sleep(time.Second)
	removeNode, err := nm.GetNode(removeID)
	assert.NoError(t, err)
	groupIDs = removeNode.GetGroupIDs()
	assert.Equal(t, 0, len(groupIDs))

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 50})
	assert.NoError(t, err)
	assert.Equal(t, int64(50), result.Value)
	//wait for  apply the command
	time.Sleep(time.Second)
	nodeIDs := nm.GetNodeIDsByGroupID(raftGroup1)
	assert.Equal(t, len(remainIDs), len(nodeIDs))
	for index, nodeID := range nodeIDs {
		assert.Equal(t, remainIDs[index], nodeID)
	}
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 50))
}

func TestJoinLearnerAndPromoteLearner(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", true)) // node3 設為 learner
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	group1PeersConfig.RemovePeer(3)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 5)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 100})
	assert.NoError(t, err)
	assert.Equal(t, int64(100), result.Value)

	result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 7})
	assert.NoError(t, err)
	assert.Equal(t, int64(107), result.Value)
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 107))

	peer3, _ := peersConfig.GetPeer(3)
	res := group1LeaderNode.AddLearner(context.Background(), raftGroup1, babuza.ClientSession{}, peer3)
	ar := res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)
	assert.NoError(t, group1PeersConfig.AddPeer(3, "localhost:14203", true))
	assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, true))

	time.Sleep(time.Second)
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	lastGroup1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1LeaderID, lastGroup1LeaderID)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 107))

	cfg, _ := group1LeaderNode.Configuration(groupIDs[0])
	for _, peer := range cfg.Peers {
		if peer.RaftPeerAttr.PeerID == peer3.PeerID {
			assert.True(t, peer.RaftPeerAttr.IsLearner)
		}
	}

	res = group1LeaderNode.PromoteLearner(context.Background(), raftGroup1, babuza.ClientSession{}, peer3.PeerID)
	ar = res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)

	cfg, _ = group1LeaderNode.Configuration(groupIDs[0])
	for _, peer := range cfg.Peers {
		if peer.RaftPeerAttr.PeerID == peer3.PeerID {
			assert.False(t, peer.RaftPeerAttr.IsLearner)
		}
	}
}

func TestTransferLeader(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	raftGroup2 := ibabuza.RaftGroupID(11)
	group2PeersConfig := peersConfig.Clone()
	group2PeersConfig.SetGroupID(raftGroup2)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node1.CreateRaftGroup(group2PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group2PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(group2PeersConfig, false))

	//wait for leader election
	time.Sleep(time.Second * 5)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)
	group2LeaderID, err := nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	t.Logf("group2 leader: %d", group2LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	group2LeaderNode, err := nm.GetNode(group2LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 10})
	assert.NoError(t, err)
	assert.Equal(t, int64(10), result.Value)

	result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 5})
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result.Value)

	assert.NoError(t, err)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Reset, Value: 20})
	assert.NoError(t, err)
	assert.Equal(t, int64(20), result.Value)
	result, err = proposeCommand(group2LeaderNode, raftGroup2, CounterCommand{Operation: Decrement, Value: 8})
	assert.NoError(t, err)
	assert.Equal(t, int64(12), result.Value)

	// wait for the command to be applied
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 15))
	assert.NoError(t, verifyCounterValue(nm, raftGroup2, 12))

	// Transfer leadership
	group1Transferee := (group1LeaderID % 3) + 1
	group2Transferee := (group2LeaderID % 3) + 1
	result1 := node1.TransferLeader(context.Background(), raftGroup1, group1Transferee)
	result2 := node2.TransferLeader(context.Background(), raftGroup2, group2Transferee)
	assert.NoError(t, result1.Wait())
	assert.NoError(t, result2.Wait())

	// check if the new leader is the one we transferred to
	group1LeaderID2, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1Transferee, group1LeaderID2)
	group2LeaderID2, err := nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	assert.Equal(t, group2Transferee, group2LeaderID2)
}

func TestSnapshot(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	cuConfig := defaultCustomConfig()
	cuConfig.SnapshotCount = 50
	nm, err := createNodeManager(nodeCfgs, cuConfig, rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.SetGroupID(raftGroup1)
	group1PeersConfig.RemovePeer(3)
	assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
	groupIDs := node1.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs = node2.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	//wait for leader election
	time.Sleep(time.Second * 5)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)

	group1LeaderNode, err := nm.GetNode(group1LeaderID)
	assert.NoError(t, err)

	result, err := proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Reset, Value: 50})
	assert.NoError(t, err)
	assert.Equal(t, int64(50), result.Value)
	// trigger snapshot
	for i := uint64(0); i < cuConfig.SnapshotCount; i++ {
		result, err = proposeCommand(group1LeaderNode, raftGroup1, CounterCommand{Operation: Increment, Value: 1})
		assert.NoError(t, err)
		assert.Equal(t, int64(51+i), result.Value)
	}
	// wait for the command to be applied
	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 100))
	for _, n := range nm.GetAllNodes() {
		n.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
			assert.Equal(t, cuConfig.SnapshotCount, value.status.GetSnapshotIndex())
			return true
		})
	}

	//node3 join group1
	peer3, _ := peersConfig.GetPeer(3)
	res := group1LeaderNode.AddVotingPeer(context.Background(), raftGroup1, babuza.ClientSession{}, peer3)
	ar := res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)
	assert.NoError(t, group1PeersConfig.AddPeer(3, "localhost:14203", false))
	assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, true))

	//wait for node3 to join group1 and apply the command
	time.Sleep(time.Second)
	for _, n := range nm.GetAllNodes() {
		n.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
			assert.Equal(t, cuConfig.SnapshotCount, value.status.GetSnapshotIndex())
			return true
		})
	}
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	lastGroup1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1LeaderID, lastGroup1LeaderID)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 100))

	//restart node3 from snapshot
	node3.Stop()
	assert.NoError(t, nm.Remove(3))

	node3, err = createNode(ClusterID, nodeCfgs[2].nodeID, cuConfig, nodeCfgs[2].raftListenAddr,
		filepath.Join(rootDir, fmt.Sprintf("%d", nodeCfgs[2].nodeID)), newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NoError(t, node3.Start())
	assert.NoError(t, nm.Add(node3))

	time.Sleep(time.Second)
	for _, n := range nm.GetAllNodes() {
		n.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
			assert.Equal(t, cuConfig.SnapshotCount, value.status.GetSnapshotIndex())
			return true
		})
	}
	lastGroup1LeaderID, err = nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1LeaderID, lastGroup1LeaderID)
	assert.NoError(t, verifyCounterValue(nm, raftGroup1, 100))
}

func TestMultipleGroup(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	totalGroups := 1000
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	cuConfig := defaultCustomConfig()
	cuConfig.CoalescedHeartbeatQueueSize = 1024
	cuConfig.TransportPeerQueueSize = 1024 * 10
	cuConfig.TransportHeartbeatBufferSize = 1024
	cuConfig.SchedulerShardNum = 16
	cuConfig.SchedulerShardWorkerNum = 8
	cuConfig.SchedulerQueueSize = 128
	cuConfig.RaftConfig.ElectionTicks = 20
	cuConfig.RaftConfig.HeartbeatTicks = 3
	nm, err := createNodeManager(nodeCfgs, cuConfig, rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	wg1 := sync.WaitGroup{}
	for i := 0; i < totalGroups; i++ {
		wg1.Add(1)
		go func(index int, peer *PeersConfiguration) {
			defer wg1.Done()
			groupID := ibabuza.RaftGroupID(i + 1)
			group1PeersConfig := peersConfig.Clone()
			group1PeersConfig.SetGroupID(groupID)
			assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
			assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
			assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, false))
		}(i, peersConfig.Clone())
	}
	wg1.Wait()

	var leaderIDs map[ibabuza.RaftGroupID]uint64
	for {
		leaderIDs = make(map[ibabuza.RaftGroupID]uint64)
		for i := 0; i < totalGroups; i++ {
			groupID := ibabuza.RaftGroupID(i + 1)
			group1LeaderID, err := nm.CheckSameLeader(groupID)
			if err != nil {
				break
			}
			leaderIDs[groupID] = group1LeaderID
		}
		if len(leaderIDs) == totalGroups {
			for groupID, leaderID := range leaderIDs {
				t.Logf("Group %d leader: %d", groupID, leaderID)
			}
			break
		}
		time.Sleep(time.Second)
	}
	// Propose commands to all groups
	proposeCount := 100
	wg := sync.WaitGroup{}
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		leaderNode, err := nm.GetNode(leaderIDs[groupID])
		assert.NoError(t, err)
		wg.Add(1)
		go func(node *Node, gid ibabuza.RaftGroupID) {
			defer wg.Done()
			result, err := proposeCommand(node, gid, CounterCommand{Operation: Reset, Value: 0})
			assert.NoError(t, err)
			assert.Equal(t, int64(0), result.Value)
			for j := 0; j < proposeCount; j++ {
				r, pErr := proposeCommand(node, gid, CounterCommand{Operation: Increment, Value: 1})
				assert.NoError(t, pErr)
				assert.Equal(t, int64(j+1), r.Value)
			}
		}(leaderNode, groupID)
	}
	wg.Wait()
	// wait for the command to be applied
	time.Sleep(time.Second)
	// Verify the counter value for all groups
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		assert.NoError(t, verifyCounterValue(nm, groupID, int64(proposeCount)))
	}
	t.Logf("total groups: %d finish", totalGroups)
}

func TestRegisterSession(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()

	nm, err := createNodeManager(nodeCfgs, defaultCustomConfig(), rootDir, newComponentFactory(LruSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)

	for _, n := range nm.GetAllNodes() {
		assert.NoError(t, n.Start())
	}
	defer func() {
		for _, n := range nm.GetAllNodes() {
			n.Stop()
		}
	}()

	raftGroup := ibabuza.RaftGroupID(10)
	peersConfig.SetGroupID(raftGroup)
	for _, n := range nm.GetAllNodes() {
		assert.NoError(t, n.CreateRaftGroup(peersConfig, false))
	}

	// wait for leader election
	time.Sleep(time.Second * 3)
	leaderID, err := nm.CheckSameLeader(raftGroup)
	assert.NoError(t, err)
	t.Logf("Leader ID: %d", leaderID)

	leaderNode, err := nm.GetNode(leaderID)
	assert.NoError(t, err)

	ctx := context.Background()
	res := leaderNode.RegisterSession(ctx, raftGroup)
	ar := res.WaitForApplyResult()
	assert.NoError(t, ar.Error)
	sessionID1 := ar.LogIndex
	assert.Nil(t, ar.Error)
	res.Release()

	cmd1 := CounterCommand{Operation: Increment, Value: 1}
	cmdBytes1, err := json.Marshal(cmd1)
	assert.NoError(t, err)

	session1 := babuza.ClientSession{
		SessionID:      sessionID1,
		SequenceNumber: 1,
	}

	res = leaderNode.Propose(ctx, raftGroup, session1, cmdBytes1)
	ar = res.WaitForApplyResult()
	assert.Nil(t, ar.Error)
	result1, ok := ar.Response.(*CounterResult)
	assert.True(t, ok)
	assert.Equal(t, int64(1), result1.Value)
	res.Release()

	// Propose multiple commands with the same session ID and the same sequence number
	for i := 0; i < 5; i++ {
		res = leaderNode.Propose(ctx, raftGroup, session1, cmdBytes1)
		ar = res.WaitForApplyResult()
		res.Release()
		assert.Nil(t, ar.Error)
		result, ok := ar.Response.(*CounterResult)
		assert.True(t, ok)
		assert.Equal(t, int64(1), result.Value)
	}

	session1.SequenceNumber = 2
	cmd2 := CounterCommand{Operation: Increment, Value: 10}
	cmdBytes2, err := json.Marshal(cmd2)
	assert.NoError(t, err)

	res = leaderNode.Propose(ctx, raftGroup, session1, cmdBytes2)
	ar = res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)
	result2, ok := ar.Response.(*CounterResult)
	assert.True(t, ok)
	assert.Equal(t, int64(11), result2.Value)

	res = leaderNode.RegisterSession(ctx, raftGroup)
	ar = res.WaitForApplyResult()
	sessionID2 := ar.LogIndex
	res.Release()
	assert.Nil(t, ar.Error)

	session2 := babuza.ClientSession{
		SessionID:      sessionID2,
		SequenceNumber: 1,
	}

	cmd3 := CounterCommand{Operation: Reset, Value: 50}
	cmdBytes3, err := json.Marshal(cmd3)
	assert.NoError(t, err)

	res = leaderNode.Propose(ctx, raftGroup, session2, cmdBytes3)
	ar = res.WaitForApplyResult()
	res.Release()
	assert.Nil(t, ar.Error)
	result3, ok := ar.Response.(*CounterResult)
	assert.True(t, ok)
	assert.Equal(t, int64(50), result3.Value)

	time.Sleep(time.Second)
	assert.NoError(t, verifyCounterValue(nm, raftGroup, 50))

	// unregister session
	res = leaderNode.UnregisterSession(ctx, raftGroup, sessionID1)
	ar = res.WaitForApplyResult()
	assert.Nil(t, ar.Error)
	assert.True(t, ar.LogIndex > sessionID1)
	res.Release()

	// Propose a command with the unregistered session ID
	session1.SequenceNumber = 3
	cmd4 := CounterCommand{Operation: Increment, Value: 5}
	cmdBytes4, err := json.Marshal(cmd4)
	assert.NoError(t, err)
	res = leaderNode.Propose(ctx, raftGroup, session1, cmdBytes4)
	ar = res.WaitForApplyResult()
	assert.Error(t, ar.Error)
	res.Release()
}

func TestLinearizableRead(t *testing.T) {
	nodeCfgs := []nodeConfig{
		{nodeID: 1, raftListenAddr: "localhost:14201"},
		{nodeID: 2, raftListenAddr: "localhost:14202"},
		{nodeID: 3, raftListenAddr: "localhost:14203"},
	}
	totalGroups := 10
	peersConfig := NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))
	rootDir := t.TempDir()
	cuConfig := defaultCustomConfig()
	nm, err := createNodeManager(nodeCfgs, cuConfig, rootDir, newComponentFactory(NoOPSessionType))
	assert.NoError(t, err)
	assert.NotNil(t, nm)
	assert.Equal(t, 3, len(nm.GetAllNodes()))
	node1, err := nm.GetNode(1)
	assert.NoError(t, err)
	node2, err := nm.GetNode(2)
	assert.NoError(t, err)
	node3, err := nm.GetNode(3)
	assert.NoError(t, err)
	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())
	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		group1PeersConfig := peersConfig.Clone()
		group1PeersConfig.SetGroupID(groupID)
		assert.NoError(t, node1.CreateRaftGroup(group1PeersConfig, false))
		assert.NoError(t, node2.CreateRaftGroup(group1PeersConfig, false))
		assert.NoError(t, node3.CreateRaftGroup(group1PeersConfig, false))
	}
	time.Sleep(time.Second * 3)
	leaderIDs := make(map[ibabuza.RaftGroupID]uint64)
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		groupIDs := node1.GetGroupIDs()
		assert.Equal(t, totalGroups, len(groupIDs))
		groupIDs = node2.GetGroupIDs()
		assert.Equal(t, totalGroups, len(groupIDs))
		groupIDs = node3.GetGroupIDs()
		assert.Equal(t, totalGroups, len(groupIDs))

		group1LeaderID, err := nm.CheckSameLeader(groupID)
		assert.NoError(t, err)
		leaderIDs[groupID] = group1LeaderID
		t.Logf("group%d leader: %d", groupID, group1LeaderID)
	}
	// Propose commands to all groups
	proposeCount := 100
	wg := sync.WaitGroup{}
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		leaderNode, err := nm.GetNode(leaderIDs[groupID])
		assert.NoError(t, err)
		wg.Add(1)
		go func(node *Node, gid ibabuza.RaftGroupID) {
			defer wg.Done()
			result, err := proposeCommand(node, gid, CounterCommand{Operation: Reset, Value: 0})
			assert.NoError(t, err)
			assert.Equal(t, int64(0), result.Value)
			for j := 0; j < proposeCount; j++ {
				r, pErr := proposeCommand(node, gid, CounterCommand{Operation: Increment, Value: 1})
				assert.NoError(t, pErr)
				assert.Equal(t, int64(j+1), r.Value)
			}
		}(leaderNode, groupID)
	}
	wg.Wait()
	// wait for the command to be applied
	time.Sleep(time.Second)
	for i := 0; i < totalGroups; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)
		assert.NoError(t, verifyCounterValue(nm, groupID, int64(proposeCount)))
		leaderNode, err := nm.GetNode(leaderIDs[groupID])
		assert.NoError(t, err)
		assert.Nil(t, leaderNode.LinearizableRead(context.Background(), groupID))
		fsm, err := leaderNode.StateMachine(groupID)
		assert.NoError(t, err)
		assert.Equal(t, int64(proposeCount), fsm.(*SimpleStateMachine).Counter())
	}
}
