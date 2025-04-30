package multiraft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"path/filepath"
	"testing"
	"time"
)

type simpleStateMachine struct {
}

func (s *simpleStateMachine) Apply(entry ibabuza.Entry) ibabuza.ApplyResult {
	return ibabuza.ApplyResult{}
}

func (s *simpleStateMachine) SaveSnapshot(context ibabuza.StateMachineSnapshotContext, writer ibabuza.StateMachineSnapshotWriter) error {
	return nil
}

func (s *simpleStateMachine) RestoreFromSnapshot(reader ibabuza.StateMachineSnapshotReader) error {
	return nil
}

func (s *simpleStateMachine) Close() error {
	return nil
}

type ComponentFactory struct {
}

func (c *ComponentFactory) CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID,
	logger ibabuza.Logger) (ibabuza.BaseStateMachine, error) {

	return &simpleStateMachine{}, nil
}

func (c *ComponentFactory) CreateCluster(logger ibabuza.Logger) ibabuza.Cluster {
	return cluster.NewCluster(logger)
}

func (c *ComponentFactory) CreateSessionManager(logger ibabuza.Logger) ibabuza.SessionManager {
	return session.NewNoOpManager(logger)
}

func createNodeManager(t *testing.T, configuration *babuza.PeersConfiguration) (*NodeManager, error) {
	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	nm := NewNodeManager()
	if err := configuration.Visit(func(attribute babuzapb.RaftPeerAttribute) error {
		node, err := createNode(100, attribute.Id, attribute.RaftListenAddr, t.TempDir())
		if err != nil {
			return err
		}
		return nm.Add(node)
	}); err != nil {
		return nil, err
	}
	return nm, nil
}

func createNode(clusterID uint64, nodeID uint64, nodeRaftListenAddr string, rootDir string) (*Node, error) {
	config := DefaultNodeConfig(clusterID, nodeID, rootDir, nodeRaftListenAddr)
	log := logger.NewRaftLogger(zap.NewExample().Sugar())
	walMgr := babuzawal.NewMultiRaftWalManager(filepath.Join(rootDir, "wal"), log)
	snapshotMgr := snapshot.NewMultiRaftSnapshotManager(snapshot.Config{
		SnapshotVersion: 1,
		MaxSnapFiles:    3,
		SnapshotDir:     filepath.Join(rootDir, "snapshot"),
	}, durable.NewSnapshotFS(), log)
	trans := transport.NewMultiRaftTransport(clusterID,
		transport.NewMultiRaftPeerManager(), limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), protocol.NewGrpcMultiRaft(log), log)

	return BootstrapOrRecoverNode(config, &ComponentFactory{}, trans, walMgr, snapshotMgr, log)
}

func TestBootstrap(t *testing.T) {
	peersConfig := babuza.NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))

	nm, err := createNodeManager(t, peersConfig)
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
	group1PeersConfig.RemovePeer(3)
	group2PeersConfig := peersConfig.Clone()
	group2PeersConfig.RemovePeer(1)
	assert.NoError(t, node1.CreateRaftGroup(raftGroup1, group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(raftGroup1, group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(raftGroup2, group2PeersConfig, false))
	assert.NoError(t, node3.CreateRaftGroup(raftGroup2, group2PeersConfig, false))
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
}

func TestJoinVotingGroup(t *testing.T) {
	peersConfig := babuza.NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(1, "localhost:14201", false))
	assert.NoError(t, peersConfig.AddPeer(2, "localhost:14202", false))
	assert.NoError(t, peersConfig.AddPeer(3, "localhost:14203", false))

	nm, err := createNodeManager(t, peersConfig)
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
	group1PeersConfig.RemovePeer(3)
	assert.NoError(t, node1.CreateRaftGroup(raftGroup1, group1PeersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(raftGroup1, group1PeersConfig, false))
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

	peer3, _ := peersConfig.GetPeer(3)
	res := group1LeaderNode.AddVotingPeer(context.Background(), raftGroup1, babuza.ClientSession{}, peer3)
	ar := res.WaitForResult()
	res.Release()
	assert.Nil(t, ar.Error)
	assert.NoError(t, node3.CreateRaftGroup(raftGroup1, peersConfig, true))
	//wait for node3 to join group1
	time.Sleep(time.Second)
	groupIDs = node3.GetGroupIDs()
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])

	lastGroup1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	assert.Equal(t, group1LeaderID, lastGroup1LeaderID)
}
