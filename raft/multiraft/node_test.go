package multiraft

import (
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

func (s *simpleStateMachine) Apply(entry ibabuza.Entry) {
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
	assert.NoError(t, nm.Start(1))
	assert.NoError(t, nm.Start(2))
	assert.NoError(t, nm.Start(3))
	defer func() {
		_ = nm.Stop(1)
		_ = nm.Stop(2)
		_ = nm.Stop(3)
	}()
	raftGroup1 := ibabuza.RaftGroupID(10)
	raftGroup2 := ibabuza.RaftGroupID(11)
	group1PeersConfig := peersConfig.Clone()
	group1PeersConfig.RemovePeer(3)
	group2PeersConfig := peersConfig.Clone()
	group2PeersConfig.RemovePeer(1)
	assert.NoError(t, nm.CreateRaftGroup(1, raftGroup1, group1PeersConfig, false))
	assert.NoError(t, nm.CreateRaftGroup(2, raftGroup1, group1PeersConfig, false))
	assert.NoError(t, nm.CreateRaftGroup(2, raftGroup2, group2PeersConfig, false))
	assert.NoError(t, nm.CreateRaftGroup(3, raftGroup2, group2PeersConfig, false))
	groupIDs, err := nm.GetGroupIDsByNodeID(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	groupIDs, err = nm.GetGroupIDsByNodeID(2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(groupIDs))
	assert.Equal(t, raftGroup1, groupIDs[0])
	assert.Equal(t, raftGroup2, groupIDs[1])
	groupIDs, err = nm.GetGroupIDsByNodeID(3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(groupIDs))
	assert.Equal(t, raftGroup2, groupIDs[0])
	// wait for leader election
	time.Sleep(time.Second * 3)
	group1LeaderID, err := nm.CheckSameLeader(raftGroup1)
	assert.NoError(t, err)
	t.Logf("group1 leader: %d", group1LeaderID)
	group2LeaderID, err := nm.CheckSameLeader(raftGroup2)
	assert.NoError(t, err)
	t.Logf("group2 leader: %d", group2LeaderID)
}
