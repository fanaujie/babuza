package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
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
	node1ID := uint64(1)
	clusterID := uint64(100)
	raftGroupID := ibabuza.RaftGroupID(500)
	node1RaftListenAddr := "localhost:14201"
	node1, err := createNode(clusterID, node1ID, node1RaftListenAddr, t.TempDir())
	assert.NoError(t, err)
	assert.Nil(t, node1.Start())
	defer node1.Stop()
	node2ID := uint64(2)
	node2RaftListenAddr := "localhost:14202"
	node2, err := createNode(clusterID, node2ID, node2RaftListenAddr, t.TempDir())
	assert.NoError(t, err)
	assert.Nil(t, node2.Start())
	defer node2.Stop()

	peersConfig := babuza.NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(node1ID, node1RaftListenAddr, false))
	assert.NoError(t, peersConfig.AddPeer(node2ID, node2RaftListenAddr, false))
	assert.NoError(t, node1.CreateRaftGroup(raftGroupID, peersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(raftGroupID, peersConfig, false))
	assert.NoError(t, node1.CreateRaftGroup(raftGroupID+1, peersConfig, false))
	assert.NoError(t, node2.CreateRaftGroup(raftGroupID+1, peersConfig, false))
	time.Sleep(time.Second * 3)
	node1Group1Status, err := node1.Status(raftGroupID)
	assert.NoError(t, err)
	node2Group1Status, err := node2.Status(raftGroupID)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, node1Group1Status.LeaderID)
	assert.Equal(t, node1Group1Status.LeaderID, node2Group1Status.LeaderID)

	node1Group2Status, err := node1.Status(raftGroupID + 1)
	assert.NoError(t, err)
	node2Group2Status, err := node2.Status(raftGroupID + 1)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, node1Group2Status.LeaderID)
	assert.Equal(t, node2Group2Status.LeaderID, node2Group2Status.LeaderID)
}
