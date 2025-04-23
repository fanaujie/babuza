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
	"path/filepath"
	"testing"
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

func TestBootstrap(t *testing.T) {
	rootDir := t.TempDir()
	nodeID := uint64(1)
	nodeRaftListenAddr := "localhost:14201"
	config := DefaultNodeConfig(100, nodeID, rootDir, nodeRaftListenAddr)
	log := &logger.Mock{}
	walMgr := babuzawal.NewMultiRaftWalManager(filepath.Join(rootDir, "wal"), log)
	snapshotMgr := snapshot.NewMultiRaftSnapshotManager(snapshot.Config{
		SnapshotVersion: 1,
		MaxSnapFiles:    3,
		SnapshotDir:     filepath.Join(rootDir, "snapshot"),
	}, durable.NewSnapshotFS(), log)
	trans := transport.NewMultiRaftTransport(100,
		transport.NewMultiRaftPeerManager(), limiter.NewNoResourceLimiter(), limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(), protocol.NewGrpcMultiRaft(log), log)

	n, err := BootstrapOrRecoverNode(config, &ComponentFactory{}, trans, walMgr, snapshotMgr, log)
	assert.NoError(t, err)
	assert.Nil(t, n.Start())
	defer n.Stop()
	peersConfig := babuza.NewPeersConfiguration()
	assert.NoError(t, peersConfig.AddPeer(nodeID, nodeRaftListenAddr, false))
	assert.NoError(t, n.CreateRaftGroup(ibabuza.RaftGroupID(1), peersConfig, false))
}
