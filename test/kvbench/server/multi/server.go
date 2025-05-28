package multi

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
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
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	"github.com/fanaujie/babuza/raft/multiraft"
	"github.com/fanaujie/babuza/test/kvbench/statemachine"
	"go.uber.org/zap"
	"net"
	"path/filepath"
	"sync"
	"time"
)

// Config holds the configuration for the multi-raft KV server
type Config struct {
	// DataDir is the directory where data will be stored
	DataDir string

	// ClusterID is the ID of the Raft cluster
	ClusterID uint64

	// LocalPeerID is the ID of the local peer
	LocalPeerID uint64

	// GrpcAddress is the address for the gRPC service
	GrpcAddress string

	// RaftAddress is the address for Raft communication
	RaftAddress string

	// InitialPeers is a list of peers to connect each other
	InitialRaftPeers map[uint64]string

	// InitialGRPCPeers is a list of gRPC peers for client connections
	InitialGRPCPeers map[uint64]string

	// ShardCount is the number of Raft groups (shards) to create
	ShardCount uint
}

// Server represents a multi-raft KV server
type Server struct {
	cfg           Config
	multiRaftNode *multiraft.Node
	stateMachines map[ibabuza.RaftGroupID]*statemachine.MemoryStore
	logger        ibabuza.Logger
	closer        *syncutil.Closer
	grpcServer    *GrpcServer
	mu            sync.Mutex
}

// KVComponentFactory implements multiraft.ComponentsFactory
type KVComponentFactory struct {
	stateMachines map[ibabuza.RaftGroupID]*statemachine.MemoryStore
	logger        ibabuza.Logger
	rootDir       string
}

func (f *KVComponentFactory) CreateCluster() ibabuza.Cluster {
	return cluster.NewCluster(f.logger)
}

func (f *KVComponentFactory) CreateSessionManager() ibabuza.SessionManager {
	return session.NewNoOpManager(f.logger)
}

// CreateStateMachine creates a state machine for the specified group
func (f *KVComponentFactory) CreateStateMachine(stateMachineRootDir string, groupID ibabuza.RaftGroupID) (ibabuza.BaseStateMachine, error) {
	// Check if we already have a state machine for this group
	if sm, ok := f.stateMachines[groupID]; ok {
		return sm, nil
	}

	// Create a new state machine
	sm := statemachine.NewMemoryStore()
	f.stateMachines[groupID] = sm
	return sm, nil
}

// GetLogger returns the logger
func (f *KVComponentFactory) GetLogger() ibabuza.Logger {
	return f.logger
}

// NewServer creates a new multi-raft KV server
func NewServer(cfg Config) (*Server, error) {
	if cfg.ShardCount <= 0 {
		return nil, fmt.Errorf("shard count must be greater than 0")
	}

	s := &Server{
		cfg:           cfg,
		closer:        syncutil.NewCloser(),
		stateMachines: make(map[ibabuza.RaftGroupID]*statemachine.MemoryStore),
	}

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create logger
	zapLogger, _ := zap.NewProduction(zap.AddCallerSkip(1))
	s.logger = logger.NewRaftLogger(zapLogger.Sugar())

	// Create node configuration
	nodeConfig := multiraft.DefaultNodeConfig(s.cfg.ClusterID, s.cfg.LocalPeerID, s.cfg.DataDir, s.cfg.RaftAddress)
	nodeConfig.EnableWalNoSync = true
	nodeConfig.SnapshotCount = 100000000
	nodeConfig.DisableProposalForwarding = false
	nodeConfig.LearnerReadyPercent = 0.95
	nodeConfig.CoalescedHeartbeatQueueSize = 2048
	nodeConfig.SchedulerShardNum = 16
	nodeConfig.SchedulerShardWorkerNum = 8
	nodeConfig.SchedulerQueueSize = 256

	// Create components factory
	factory := &KVComponentFactory{
		stateMachines: s.stateMachines,
		logger:        s.logger,
		rootDir:       s.cfg.DataDir,
	}

	// Create WAL manager
	walMgr := lsmtwal.NewMultiRaftBadgerWalManager(lsmtwal.MultiRaftConfig{
		InMemory:           false,
		WalDir:             filepath.Join(s.cfg.DataDir, "wal"),
		KeyPrefixCacheSize: 1024,
	}, s.logger)

	// Create snapshot manager
	snapshotMgr := snapshot.NewMultiRaftSnapshotManager(snapshot.Config{
		SnapshotVersion: 1,
		MaxSnapFiles:    3,
		SnapshotDir:     filepath.Join(s.cfg.DataDir, "snapshot"),
	}, durable.NewSnapshotFS(), s.logger)

	// Create transport
	peerManager := transport.NewPeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter]()
	trans := transport.NewMultiRaftTransport(
		s.cfg.ClusterID,
		peerManager,
		limiter.NewNoResourceLimiter(),
		limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(),
		protocol.NewGrpcMultiRaft(s.logger),
		s.logger,
		transport.SetTransportOptionsWithPeerQueueSize(2048*10),
		transport.SetTransportOptionsWithHeartbeatBufferSize(1024),
	)

	// Create MultiRaft node
	node, err := multiraft.BootstrapOrRecoverNode(nodeConfig, factory, trans, walMgr, snapshotMgr)
	if err != nil {
		return fmt.Errorf("failed to create MultiRaft node: %w", err)
	}

	s.multiRaftNode = node

	// Start MultiRaft node
	if err = s.multiRaftNode.Start(); err != nil {
		return fmt.Errorf("failed to start MultiRaft node: %w", err)
	}

	// Create Raft groups for each shard
	for i := uint(0); i < s.cfg.ShardCount; i++ {
		groupID := ibabuza.RaftGroupID(i + 1) // Group IDs start from 1

		// Create peer configuration for this group
		peersConfig := multiraft.NewPeersConfiguration()
		peersConfig.SetGroupID(groupID)
		for peerID, addr := range s.cfg.InitialRaftPeers {
			if err = peersConfig.AddPeer(peerID, addr, false); err != nil {
				return fmt.Errorf("failed to add peer %d: %w", peerID, err)
			}
		}

		// Create Raft group
		if err = s.multiRaftNode.CreateRaftGroup(peersConfig, false); err != nil {
			return fmt.Errorf("failed to create Raft group %d: %w", groupID, err)
		}

		s.logger.Infof("Created Raft group %d", groupID)
	}

	// Start gRPC server
	return s.startGrpcServer()
}

// startGrpcServer starts the gRPC server
func (s *Server) startGrpcServer() error {
	lis, err := net.Listen("tcp", s.cfg.GrpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.GrpcAddress, err)
	}

	s.grpcServer = NewGrpcServer(s.cfg, s.multiRaftNode, s.stateMachines, s.logger)

	s.closer.Run(func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Errorf("gRPC server failed: %v", err)
		}
	})

	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}

	if s.multiRaftNode != nil {
		s.multiRaftNode.Stop()
	}

	s.closer.Close()
	return nil
}

// WaitForLeadership waits for leadership of all Raft groups
func (s *Server) WaitForLeadership(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		allHaveLeader := true

		for i := uint(0); i < s.cfg.ShardCount; i++ {
			groupID := ibabuza.RaftGroupID(i + 1)
			status, err := s.multiRaftNode.Status(groupID)
			if err != nil {
				return err
			}

			if status.LeaderID == 0 {
				allHaveLeader = false
				break
			}
		}

		if allHaveLeader {
			for i := uint(0); i < s.cfg.ShardCount; i++ {
				groupID := ibabuza.RaftGroupID(i + 1)
				status, _ := s.multiRaftNode.Status(groupID)
				fmt.Printf("Leader %d: %d\n", groupID, status.LeaderID)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leadership")
		case <-ticker.C:
			// Continue checking
		}
	}
}
