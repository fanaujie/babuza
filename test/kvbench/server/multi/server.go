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
	"github.com/fanaujie/babuza/raft/experimental"
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

	// StoreID is the ID of the store
	StoreID uint64

	// GrpcAddress is the address for the gRPC service
	GrpcAddress string

	// RaftAddress is the address for Raft communication
	RaftAddress string

	// InitialRaftStores is a list of store to connect each other
	InitialRaftStores map[uint64]string

	// InitialGRPCStores is a list of gRPC peers for client connections
	InitialGRPCStores map[uint64]string

	// ShardCount is the number of Raft groups (shards) to create
	ShardCount uint64
}

// Server represents a multi-raft KV server
type Server struct {
	cfg           Config
	store         *experimental.Store
	stateMachines map[ibabuza.RaftGroupID]*statemachine.MemoryStore
	logger        ibabuza.Logger
	closer        *syncutil.Closer
	grpcServer    *GrpcServer
	mu            sync.Mutex
}

// KVComponentFactory implements experimental.ComponentsFactory
type KVComponentFactory struct {
	stateMachines map[ibabuza.RaftGroupID]*statemachine.MemoryStore
	logger        ibabuza.Logger
	rootDir       string
}

func (f *KVComponentFactory) CreateCluster() ibabuza.Cluster {
	return cluster.NewCluster()
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

	// Create store configuration
	storeConfig := experimental.DefaultStoreConfig(s.cfg.ClusterID, s.cfg.StoreID, s.cfg.DataDir, s.cfg.RaftAddress)
	storeConfig.SnapshotCount = 100000000
	storeConfig.DisableProposalForwarding = false
	storeConfig.LearnerReadyPercent = 0.95
	storeConfig.CoalescedHeartbeatQueueSize = 2048
	storeConfig.SchedulerShardNum = 8
	storeConfig.SchedulerShardWorkerNum = 4
	storeConfig.SchedulerQueueSize = 256
	storeConfig.JobQueueShardNum = 16

	// Create components factory
	factory := &KVComponentFactory{
		stateMachines: s.stateMachines,
		logger:        s.logger,
		rootDir:       s.cfg.DataDir,
	}

	// Create WAL manager
	walMgr := lsmtwal.NewMultiRaftWalManager(lsmtwal.MultiRaftConfig{
		InMemory:           false,
		WalDir:             filepath.Join(s.cfg.DataDir, "wal"),
		KeyPrefixCacheSize: 1024,
		ManagerType:        lsmtwal.WalManagerTypeBadger,
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
	node, err := experimental.BootstrapOrRecoverStore(storeConfig, factory, trans, walMgr, snapshotMgr, nil)
	if err != nil {
		return fmt.Errorf("failed to create MultiRaft node: %w", err)
	}

	s.store = node

	// Start MultiRaft node
	if err = s.store.Start(); err != nil {
		return fmt.Errorf("failed to start MultiRaft node: %w", err)
	}

	// Create Raft groups for each shard
	for i := uint64(0); i < s.cfg.ShardCount; i++ {
		groupID := ibabuza.RaftGroupID(i + 1) // Group IDs start from 1

		// Create peer configuration for this group
		peersConfig := experimental.NewPeersConfiguration()
		peersConfig.SetGroupID(groupID)
		for storeID, addr := range s.cfg.InitialRaftStores {
			peerID := storeID
			if err = peersConfig.AddPeer(peerID, storeID, addr, false); err != nil {
				return fmt.Errorf("failed to add peer %d  to store %d: %w", peerID, storeID, err)
			}
		}

		// Create Raft group
		if err = s.store.CreateRaftGroup(peersConfig, false); err != nil {
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

	s.grpcServer = NewGrpcServer(s.cfg, s.store, s.stateMachines, s.logger)

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

	if s.store != nil {
		s.store.Stop()
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

		for i := uint64(0); i < s.cfg.ShardCount; i++ {
			groupID := ibabuza.RaftGroupID(i + 1)
			status, err := s.store.RaftGroupStatus(groupID)
			if err != nil {
				return err
			}

			if status.LeaderID == 0 {
				allHaveLeader = false
				break
			}
		}

		if allHaveLeader {
			for i := uint64(0); i < s.cfg.ShardCount; i++ {
				groupID := ibabuza.RaftGroupID(i + 1)
				status, _ := s.store.RaftGroupStatus(groupID)
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
