package server

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/kvbench/server/kvstore"
	"net"
	"sync"
)

// Config holds the configuration for the KV server
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

	// JoinCluster indicates whether to join an existing cluster
	JoinCluster bool

	// InitialPeers is a list of peers to connect to when joining a cluster
	InitialPeers map[uint64]string
}

// Server represents a KV server
type Server struct {
	cfg          Config
	raft         *babuza.Raft
	stateMachine *kvstore.MemoryStore
	logger       ibabuza.Logger
	closer       *syncutil.Closer
	grpcServer   *GrpcServer
	mu           sync.Mutex
}

// NewServer creates a new KV server
func NewServer(cfg Config) (*Server, error) {
	s := &Server{
		cfg:    cfg,
		closer: syncutil.NewCloser(),
	}

	// Create state machine
	s.stateMachine = kvstore.NewMemoryStore()

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create basic configuration
	babuzaCfg := babuza.DefaultBabuzaConfig(s.cfg.ClusterID,
		s.cfg.LocalPeerID, s.cfg.RaftAddress)
	babuzaCfg.SnapshotCount = 1000000
	babuzaCfg.DisableProposalForwarding = false
	// Set additional configuration options
	babuzaCfg.Join = s.cfg.JoinCluster

	// Set up State machine
	s.stateMachine = kvstore.NewMemoryStore()

	// Create Babuza components
	babuzaComponents := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		ClusterId:      s.cfg.ClusterID,
		StorageRootDir: s.cfg.DataDir,
		SessionType:    builder.NoOpSession,
		TransportType:  builder.TcpTransport,
		WalType:        builder.BabuzaWal,
		SnapshotType:   builder.DurableSnapshot,
		MetricType:     builder.MetricsPrometheus,
	}).AddTransportOptions(transport.SetTransportOptionsWithPeerQueueSize(4096)).Build()

	// Set cluster configuration
	peersConfiguration := babuza.NewPeersConfiguration()
	for id, endpoint := range s.cfg.InitialPeers {
		// In a real implementation, we would need to parse the peer ID from the endpoint
		// For simplicity, we'll just use a dummy ID
		if err := peersConfiguration.AddPeer(id, endpoint, false); err != nil {
			return err
		}
	}

	// Initialize Raft cluster using BootstrapBuilder
	bootstrap, err := babuza.NewBootstrapRaftCluster(babuzaCfg, *peersConfiguration, s.stateMachine, babuzaComponents.Cluster,
		babuzaComponents.RaftNode, babuzaComponents.SessionManager, babuzaComponents.SnapshotManager, babuzaComponents.WalManager,
		babuzaComponents.Transport, babuzaComponents.Logger, babuzaComponents.MetricsController)
	if err != nil {
		return err
	}

	// Create Raft instance
	r, err := babuza.NewRaft(babuzaCfg, bootstrap)
	if err != nil {
		return err
	}

	// Listen for leadership change events
	s.closer.Run(func() {
		for {
			select {
			case <-s.closer.CloseCh():
				return
			case isLeader := <-r.LeaderCh():
				if isLeader {
					s.logger.Infof("I am leader")
				} else {
					s.logger.Infof("I have lost my leadership")
				}
			}
		}
	})

	s.raft = r
	s.logger = babuzaComponents.Logger

	// Start application service
	if err = <-s.raft.ApplicationServiceStart(context.Background(), []string{s.cfg.GrpcAddress}); err != nil {
		return err
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

	s.grpcServer = NewGrpcServer(s.raft, s.stateMachine)

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

	if s.raft != nil {
		shutdownResult := s.raft.Shutdown()
		if err := shutdownResult.Wait(); err != nil {
			return fmt.Errorf("failed to shutdown Raft: %w", err)
		}
	}

	s.closer.Close()
	return nil
}
