// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package single

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/kvbench/statemachine"
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

	// InitialPeers is a list of peers to connect each other
	InitialPeers map[uint64]string
}

// Server represents a KV server
type Server struct {
	cfg          Config
	raft         *babuza.Raft
	stateMachine *statemachine.MemoryStore
	logger       ibabuza.Logger
	closer       *syncutil.Closer
	grpcServer   *GrpcServer
	mu           sync.Mutex
}

func (s *Server) OnLeaderChange(term, leaderID uint64) {
	s.logger.Infof("leader change term: %d, leaderID: %d", term, leaderID)
}

func (s *Server) OnMemberChange(memberEvent int, term, peerID uint64) {
	s.logger.Infof("member change term: %d, peerID: %d", term, peerID)
}

func (s *Server) OnRaftShutdown() {
	s.logger.Infof("raft shutdown")
}

func (s *Server) OnAcquiredLeader() {

}

func (s *Server) OnLostLeader() {

}

// NewServer creates a new KV server
func NewServer(cfg Config) (*Server, error) {
	s := &Server{
		cfg:    cfg,
		closer: syncutil.NewCloser(),
	}

	// Create state machine
	s.stateMachine = statemachine.NewMemoryStore()

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

	// Set up State machine
	s.stateMachine = statemachine.NewMemoryStore()

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
	r, err := babuza.NewRaft(babuzaCfg, bootstrap, s)
	if err != nil {
		return err
	}

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
