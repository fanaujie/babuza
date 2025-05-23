package dragonboat

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"net"
	"os"
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
	cfg        Config
	nh         *dragonboat.NodeHost
	grpcServer *GrpcServer
	closer     *syncutil.Closer
	mu         sync.Mutex
}

// NewServer creates a new multi-raft KV server
func NewServer(cfg Config) (*Server, error) {
	if cfg.ShardCount <= 0 {
		return nil, fmt.Errorf("shard count must be greater than 0")
	}

	s := &Server{
		cfg:    cfg,
		closer: syncutil.NewCloser(),
	}

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// config for raft
	// note the ShardID value is not specified here
	rc := config.Config{
		ReplicaID:          uint64(s.cfg.LocalPeerID),
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    1000000000,
		CompactionOverhead: 128,
	}

	nhc := config.NodeHostConfig{
		DeploymentID:      s.cfg.ClusterID,
		WALDir:            filepath.Join(s.cfg.DataDir, "wal"),
		NodeHostDir:       s.cfg.DataDir,
		RTTMillisecond:    100,
		RaftAddress:       s.cfg.InitialRaftPeers[s.cfg.LocalPeerID],
		RaftEventListener: s,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}
	s.nh = nh
	for i := uint(0); i < s.cfg.ShardCount; i++ {

		// Create peer configuration for this group
		peersConfig := babuza.NewPeersConfiguration()

		for peerID, addr := range s.cfg.InitialRaftPeers {
			if err = peersConfig.AddPeer(peerID, addr, false); err != nil {
				return fmt.Errorf("failed to add peer %d: %w", peerID, err)
			}
		}
		rc.ShardID = uint64(i + 1)
		// Create Raft group
		if err = nh.StartReplica(s.cfg.InitialRaftPeers, false, func(shardID uint64, replicaID uint64) sm.IStateMachine {
			return NewMemoryStore()
		}, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}

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
	s.grpcServer = NewGrpcServer(s.cfg, s.nh)

	s.closer.Run(func() {
		if err = s.grpcServer.Serve(lis); err != nil {
			fmt.Errorf("gRPC server failed: %v", err)
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

	if s.nh != nil {
		s.nh.Close()
	}
	s.closer.Close()
	return nil
}

func (s *Server) LeaderUpdated(info raftio.LeaderInfo) {

}

// WaitForLeadership waits for leadership of all Raft groups
func (s *Server) WaitForLeadership(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		allHaveLeader := true
		info := s.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
		for _, shardInfo := range info.ShardInfoList {
			if shardInfo.LeaderID == 0 {
				allHaveLeader = false
				break
			}
		}

		if allHaveLeader {
			for _, shardInfo := range info.ShardInfoList {
				fmt.Printf("Leader %d: %d\n", shardInfo.ShardID, shardInfo.LeaderID)
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
