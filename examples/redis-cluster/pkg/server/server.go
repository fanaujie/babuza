package server

import (
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/cluster"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/command"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/rediscommon"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	"github.com/fanaujie/babuza/raft/multiraft"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

type Server struct {
	config      Config
	node        *multiraft.Node
	redisServer *redcon.Server
	router      *command.Router
	logger      ibabuza.Logger
}

type ShardInfo struct {
	GroupID ibabuza.RaftGroupID
	Leader  uint64
}

func NewServer(config Config) (*Server, error) {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	babuzaLogger := logger.NewRaftLogger(zapLogger.Sugar())

	server := &Server{
		config: config,
		logger: babuzaLogger,
	}
	if err = server.setupNode(); err != nil {
		return nil, err
	}
	server.router = command.NewRouter(config.InitialShards, cluster.NewMultiRaft(server.node))
	server.registerCommand()
	return server, nil
}

func (s *Server) setupNode() error {
	nodeConfig := multiraft.DefaultNodeConfig(
		s.config.ClusterID,
		s.config.NodeID,
		s.config.DataDir,
		s.config.RaftAddr,
	)
	nodeConfig.ElectionTicks = 20
	nodeConfig.HeartbeatTicks = 2

	walMgr := lsmtwal.NewMultiRaftBadgerWalManager(lsmtwal.MultiRaftConfig{
		InMemory:           false,
		WalDir:             filepath.Join(s.config.DataDir, "wal"),
		KeyPrefixCacheSize: 1024,
	}, s.logger)

	snapshotMgr := snapshot.NewMultiRaftSnapshotManager(snapshot.Config{
		SnapshotVersion: 1,
		MaxSnapFiles:    3,
		SnapshotDir:     filepath.Join(s.config.DataDir, "snapshot"),
	}, durable.NewSnapshotFS(), s.logger)

	trans := transport.NewMultiRaftTransport(
		s.config.ClusterID,
		transport.NewPeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter](),
		limiter.NewNoResourceLimiter(),
		limiter.NewNoOpRateLimiter(),
		breaker.NewNoOpBreaker(),
		protocol.NewGrpcMultiRaft(s.logger),
		s.logger,
	)

	factory := &redisComponentFactory{
		logger: s.logger,
	}

	var err error
	s.node, err = multiraft.BootstrapOrRecoverNode(nodeConfig, factory, trans, walMgr, snapshotMgr)
	if err != nil {
		return fmt.Errorf("failed to bootstrap node: %w", err)
	}

	if len(s.node.GetGroupIDs()) == 0 {
		if !s.config.JoinExisting {
			if err = s.createInitialShards(); err != nil {
				s.node.Stop()
				return fmt.Errorf("failed to create initial shards: %w", err)
			}
		}
	}

	return nil
}

func (s *Server) Run() error {
	if err := s.node.Start(); err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	}

	s.setupRedisServer()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	fmt.Println("Shutting down...")

	if s.redisServer != nil {
		s.redisServer.Close()
	}
	s.node.Stop()
	return nil
}

func (s *Server) setupRedisServer() {
	s.redisServer = redcon.NewServer(s.config.ListenAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.router.RunCommand(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			s.logger.Infof("New connection: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			s.logger.Infof("Connection closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)

	go func() {
		s.logger.Infof("Redis server started on %s", s.config.ListenAddr)
		err := s.redisServer.ListenAndServe()
		if err != nil {
			s.logger.Infof("Redis server error: %v", err)
		}
	}()
}

func (s *Server) createInitialShards() error {
	for i := 0; i < s.config.InitialShards; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)

		peersConfig, err := s.config.CreatePeersConfig(groupID)
		if err != nil {
			return fmt.Errorf("failed to create peers config for shard %d: %w", groupID, err)
		}

		if err = s.node.CreateRaftGroup(peersConfig, false); err != nil {
			return fmt.Errorf("failed to create Raft group %d: %w", groupID, err)
		}
		s.logger.Infof("Created shard %d", groupID)
	}
	return nil
}

func (s *Server) registerCommand() {
	s.router.RegisterCommand(rediscommon.RedisPing, command.Handler{
		OperationCmd: true,
		Executor:     command.Ping,
	})
	s.router.RegisterCommand(rediscommon.RedisEcho, command.Handler{
		OperationCmd: true,
		Executor:     command.Echo,
	})
	s.router.RegisterCommand(rediscommon.RedisSet, command.Handler{
		OperationCmd: false,
		Executor:     command.Set,
	})
	s.router.RegisterCommand(rediscommon.RedisGet, command.Handler{
		OperationCmd: false,
		Executor:     command.Get,
	})
}
