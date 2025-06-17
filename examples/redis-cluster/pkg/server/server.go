package server

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/command"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdclient"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/rediscommon"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

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
	store       *multiraft.Store
	redisServer *redcon.Server
	pdClient    *pdclient.PDClient
	router      *command.Router
	clMgr       *clusterMgr
	logger      ibabuza.Logger
	stopCh      chan struct{}
}

func (s *Server) OnAcquiredLeader(groupID ibabuza.RaftGroupID, term, leaderID uint64) {
	s.logger.Infof("Acquired leader for group %d, term %d, leaderID %d", groupID, term, leaderID)
	// immediately send heartbeat to PD
	info, err := s.store.RaftGroupPeersInfo(groupID)
	if err != nil {
		s.logger.Errorf("Failed to get Raft group peers info for group %d: %v", groupID, err)
		return
	}
	if err = s.replicaLeaderHeartbeat(info); err != nil {
		s.logger.Errorf("Failed to send leader heartbeat for group %d: %v", groupID, err)
		return
	}
}

func (s *Server) OnLostLeader(groupID ibabuza.RaftGroupID, term, leaderID uint64) {
	s.logger.Infof("Lost leader for group %d, term %d, leaderID %d", groupID, term, leaderID)
}

func (s *Server) OnLeaderChange(groupID ibabuza.RaftGroupID, term, leaderID uint64) {
	s.logger.Infof("Leader changed for group %d, term %d, new leaderID %d", groupID, term, leaderID)
}

func (s *Server) OnMemberChange(memberEvent int, groupID ibabuza.RaftGroupID, term, peerID uint64) {
	s.logger.Infof("Member change event %d for group %d, term %d, peerID %d", memberEvent, groupID, term, peerID)
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
		stopCh: make(chan struct{}),
	}
	if err = server.setupNode(); err != nil {
		return nil, err
	}
	server.clMgr = newClusterMgr(config.RedisListenAddr, server.store)
	server.router = command.NewRouter(config.InitialShards, server.clMgr)
	server.registerCommand()
	return server, nil
}

func (s *Server) setupNode() error {
	nodeConfig := multiraft.DefaultStoreConfig(
		s.config.ClusterID,
		s.config.StoreID,
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
	s.store, err = multiraft.BootstrapOrRecoverStore(nodeConfig, factory, trans, walMgr, snapshotMgr, s)
	if err != nil {
		return fmt.Errorf("failed to bootstrap node: %w", err)
	}

	if len(s.store.GetGroupIDs()) == 0 {
		if err = s.createInitialShards(); err != nil {
			s.store.Stop()
			return fmt.Errorf("failed to create initial shards: %w", err)
		}
	}

	return nil
}

func (s *Server) Run() error {
	if err := s.store.Start(); err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	}
	var err error
	s.pdClient, err = pdclient.NewPDClient(s.config.PdGRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to create pd client: %w", err)
	}
	s.setupRedisServer()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	fmt.Println("Shutting down...")
	close(s.stopCh)
	if s.redisServer != nil {
		s.redisServer.Close()
	}
	s.clMgr.Close()
	s.store.Stop()
	return nil
}

func (s *Server) setupRedisServer() {
	s.redisServer = redcon.NewServer(s.config.RedisListenAddr,
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
		s.logger.Infof("Redis server started on %s", s.config.RedisListenAddr)
		_ = s.redisServer.ListenAndServe()
	}()
	go func() {
		s.heartbeat()
	}()
}

func (s *Server) createInitialShards() error {
	for i := 0; i < s.config.InitialShards; i++ {
		groupID := ibabuza.RaftGroupID(i + 1)

		peersConfig, err := s.config.CreatePeersConfig(groupID)
		if err != nil {
			return fmt.Errorf("failed to create peers config for shard %d: %w", groupID, err)
		}

		if err = s.store.CreateRaftGroup(peersConfig, false); err != nil {
			return fmt.Errorf("failed to create Raft group %d: %w", groupID, err)
		}
		s.logger.Infof("Created shard %d", groupID)
	}
	return nil
}

func (s *Server) heartbeat() {
	storeHeartbeatTicker := time.NewTicker(time.Duration(s.config.IntervalHeartbeatStore) * time.Second)
	defer storeHeartbeatTicker.Stop()
	leaderHeartbeatTicker := time.NewTicker(time.Duration(s.config.IntervalHeartbeatRaftGroupLeader) * time.Second)
	defer leaderHeartbeatTicker.Stop()
	shardIDs := make([]uint64, 0, s.config.InitialShards)
	for {
		select {
		case <-s.stopCh:
			return
		case <-storeHeartbeatTicker.C:
			shardIDs = shardIDs[:0] // Reset leader IDs for this heartbeat
			leaderCount := uint64(0)
			for _, groupID := range s.store.GetGroupIDs() {
				info, err := s.store.RaftGroupPeersInfo(groupID)
				if err != nil {
					s.logger.Errorf("failed to get Raft group peers info for group %d: %v", groupID, err)
					continue
				}
				if info.IsLeader() {
					leaderCount++
					shardIDs = append(shardIDs, uint64(groupID))
				}
			}
			if res, err := s.pdClient.StoreHeartbeat(context.TODO(),
				&pb.StoreHeartbeatReq{
					StoreID:         s.config.StoreID,
					ClusterID:       s.config.ClusterID,
					LeaderCount:     leaderCount,
					RedisListenAddr: s.config.RedisListenAddr,
					LeaderGroupIDs:  shardIDs,
				}); err != nil {
				s.logger.Errorf("failed to send store heartbeat: %v", err)
			} else {
				s.clMgr.UpdateRoutingTable(res.RedisRoutingTable)
			}
		case <-leaderHeartbeatTicker.C:
			for _, groupID := range s.store.GetGroupIDs() {
				info, err := s.store.RaftGroupPeersInfo(groupID)
				if err != nil {
					continue
				}
				if info.LeaderID == info.LocalPeerID {
					if err = s.replicaLeaderHeartbeat(info); err != nil {
						s.logger.Errorf("failed to heartbeat leader: %v", err)
						continue
					}
				}
			}
		}
	}
}

func (s *Server) replicaLeaderHeartbeat(info multiraft.RaftGroupPeersInfo) error {
	resp, err := s.pdClient.RaftGroupLeaderHeartbeat(context.TODO(),
		&pb.RaftGroupLeaderHeartbeatReq{
			StoreID:  s.config.StoreID,
			GroupID:  uint64(info.GroupID),
			LeaderID: info.LeaderID,
			Peers:    info.Peers,
		})
	if err != nil {
		return err
	}
	if resp.TransferLeader != nil {
		s.logger.Infof("Transferring leader for group %d to new leader %d", info.GroupID, resp.TransferLeader.NewLeaderID)
		s.store.TransferLeader(context.TODO(), info.GroupID, resp.TransferLeader.NewLeaderID)
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
