package server

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/api"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/fanaujie/babuza/pkg/wal/etcdwal"
	"github.com/fanaujie/babuza/pkg/wal/lsmtwal"
	babuza "github.com/fanaujie/babuza/raft"
	"go.uber.org/zap/zapcore"
	"net/http"
	"path/filepath"
)

// Constants for component types, using descriptive string values
const (
	// Session types
	NoOpSession   = "noop"
	ExpireSession = "expire"
	LRUSession    = "lru"

	// Transport protocols
	TcpTransport       = "tcp"
	TcpMemoryTransport = "tcp-memory"
	HttpTransport      = "http"
	GRPCTranspost      = "grpc"

	// WAL implementations
	BabuzaWal     = "babuza-wal"
	ETCDWal       = "etcd-wal"
	LsmtWalDisk   = "lsmt-wal"
	LsmtWalMemory = "lsmt-wal-memory"

	// Snapshot implementations
	DurableSnapshot  = "durable"
	VolatileSnapshot = "volatile"
	MinIOSnapshot    = "minio"

	// State machine types
	StateMachineMemory                           = "memory"
	StateMachineMemoryWithConcurrentSnapshotType = "memory-concurrent"
	StateMachineDisk                             = "disk"
)

type Config struct {
	KvServiceHttpAddress string
	HttpCertFile         string
	HttpKeyFile          string

	RaftClusterId            uint64
	RaftLocalPeerId          uint64
	RaftVoterOrLearner       bool
	RaftLocalPeerAddress     string //must be ip address with port
	RaftClusterVotersAddress map[uint64]string
	RaftTLSCertFile          string
	RaftTLSKeyFile           string
	RaftTLSRootCA            string
	RaftStorageDir           string
	BabuzaSession            string
	BabuzaTransportProtocol  string
	BabuzaWal                string
	BabuzaSnapshot           string
	StateMachine             string
	JoinRaftCluster          bool
	RaftEncrypt              bool
	RaftDisableForwarding    bool
	RaftWalNoSync            bool

	// MinIO Configuration
	MinIOEndpoint        string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinIOBucket          string
	MinIOPrefix          string
}

type Server struct {
	cfg          Config
	raft         *babuza.Raft
	httpSrv      *http.Server
	stateMachine ibabuza.BaseStateMachine
	logger       ibabuza.Logger
	closer       *syncutil.Closer
}

func NewServer(cfg Config) *Server {
	return &Server{
		cfg:    cfg,
		closer: syncutil.NewCloser(),
	}
}

func (s *Server) Start() error {
	// Create basic configuration
	babuzaCfg := babuza.DefaultBabuzaConfig(s.cfg.RaftClusterId,
		s.cfg.RaftLocalPeerId, s.cfg.RaftLocalPeerAddress)

	// Set additional configuration options
	babuzaCfg.RaftConfig.DisableProposalForwarding = s.cfg.RaftDisableForwarding
	babuzaCfg.Join = s.cfg.JoinRaftCluster
	babuzaCfg.EnableWalNoSync = s.cfg.RaftWalNoSync

	// Set TLS configuration
	if s.cfg.RaftEncrypt {
		babuzaCfg.EnableTLS = true
		babuzaCfg.MutualTLS = true
		babuzaCfg.TLSCert = s.cfg.RaftTLSCertFile
		babuzaCfg.TLSKey = s.cfg.RaftTLSKeyFile
		babuzaCfg.TLSRootCA = s.cfg.RaftTLSRootCA
	}

	// Set cluster configuration
	peersConfiguration := babuza.NewVotingPeersConfiguration()
	for id, endpoint := range s.cfg.RaftClusterVotersAddress {
		if err := peersConfiguration.AddPeer(id, endpoint); err != nil {
			return err
		}
	}

	// Create logger
	var zapLogger = logger.NewZapLogger(
		zapcore.DebugLevel, []string{"stdout"}, "")
	raftLogger := logger.NewRaftLogger(zapLogger.Sugar())

	// Set up Session manager
	var sessionMgr ibabuza.SessionManager
	switch s.cfg.BabuzaSession {
	case NoOpSession:
		sessionMgr = session.NewNoOpManager(raftLogger)
	case ExpireSession:
		sessionMgr = session.NewExpiredManager(raftLogger)
	case LRUSession:
		sessionMgr = session.NewLruManager(raftLogger)
	default:
		return fmt.Errorf("unsupported session type: %s", s.cfg.BabuzaSession)
	}

	// Set up State machine
	switch s.cfg.StateMachine {
	case StateMachineMemory:
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewMemoryStore()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithSession()
		}
	case StateMachineMemoryWithConcurrentSnapshotType:
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshot()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
		}
	case StateMachineDisk:
		dataDir := filepath.Join(s.cfg.RaftStorageDir, "StateMachine")
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewDisk(dataDir)
		} else {
			s.stateMachine = kvstore.NewDiskStoreWithSession(dataDir)
		}
	default:
		return fmt.Errorf("unsupported state machine type: %s", s.cfg.StateMachine)
	}

	// Set up Transport layer
	var trans ibabuza.Transport
	switch s.cfg.BabuzaTransportProtocol {
	case TcpTransport:
		trans = transport.New(s.cfg.RaftClusterId, transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
			limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(), protocol.NewTcp(networkio.NewTcpPhysicalIO(),
				raftLogger), raftLogger)
	case TcpMemoryTransport:
		trans = transport.New(s.cfg.RaftClusterId, transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
			limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(), protocol.NewTcp(networkio.NewTcpMemoryIO(),
				raftLogger), raftLogger)
	case HttpTransport:
		trans = transport.New(s.cfg.RaftClusterId, transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
			limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(), protocol.NewHttp(raftLogger), raftLogger)
	case GRPCTranspost:
		trans = transport.New(s.cfg.RaftClusterId, transport.NewPeerManager(), limiter.NewNoResourceLimiter(),
			limiter.NewNoOpRateLimiter(), breaker.NewNoOpBreaker(), protocol.NewGrpc(raftLogger), raftLogger)
	default:
		return fmt.Errorf("unsupported transport protocol: %s", s.cfg.BabuzaTransportProtocol)
	}

	// Set up WAL manager
	var walManager ibabuza.WalManager
	walDir := filepath.Join(s.cfg.RaftStorageDir, "wal")
	switch s.cfg.BabuzaWal {
	case BabuzaWal:
		walManager = babuzawal.NewWalManager(walDir, raftLogger)
	case ETCDWal:
		walManager = etcdwal.NewWalManager(walDir, zapLogger)
	case LsmtWalDisk:
		walManager = lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			WalDir: walDir,
		}, raftLogger)
	case LsmtWalMemory:
		walManager = lsmtwal.NewBadgerWalManager(lsmtwal.Config{
			InMemory: true,
		}, raftLogger)
	default:
		return fmt.Errorf("unsupported WAL type: %s", s.cfg.BabuzaWal)
	}

	// Set up Snapshot manager
	var snapshotManager ibabuza.SnapshotManager
	snapDir := filepath.Join(s.cfg.RaftStorageDir, "snap")
	switch s.cfg.BabuzaSnapshot {
	case DurableSnapshot:
		snapshotManager = snapshot.NewDurableSnapshotManager(snapDir, raftLogger)
	case VolatileSnapshot:
		snapshotManager = snapshot.NewVolatileSnapshotManager(snapDir, raftLogger)
	case MinIOSnapshot:
		// Use MinIO configuration from settings
		snapshotManager = snapshot.NewMinIOSnapshotManager("/snap", cloudstorage.Config{
			Endpoint:        s.cfg.MinIOEndpoint,
			AccessKeyID:     s.cfg.MinIOAccessKeyID,
			SecretAccessKey: s.cfg.MinIOSecretAccessKey,
			UseSSL:          s.cfg.MinIOUseSSL,
			Bucket:          s.cfg.MinIOBucket,
			Prefix:          s.cfg.MinIOPrefix,
		}, raftLogger)
	default:
		return fmt.Errorf("unsupported snapshot type: %s", s.cfg.BabuzaSnapshot)
	}

	// Set up Raft node
	var raftNode ibabuza.RaftNode = raftnode.NewEtcdRaftNode()

	// Set up cluster
	var cls ibabuza.Cluster = cluster.NewCluster(raftLogger)

	// Initialize Raft cluster using BootstrapBuilder
	bootstrap, err := babuza.NewBootstrapRaftCluster(babuzaCfg, *peersConfiguration, s.stateMachine, cls,
		raftNode, sessionMgr, snapshotManager, walManager, trans, raftLogger)
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
			case <-r.ClusterMemberEventCh():
				s.logger.Infof("Cluster membership changed")
			}
		}
	})

	s.raft = r
	s.logger = raftLogger

	// Start application service
	if err = <-s.raft.ApplicationServiceStart(context.Background(), []string{s.cfg.KvServiceHttpAddress}); err != nil {
		return err
	}

	return s.startService()
}

func (s *Server) startService() error {
	m := http.NewServeMux()
	m.Handle(api.SessionsHttpPath, api.NewSessionResourceHandler(s.raft))
	m.Handle(api.ClusterPeersHttpPath, api.NewClusterPeerResourceHandler(s.raft))
	m.Handle(api.PromoteLearnerHttpPath, api.NewPromoteLearnerHandler(s.raft))
	m.Handle(api.TransferLeaderHttpPath, api.NewTransferLeaderHandler(s.raft))
	m.Handle(api.KvHttpPath, api.NewKvStoreResourceHandler(true, s.raft, s.stateMachine.(api.ReadKvStore)))
	s.httpSrv = &http.Server{
		Addr:    s.cfg.KvServiceHttpAddress,
		Handler: m,
	}
	if s.cfg.HttpCertFile != "" && s.cfg.HttpKeyFile != "" {
		return s.httpSrv.ListenAndServeTLS(s.cfg.HttpCertFile, s.cfg.HttpKeyFile)
	} else {
		return s.httpSrv.ListenAndServe()
	}
}
