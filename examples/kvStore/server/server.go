package server

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvStore/server/api"
	"github.com/fanaujie/babuza/examples/kvStore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/raftnode"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/snapshot"
	"github.com/fanaujie/babuza/pkg/transport"
	"github.com/fanaujie/babuza/pkg/transport/protocol"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal"
	"github.com/fanaujie/babuza/pkg/wal/etcdwal"
	babuza "github.com/fanaujie/babuza/raft"
	"go.uber.org/zap/zapcore"
	"net/http"
	"path/filepath"
)

const (
	NoOpSession   = "no"
	ExpireSession = "expire"
	LRUSession    = "lru"

	TcpTransport  = "tcp"
	HttpTransport = "http"

	BabuzaWal   = "babuzawal"
	ETCDWal     = "etcdwal"
	VolatileWal = "volatile"

	DurableSnapshot  = "durable"
	VolatileSnapshot = "volatile"

	MemoryType                       = "memory"
	MemoryWithConcurrentSnapshotType = "memory-concurrent-snapshot"

	DiskType = "disk"
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

	babuzaCfg := babuza.DefaultBabuzaConfig(s.cfg.RaftClusterId,
		s.cfg.RaftLocalPeerId, s.cfg.RaftLocalPeerAddress)
	//enable follower forward proposal to leader
	babuzaCfg.RaftConfig.DisableProposalForwarding = false
	babuzaCfg.Join = s.cfg.JoinRaftCluster
	if s.cfg.RaftEncrypt {
		babuzaCfg.EnableTLS = true
		babuzaCfg.MutualTLS = true
		babuzaCfg.TLSCert = s.cfg.RaftTLSCertFile
		babuzaCfg.TLSKey = s.cfg.RaftTLSKeyFile
		babuzaCfg.TLSRootCA = s.cfg.RaftTLSRootCA
	}
	peersConfiguration := babuza.NewVotingPeersConfiguration()
	for id, endpoint := range s.cfg.RaftClusterVotersAddress {
		if err := peersConfiguration.AddPeer(id, endpoint); err != nil {
			return err
		}
	}

	var zapLogger = logger.NewZapLogger(
		zapcore.DebugLevel, []string{"stdout"}, "")
	raftLogger := logger.NewRaftLogger(zapLogger.Sugar())

	var sessionMgr ibabuza.SessionManager
	switch s.cfg.BabuzaSession {
	case NoOpSession:
		sessionMgr = session.NewNoOpManager(raftLogger)
	case ExpireSession:
		sessionMgr = session.NewExpiredManager(raftLogger)
	case LRUSession:
		sessionMgr = session.NewLruManager(raftLogger)
	}

	switch s.cfg.StateMachine {
	case MemoryType:
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewMemoryStore()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithSession()
		}
	case MemoryWithConcurrentSnapshotType:
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshot()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
		}
	case DiskType:
		dataDir := filepath.Join(s.cfg.RaftStorageDir, "StateMachine")
		if s.cfg.BabuzaSession == NoOpSession {
			s.stateMachine = kvstore.NewDisk(dataDir)
		} else {
			s.stateMachine = kvstore.NewDiskStoreWithSession(dataDir)
		}
	}
	var trans ibabuza.Transport
	switch s.cfg.BabuzaTransportProtocol {
	case TcpTransport:
		trans = transport.New(protocol.NewTcp(networkio.NewTcpPhysicalIO(), raftLogger), raftLogger)
	case HttpTransport:
		trans = transport.New(protocol.NewHttp(raftLogger), raftLogger)
	}

	var walManager ibabuza.WalManager
	walDir := filepath.Join(s.cfg.RaftStorageDir, "wal")
	switch s.cfg.BabuzaWal {
	case BabuzaWal:
		walManager = babuzawal.NewWalManager(walDir, raftLogger)
	case ETCDWal:
		walManager = etcdwal.NewWalManager(walDir, zapLogger)
	}

	var snapshotManager ibabuza.SnapshotManager
	snapDir := filepath.Join(s.cfg.RaftStorageDir, "snap")
	switch s.cfg.BabuzaSnapshot {
	case DurableSnapshot:
		snapshotManager = snapshot.NewDurableSnapshotManager(snapDir, raftLogger)
	case VolatileSnapshot:
		snapshotManager = snapshot.NewVolatileSnapshotManager(snapDir, raftLogger)
	}
	var raftNode ibabuza.RaftNode
	raftNode = raftnode.NewEtcdRaftNode()
	var cls ibabuza.Cluster
	cls = cluster.NewCluster(raftLogger)
	bootstrap, err := babuza.NewBootstrapRaftCluster(babuzaCfg, peersConfiguration, s.stateMachine, cls,
		raftNode, sessionMgr, snapshotManager, walManager, trans, raftLogger)
	if err != nil {
		return err
	}
	r, err := babuza.NewRaft(babuzaCfg, bootstrap)
	if err != nil {
		return err
	}
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
			}
		}
	})
	s.raft = r
	s.logger = raftLogger
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
