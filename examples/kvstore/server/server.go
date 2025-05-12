package server

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/api"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
	"net/http"
	"path/filepath"
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
	peersConfiguration := babuza.NewPeersConfiguration()
	for id, endpoint := range s.cfg.RaftClusterVotersAddress {
		if err := peersConfiguration.AddPeer(id, endpoint, false); err != nil {
			return err
		}
	}

	// Set up State machine
	switch s.cfg.StateMachine {
	case builder.StateMachineMemory:
		if s.cfg.BabuzaSession == builder.NoOpSession {
			s.stateMachine = kvstore.NewMemoryStore()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithSession()
		}
	case builder.StateMachineMemoryWithConcurrentSnapshotType:
		if s.cfg.BabuzaSession == builder.NoOpSession {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshot()
		} else {
			s.stateMachine = kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
		}
	case builder.StateMachineDisk:
		dataDir := filepath.Join(s.cfg.RaftStorageDir, "StateMachine")
		if s.cfg.BabuzaSession == builder.NoOpSession {
			s.stateMachine = kvstore.NewDisk(dataDir)
		} else {
			s.stateMachine = kvstore.NewDiskStoreWithSession(dataDir)
		}
	default:
		return fmt.Errorf("unsupported state machine type: %s", s.cfg.StateMachine)
	}

	babuzaComponets := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		ClusterId:      s.cfg.RaftClusterId,
		StorageRootDir: s.cfg.RaftStorageDir,
		SessionType:    s.cfg.BabuzaSession,
		TransportType:  s.cfg.BabuzaTransportProtocol,
		WalType:        s.cfg.BabuzaWal,
		SnapshotType:   s.cfg.BabuzaSnapshot,
		MetricType:     builder.MetricsPrometheus,
		MinIOConfig: &cloudstorage.Config{
			Endpoint:        s.cfg.MinIOEndpoint,
			AccessKeyID:     s.cfg.MinIOAccessKeyID,
			SecretAccessKey: s.cfg.MinIOSecretAccessKey,
			UseSSL:          s.cfg.MinIOUseSSL,
			Bucket:          s.cfg.MinIOBucket,
			Prefix:          s.cfg.MinIOPrefix,
		},
	}).Build()

	// Initialize Raft cluster using BootstrapBuilder
	bootstrap, err := babuza.NewBootstrapRaftCluster(babuzaCfg, *peersConfiguration, s.stateMachine, babuzaComponets.Cluster,
		babuzaComponets.RaftNode, babuzaComponets.SessionManager, babuzaComponets.SnapshotManager, babuzaComponets.WalManager,
		babuzaComponets.Transport, babuzaComponets.Logger, babuzaComponets.MetricsController)
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
	s.logger = babuzaComponets.Logger

	// Start application service
	if err = <-s.raft.ApplicationServiceStart(context.Background(), []string{s.cfg.KvServiceHttpAddress}); err != nil {
		return err
	}

	return s.startService()
}

// @title Babuza KV Store API
// @version 1.0
// @description A distributed key-value store using Babuza
// @license.name Apache 2.0
// @BasePath /
func (s *Server) startService() error {
	m := http.NewServeMux()
	m.Handle(api.SessionsHttpPath, corsMiddleware(api.NewSessionResourceHandler(s.raft)))
	m.Handle(api.ClusterPeersHttpPath, corsMiddleware(api.NewClusterPeerResourceHandler(s.raft)))
	m.Handle(api.PromoteLearnerHttpPath, corsMiddleware(api.NewPromoteLearnerHandler(s.raft)))
	m.Handle(api.TransferLeaderHttpPath, corsMiddleware(api.NewTransferLeaderHandler(s.raft)))
	m.Handle(api.KvHttpPath, corsMiddleware(api.NewKvStoreResourceHandler(s.raft, s.stateMachine.(api.ReadKvStore))))
	m.Handle("/metrics", promhttp.Handler())
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

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow any origin
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}
