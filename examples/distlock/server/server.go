package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/server/api"
	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	babuza "github.com/fanaujie/babuza/raft"
)

type Config struct {
	HttpAddress              string
	RaftClusterId            uint64
	RaftLocalPeerId          uint64
	RaftLocalPeerAddress     string
	RaftClusterVotersAddress map[uint64]string
	RaftStorageDir           string
	JoinRaftCluster          bool
}

type Server struct {
	cfg          Config
	raft         *babuza.Raft
	httpSrv      *http.Server
	stateMachine *lockstore.LockStore
	lockHandler  *api.LockResourceHandler
	leaseHandler *api.LeaseResourceHandler
	logger       ibabuza.Logger
	closer       *syncutil.Closer

	tickerMu     sync.Mutex
	tickerCancel context.CancelFunc
}

func (s *Server) OnAcquiredLeader() {
	s.tickerMu.Lock()
	defer s.tickerMu.Unlock()

	if s.tickerCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.tickerCancel = cancel

	go s.runTicker(ctx)
	s.logger.Infof("Leader ticker started")
}

func (s *Server) OnLostLeader() {
	s.tickerMu.Lock()
	defer s.tickerMu.Unlock()

	if s.tickerCancel != nil {
		s.tickerCancel()
		s.tickerCancel = nil
		s.logger.Infof("Leader ticker stopped")
	}
}

func (s *Server) runTicker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.leaseHandler.ShouldTick() {
				s.leaseHandler.ProposeTick()
			}
		}
	}
}

func (s *Server) OnLeaderChange(term, leaderID uint64) {
	s.logger.Infof("Leader changed to %d in term %d", leaderID, term)
}

func (s *Server) OnMemberChange(memberEvent int, term, peerID uint64) {
	s.logger.Infof("Member event %d for peer %d in term %d", memberEvent, peerID, term)
}

func (s *Server) OnRaftShutdown() {
	s.logger.Infof("Shutting down")
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

	babuzaCfg.Join = s.cfg.JoinRaftCluster

	peersConfiguration := babuza.NewPeersConfiguration()
	for id, endpoint := range s.cfg.RaftClusterVotersAddress {
		if err := peersConfiguration.AddPeer(id, endpoint, false); err != nil {
			return err
		}
	}

	s.stateMachine = lockstore.NewLockStore()

	babuzaComponents := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		ClusterId:      s.cfg.RaftClusterId,
		StorageRootDir: s.cfg.RaftStorageDir,
		SessionType:    builder.NoOpSession,
		TransportType:  builder.TcpTransport,
		WalType:        builder.BadgerWalMemory,
		SnapshotType:   builder.VolatileSnapshot,
		MetricType:     builder.MetricsPrometheus,
	}).Build()

	bootstrap, err := babuza.NewBootstrapRaftCluster(babuzaCfg, *peersConfiguration, s.stateMachine,
		babuzaComponents.Cluster,
		babuzaComponents.RaftNode,
		babuzaComponents.SessionManager,
		babuzaComponents.SnapshotManager,
		babuzaComponents.WalManager,
		babuzaComponents.Transport,
		babuzaComponents.Logger,
		babuzaComponents.MetricsController)
	if err != nil {
		return err
	}

	r, err := babuza.NewRaft(babuzaCfg, bootstrap, s)
	if err != nil {
		return err
	}

	s.raft = r
	s.logger = babuzaComponents.Logger

	if err = <-s.raft.ApplicationServiceStart(context.Background(), []string{s.cfg.HttpAddress}); err != nil {
		return err
	}

	return s.startService()
}

func (s *Server) startService() error {
	m := http.NewServeMux()
	s.lockHandler = api.NewLockResourceHandler(s.raft, s.stateMachine)
	s.leaseHandler = api.NewLeaseResourceHandler(s.raft, s.stateMachine, s.lockHandler)
	m.Handle(api.LocksPath, corsMiddleware(s.lockHandler))
	m.Handle(api.LeasesPath, corsMiddleware(s.leaseHandler))

	s.httpSrv = &http.Server{
		Addr:    s.cfg.HttpAddress,
		Handler: m,
	}

	return s.httpSrv.ListenAndServe()
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
