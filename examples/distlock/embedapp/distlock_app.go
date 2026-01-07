package embedapp

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/server/api"
	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	babuza "github.com/fanaujie/babuza/raft"
)

type DistLockAppConfig struct {
	BabuzaConfig   babuza.BabuzaConfig
	VotingPeersCfg *babuza.PeersConfiguration
	ServiceAddress string
}

type DistLockApp struct {
	serviceAddress string
	stopCh         chan struct{}
	stateMachine   *lockstore.LockStore
	babuza         *babuza.Raft
	httpSrv        *http.Server
	logger         ibabuza.Logger
	lockHandler    *api.LockResourceHandler
	leaseHandler   *api.LeaseResourceHandler

	tickerMu     sync.Mutex
	tickerCancel context.CancelFunc
}

func (k *DistLockApp) OnLeaderChange(term, leaderID uint64) {
	k.logger.Infof("service %s: leader changed to %d in term %d", k.serviceAddress, leaderID, term)
}

func (k *DistLockApp) OnAcquiredLeader() {
	k.tickerMu.Lock()
	defer k.tickerMu.Unlock()

	if k.tickerCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	k.tickerCancel = cancel

	go k.runTicker(ctx)
	k.logger.Infof("Leader ticker started")
}

func (k *DistLockApp) OnLostLeader() {
	k.tickerMu.Lock()
	defer k.tickerMu.Unlock()

	if k.tickerCancel != nil {
		k.tickerCancel()
		k.tickerCancel = nil
		k.logger.Infof("Leader ticker stopped")
	}
}

func (k *DistLockApp) runTicker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if k.leaseHandler.ShouldTick() {
				k.leaseHandler.ProposeTick()
			}
		}
	}
}

func (k *DistLockApp) OnMemberChange(event int, term uint64, peerID uint64) {
	k.logger.Infof("service %s: member event %d for peer %d in term %d", k.serviceAddress, event, peerID, term)
}

func (k *DistLockApp) OnRaftShutdown() {
	k.logger.Infof("service %s: raft shutdown", k.serviceAddress)
}

func NewDistLockApp(appConfig DistLockAppConfig, stateMachine *lockstore.LockStore, customBuilder builder.BabuzaComponent) (*DistLockApp, error) {
	app := &DistLockApp{}
	app.logger = customBuilder.Logger
	app.stateMachine = stateMachine

	bootstrap, err := babuza.NewBootstrapRaftCluster(
		appConfig.BabuzaConfig, *appConfig.VotingPeersCfg, stateMachine, customBuilder.Cluster,
		customBuilder.RaftNode, customBuilder.SessionManager, customBuilder.SnapshotManager, customBuilder.WalManager,
		customBuilder.Transport, customBuilder.Logger, customBuilder.MetricsController)

	if err != nil {
		return nil, err
	}
	r, err := babuza.NewRaft(appConfig.BabuzaConfig, bootstrap, app)
	if err != nil {
		return nil, err
	}
	app.serviceAddress = appConfig.ServiceAddress
	app.stopCh = make(chan struct{})
	app.babuza = r
	return app, nil
}

func (k *DistLockApp) StartService() error {
	m := http.NewServeMux()
	k.lockHandler = api.NewLockResourceHandler(k.babuza, k.stateMachine)
	k.leaseHandler = api.NewLeaseResourceHandler(k.babuza, k.stateMachine, k.lockHandler)
	m.Handle(api.LocksPath, k.lockHandler)
	m.Handle(api.LeasesPath, k.leaseHandler)
	k.httpSrv = &http.Server{
		Addr:    k.serviceAddress,
		Handler: m,
	}
	k.httpSrv.ListenAndServe()
	return nil
}

func (k *DistLockApp) PublishService(ctx context.Context) chan error {
	return k.babuza.ApplicationServiceStart(ctx, []string{k.serviceAddress})
}

func (k *DistLockApp) Stop() error {
	me := multierror.New()
	if k.httpSrv != nil {
		me.Append(k.httpSrv.Close())
	}
	if err := k.babuza.Shutdown().Wait(); err != nil {
		if !errors.Is(err, babuza.ErrStopped) {
			me.Append(err)
		}
	}
	if err := k.stateMachine.Close(); err != nil {
		me.Append(err)
	}
	close(k.stopCh)
	return me.Get()
}

func (k *DistLockApp) Raft() *babuza.Raft {
	return k.babuza
}

func (k *DistLockApp) StateMachineHash() uint32 {
	return k.stateMachine.Hash()
}

func (k *DistLockApp) LockStore() *lockstore.LockStore {
	return k.stateMachine
}
