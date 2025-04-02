package embedapp

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/api"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	babuza "github.com/fanaujie/babuza/raft"
	"net/http"
)

type KvStoreAppConfig struct {
	BubuzaConfig     babuza.BabuzaConfig
	VotingPeersCfg   *babuza.VotingPeersConfiguration
	ServiceAddress   string
	LinearizableRead bool
}

type KvStoreApp struct {
	isLeader                  uint64 //must use atomic operations to access; keep 64-bit aligned
	serviceAddress            string
	disableProposalForwarding bool
	enableLinearizableRead    bool
	stopCh                    chan struct{}
	stateMachine              ibabuza.BaseStateMachine
	babuza                    *babuza.Raft
	httpSrv                   *http.Server
}

func NewKvStoreApp(appConfig KvStoreAppConfig, stateMachine ibabuza.BaseStateMachine, customBuilder builder.BabuzaComponent) (*KvStoreApp, error) {
	app := &KvStoreApp{}
	app.stateMachine = stateMachine
	bootstrap, err := babuza.NewBootstrapRaftCluster(
		appConfig.BubuzaConfig, *appConfig.VotingPeersCfg, stateMachine, customBuilder.Cluster,
		customBuilder.RaftNode, customBuilder.SessionManager, customBuilder.SnapshotManager, customBuilder.WalManager,
		customBuilder.Transport, customBuilder.Logger)
	if err != nil {
		return nil, err
	}
	r, err := babuza.NewRaft(appConfig.BubuzaConfig, bootstrap)
	if err != nil {
		return nil, err
	}
	app.serviceAddress = appConfig.ServiceAddress
	app.disableProposalForwarding = appConfig.BubuzaConfig.DisableProposalForwarding
	app.enableLinearizableRead = appConfig.LinearizableRead
	app.stopCh = make(chan struct{})
	app.babuza = r
	go func() {
		for {
			select {
			case <-app.stopCh:
				return
			case isLeader := <-r.LeaderCh():
				if isLeader {
					app.isLeader = 1
				} else {
					app.isLeader = 0
				}
			case <-r.ClusterMemberEventCh():
			}
		}
	}()
	return app, nil
}

func (k *KvStoreApp) StartService() error {
	m := http.NewServeMux()
	m.Handle(api.SessionsHttpPath, api.NewSessionResourceHandler(k.babuza))
	m.Handle(api.ClusterPeersHttpPath, api.NewClusterPeerResourceHandler(k.babuza))
	m.Handle(api.PromoteLearnerHttpPath, api.NewPromoteLearnerHandler(k.babuza))
	m.Handle(api.TransferLeaderHttpPath, api.NewTransferLeaderHandler(k.babuza))
	m.Handle(api.KvHttpPath, api.NewKvStoreResourceHandler(k.enableLinearizableRead, k.babuza, k.stateMachine.(api.ReadKvStore)))
	k.httpSrv = &http.Server{
		Addr:    k.serviceAddress,
		Handler: m,
	}
	k.httpSrv.ListenAndServe()
	return nil
}

func (k *KvStoreApp) PublishService(ctx context.Context) chan error {
	return k.babuza.ApplicationServiceStart(ctx, []string{k.serviceAddress})
}

func (k *KvStoreApp) Stop() error {
	me := multierror.New()
	me.Append(k.httpSrv.Close())
	if err := k.babuza.Shutdown().Wait(); err != nil {
		if !errors.Is(err, babuza.ErrStopped) {
			me.Append(err)
		}
	}
	close(k.stopCh)
	return me.Get()
}

func (k *KvStoreApp) Raft() *babuza.Raft {
	return k.babuza
}

func (k *KvStoreApp) StateMachineHash() uint32 {
	return k.stateMachine.(api.ReadKvStore).Hash()
}
