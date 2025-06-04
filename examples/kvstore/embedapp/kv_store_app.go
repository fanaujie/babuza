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
	BubuzaConfig   babuza.BabuzaConfig
	VotingPeersCfg *babuza.PeersConfiguration
	ServiceAddress string
}

type KvStoreApp struct {
	serviceAddress            string
	disableProposalForwarding bool
	stopCh                    chan struct{}
	stateMachine              ibabuza.BaseStateMachine
	babuza                    *babuza.Raft
	httpSrv                   *http.Server
	logger                    ibabuza.Logger
}

func (k *KvStoreApp) OnLeaderChange(term, leaderID uint64) {
	k.logger.Infof("service %s: leader changed to %d in term %d", k.serviceAddress, leaderID, term)
}

func (k *KvStoreApp) OnAcquiredLeader(term, leaderID uint64) {
	k.logger.Infof("service %s: acquired leader %d in term %d", k.serviceAddress, leaderID, term)
}

func (k *KvStoreApp) OnLostLeader(term, leaderID uint64) {
	k.logger.Infof("service %s: lost leader %d in term %d", k.serviceAddress, leaderID, term)
}
func (k *KvStoreApp) OnMemberChange(event int, term uint64, peerID uint64) {
	switch event {
	case ibabuza.MemberJoined:
		k.logger.Infof("service %s: member %d joined in term %d", k.serviceAddress, peerID, term)
	case ibabuza.MemberUpdated:
		k.logger.Infof("service %s: member %d updated in term %d", k.serviceAddress, peerID, term)
	case ibabuza.MemberRemoved:
		k.logger.Infof("service %s: member %d removed in term %d", k.serviceAddress, peerID, term)
	case ibabuza.LeanerAdded:
		k.logger.Infof("service %s: learner %d added in term %d", k.serviceAddress, peerID, term)
	case ibabuza.LeanerPromoted:
		k.logger.Infof("service %s: learner %d promoted in term %d", k.serviceAddress, peerID, term)
	default:
		k.logger.Warningf("service %s: unknown member event %d for peer %d in term %d", k.serviceAddress, event, peerID, term)
	}
}

func (k *KvStoreApp) OnRaftShutdown() {
	k.logger.Infof("service %s: raft shutdown", k.serviceAddress)
}

func NewKvStoreApp(appConfig KvStoreAppConfig, stateMachine ibabuza.BaseStateMachine, customBuilder builder.BabuzaComponent) (*KvStoreApp, error) {
	app := &KvStoreApp{}
	app.logger = customBuilder.Logger
	app.stateMachine = stateMachine
	bootstrap, err := babuza.NewBootstrapRaftCluster(
		appConfig.BubuzaConfig, *appConfig.VotingPeersCfg, stateMachine, customBuilder.Cluster,
		customBuilder.RaftNode, customBuilder.SessionManager, customBuilder.SnapshotManager, customBuilder.WalManager,
		customBuilder.Transport, customBuilder.Logger, customBuilder.MetricsController)
	if err != nil {
		return nil, err
	}
	r, err := babuza.NewRaft(appConfig.BubuzaConfig, bootstrap, app)
	if err != nil {
		return nil, err
	}
	app.serviceAddress = appConfig.ServiceAddress
	app.disableProposalForwarding = appConfig.BubuzaConfig.DisableProposalForwarding
	app.stopCh = make(chan struct{})
	app.babuza = r
	return app, nil
}

func (k *KvStoreApp) StartService() error {
	m := http.NewServeMux()
	m.Handle(api.SessionsHttpPath, api.NewSessionResourceHandler(k.babuza))
	m.Handle(api.ClusterPeersHttpPath, api.NewClusterPeerResourceHandler(k.babuza))
	m.Handle(api.PromoteLearnerHttpPath, api.NewPromoteLearnerHandler(k.babuza))
	m.Handle(api.TransferLeaderHttpPath, api.NewTransferLeaderHandler(k.babuza))
	m.Handle(api.KvHttpPath, api.NewKvStoreResourceHandler(k.babuza, k.stateMachine.(api.ReadKvStore)))
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
	// close state machine
	if err := k.stateMachine.Close(); err != nil {
		me.Append(err)
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
