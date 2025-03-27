package testcluster

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	babuza "github.com/fanaujie/babuza/raft"
	"math/rand"
	"path/filepath"
	"time"
)

type BabuzaPeer struct {
	Id                  uint64
	RaftListenAddr      string
	TLSConfig           ibabuza.TLSConfig
	ProxyListenAddr     string
	ProxyTLSConfig      ibabuza.TLSConfig
	AppServiceAddresses []string
	IsLearner           bool
}

type EmbeddedApp interface {
	PublishService(context.Context) chan error
	StartService() error
	Stop() error
	Raft() *babuza.Raft
	StateMachineHash() uint32
}

type EmbeddedClient interface {
	Join(ctx context.Context, peerId uint64, raftListenAddr string, isLearner bool) error
	Update(ctx context.Context, peerId uint64, raftListenAddr string) error
	Remove(ctx context.Context, peerId uint64) error
	PromoteLearner(ctx context.Context, peerId uint64) error
	TransferLeader(ctx context.Context, transferee uint64) error
}

type CreateEmbeddedApp func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool,
	testNetwork ibabuza.ProxyNetwork, peerRootDir string, appServiceAddresses []string) (EmbeddedApp, error)

type appController struct {
	app                  EmbeddedApp
	appsServiceAddresses []string
	appStopCh            chan error
}

func (a *appController) startService() {
	a.appStopCh <- a.app.StartService()
}

func (a *appController) stop() error {
	return a.app.Stop()
}

func (a *appController) wait() error {
	return <-a.appStopCh
}

type BabuzaCluster struct {
	clusterId         uint64
	storageRootDir    string
	config            babuza.BabuzaConfig
	createEmbeddedApp CreateEmbeddedApp
	votingPeersCfg    *babuza.VotingPeersConfiguration
	appControllers    map[uint64]*appController
	proxyNetwork      ibabuza.ProxyNetwork
	useProxyNetwork   bool // New flag to control proxy network usage
}

func CreateTestCluster(clusterId uint64, storageRootDir string, proxyNetwork ibabuza.ProxyNetwork,
	createEmbeddedApp CreateEmbeddedApp) *BabuzaCluster {
	config := babuza.DefaultBabuzaConfig(0, 0, "")
	useProxyNetwork := proxyNetwork != nil

	return &BabuzaCluster{
		clusterId:         clusterId,
		config:            config,
		storageRootDir:    storageRootDir,
		createEmbeddedApp: createEmbeddedApp,
		votingPeersCfg:    babuza.NewVotingPeersConfiguration(),
		appControllers:    make(map[uint64]*appController),
		proxyNetwork:      proxyNetwork,
		useProxyNetwork:   useProxyNetwork,
	}
}

// Helper method to determine which address to use
func (c *BabuzaCluster) getPeerListenAddress(peer BabuzaPeer) string {
	if c.useProxyNetwork {
		return peer.ProxyListenAddr
	}
	return peer.RaftListenAddr
}

func (c *BabuzaCluster) RaftElectionTimeout() time.Duration {
	return time.Duration(c.config.RaftConfig.LogicalTickMs*c.config.RaftConfig.ElectionTicks) * time.Millisecond
}

func (c *BabuzaCluster) MakeCluster(wait time.Duration, votingPeers []BabuzaPeer) error {
	for _, peer := range votingPeers {
		if peer.IsLearner {
			return fmt.Errorf("test cluster: failed to make cluster. found a learner peer id(%d)", peer.Id)
		}
		peerAddr := c.getPeerListenAddress(peer)
		if err := c.votingPeersCfg.AddPeer(peer.Id, peerAddr); err != nil {
			return err
		}
	}
	connectedGroup := make([]uint64, 0, len(votingPeers))
	for _, peer := range votingPeers {
		cfg, peerRootDir, err := c.genPeerConfig(peer, false)
		connectedGroup = append(connectedGroup, peer.Id)
		app, err := c.createEmbeddedApp(c.votingPeersCfg.Clone(), cfg, false, c.proxyNetwork, peerRootDir, peer.AppServiceAddresses)
		if err != nil {
			return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(false) join(false). err=%s", peer, err)
		}

		if c.useProxyNetwork {
			if err = c.proxyNetwork.AddProxy(ibabuza.ProxyConfig{
				Id:        peer.Id,
				InAddr:    peer.ProxyListenAddr,
				OutAddr:   peer.RaftListenAddr,
				TLSConfig: peer.ProxyTLSConfig,
			}); err != nil {
				return err
			}

			if err = c.proxyNetwork.ConnectProxy(peer.Id); err != nil {
				return err
			}
		}

		controller := &appController{
			app:                  app,
			appsServiceAddresses: peer.AppServiceAddresses,
			appStopCh:            make(chan error, 1),
		}
		c.appControllers[peer.Id] = controller
	}

	if c.useProxyNetwork {
		if err := c.proxyNetwork.SetPartition(connectedGroup); err != nil {
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for _, controller := range c.appControllers {
		if err := <-controller.app.PublishService(ctx); err != nil {
			return err
		}
		continue
	}
	for _, controller := range c.appControllers {
		go controller.startService()
	}
	return nil
}

func (c *BabuzaCluster) GetAllAppServiceAddresses() map[uint64][]string {
	result := make(map[uint64][]string)
	for id, controller := range c.appControllers {
		result[id] = controller.appsServiceAddresses
	}
	return result
}

func (c *BabuzaCluster) Teardown() error {
	mu := multierror.New()
	if c.useProxyNetwork {
		if err := c.proxyNetwork.TeardownNetwork(); err != nil {
			mu.Append(err)
		}
	}
	for _, controller := range c.appControllers {
		mu.Append(controller.stop())
		mu.Append(controller.wait())
	}
	c.appControllers = make(map[uint64]*appController)
	return mu.Get()
}

func (c *BabuzaCluster) JoinPeerToCluster(wait time.Duration, client EmbeddedClient, peer BabuzaPeer, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	peerAddr := c.getPeerListenAddress(peer)
	if err := client.Join(ctx, peer.Id, peerAddr, peer.IsLearner); err != nil {
		return err
	}

	cfg, appStorageDir, err := c.genPeerConfig(peer, true)
	if !peer.IsLearner {
		if err = c.votingPeersCfg.AddPeer(peer.Id, peerAddr); err != nil {
			return fmt.Errorf("test cluster: failed to add peer to votingPeersCfg (peerId=%d) (endpoint=%s). err=%s",
				peer.Id, peerAddr, err)
		}
	}

	if c.useProxyNetwork {
		if err = c.proxyNetwork.AddProxy(ibabuza.ProxyConfig{
			Id:        peer.Id,
			InAddr:    peer.ProxyListenAddr,
			OutAddr:   peer.RaftListenAddr,
			TLSConfig: peer.ProxyTLSConfig,
		}); err != nil {
			return err
		}

		if err = c.proxyNetwork.ConnectProxy(peer.Id); err != nil {
			return err
		}

		if err = c.proxyNetwork.SetPartition(connectedGroup); err != nil {
			return err
		}
	}

	app, err := c.createEmbeddedApp(c.votingPeersCfg.Clone(), cfg, false, c.proxyNetwork, appStorageDir, peer.AppServiceAddresses)
	if err != nil {
		return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(false) join(true). err=%s", peer, err)
	}

	controller := &appController{
		app:                  app,
		appsServiceAddresses: peer.AppServiceAddresses,
		appStopCh:            make(chan error, 1),
	}
	c.appControllers[peer.Id] = controller
	if err = <-app.PublishService(ctx); err != nil {
		return err
	}
	go controller.startService()
	return nil
}

func (c *BabuzaCluster) RemovePeerFromCluster(wait time.Duration, client EmbeddedClient, peerId uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := client.Remove(ctx, peerId); err != nil {
		return err
	}

	if c.useProxyNetwork {
		if err := c.proxyNetwork.DeleteProxy(peerId); err != nil {
			return err
		}
	}

	c.votingPeersCfg.RemovePeer(peerId)
	controller, ok := c.appControllers[peerId]
	me := multierror.New()
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerId)
	}
	me.Append(controller.stop())
	me.Append(controller.wait())
	delete(c.appControllers, peerId)
	return me.Get()
}

func (c *BabuzaCluster) ShutdownPeer(peerId uint64) error {
	controller, ok := c.appControllers[peerId]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerId)
	}
	me := multierror.New()

	if c.useProxyNetwork {
		if err := c.proxyNetwork.DeleteProxy(peerId); err != nil {
			me.Append(err)
		}
	}

	me.Append(controller.stop())
	me.Append(controller.wait())
	delete(c.appControllers, peerId)
	return me.Get()
}

func (c *BabuzaCluster) RestartPeer(wait time.Duration, peer BabuzaPeer, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if _, ok := c.appControllers[peer.Id]; ok {
		return fmt.Errorf("test cluster: found app (id=%d)", peer.Id)
	}

	cfg, storageDir, err := c.genPeerConfig(peer, false)
	app, err := c.createEmbeddedApp(c.votingPeersCfg, cfg, true, c.proxyNetwork, storageDir, peer.AppServiceAddresses)
	if err != nil {
		return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(true) join(false). err=%s", peer, err)
	}

	if c.useProxyNetwork {
		if err = c.proxyNetwork.AddProxy(ibabuza.ProxyConfig{
			Id:        peer.Id,
			InAddr:    peer.ProxyListenAddr,
			OutAddr:   peer.RaftListenAddr,
			TLSConfig: peer.ProxyTLSConfig,
		}); err != nil {
			return err
		}

		if err = c.proxyNetwork.ConnectProxy(peer.Id); err != nil {
			return err
		}

		if err = c.proxyNetwork.SetPartition(connectedGroup); err != nil {
			return err
		}
	}

	controller := &appController{
		app:                  app,
		appsServiceAddresses: peer.AppServiceAddresses,
		appStopCh:            make(chan error, 1),
	}
	c.appControllers[peer.Id] = controller
	if err = <-controller.app.PublishService(ctx); err != nil {
		return err
	}
	go controller.startService()
	return nil
}

func (c *BabuzaCluster) ExecutePeerRaftOperation(peerId uint64, raftOperation func(r *babuza.Raft) error) error {
	controller, ok := c.appControllers[peerId]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerId)
	}
	return raftOperation(controller.app.Raft())
}

func (c *BabuzaCluster) DisconnectPeer(peerId uint64) error {
	if c.useProxyNetwork {
		return c.proxyNetwork.DisconnectProxy(peerId)
	}
	return nil // No operation needed in direct connection mode
}

func (c *BabuzaCluster) ConnectPeer(peerId uint64) error {
	if c.useProxyNetwork {
		return c.proxyNetwork.ConnectProxy(peerId)
	}
	return nil // No operation needed in direct connection mode
}

func (c *BabuzaCluster) CheckOneLeader(wait time.Duration, connectedGroup []uint64) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()

	for {
		select {
		case <-checkT.C:
			if leaderId, _, ok := c.findConsensusLeader(connectedGroup); ok {
				return leaderId, nil
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (c *BabuzaCluster) CheckNoLeader(wait time.Duration, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()

	for {
		select {
		case <-checkT.C:
			if !c.hasLeader(connectedGroup) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *BabuzaCluster) CheckPeersConsistency(wait time.Duration, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()

	for {
		select {
		case <-checkT.C:
			if c.areStateMachinesConsistent(connectedGroup) {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("timeout while checking state machine consistency: %w", ctx.Err())
		}
	}
}

func (c *BabuzaCluster) CheckStatus(wait time.Duration, peerId uint64, matchFunc func(s babuza.Status) bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()
	controller, ok := c.appControllers[peerId]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerId)
	}
	for {
		select {
		case <-checkT.C:
			if matchFunc(controller.app.Raft().Status()) == true {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *BabuzaCluster) getCheckTimeout() time.Duration {
	return c.RaftElectionTimeout() + time.Duration(rand.Int63n(int64(c.RaftElectionTimeout()/10)))
}

func (c *BabuzaCluster) genPeerConfig(peer BabuzaPeer, join bool) (babuza.BabuzaConfig, string, error) {
	cfg := c.config
	cfg.ClusterId = c.clusterId
	cfg.LocalPeerId = peer.Id
	cfg.RaftListenAddress = peer.RaftListenAddr
	cfg.TLSConfig = peer.TLSConfig
	cfg.Join = join
	peerDir := filepath.Join(c.storageRootDir, fmt.Sprintf("%d", peer.Id))
	return cfg, peerDir, nil
}

func (c *BabuzaCluster) findConsensusLeader(peerIds []uint64) (uint64, uint64, bool) {
	var leaderId, term uint64

	for _, id := range peerIds {
		controller, ok := c.appControllers[id]
		if !ok {
			return 0, 0, false
		}

		status := controller.app.Raft().Status()
		if status.LeaderId == babuza.None {
			return 0, 0, false
		}

		if term == 0 {
			term = status.RaftTerm
			leaderId = status.LeaderId
			continue
		}

		if term != status.RaftTerm || leaderId != status.LeaderId {
			return 0, 0, false
		}
	}

	return leaderId, term, true
}

func (c *BabuzaCluster) hasLeader(peerIds []uint64) bool {
	for _, id := range peerIds {
		controller, ok := c.appControllers[id]
		if !ok {
			continue
		}
		status := controller.app.Raft().Status()
		if status.LeaderId != babuza.None {
			return false
		}
	}
	return true
}

func (c *BabuzaCluster) areStateMachinesConsistent(peerIds []uint64) bool {
	hashMap := c.collectStateMachineHashes(peerIds)
	if len(hashMap) == 0 {
		return false
	}
	var baseHash uint32
	for _, hash := range hashMap {
		if baseHash == 0 {
			baseHash = hash
			continue
		}
		if baseHash != hash {
			return false
		}
	}
	return true
}

func (c *BabuzaCluster) collectStateMachineHashes(peerIds []uint64) map[uint64]uint32 {
	hashes := make(map[uint64]uint32, len(peerIds))
	for _, id := range peerIds {
		controller, ok := c.appControllers[id]
		if !ok {
			continue
		}
		hashes[id] = controller.app.StateMachineHash()
	}
	return hashes
}
