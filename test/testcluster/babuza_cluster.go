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

type EmbeddedApp interface {
	PublishService(context.Context) chan error
	StartService() error
	Stop() error
	Raft() *babuza.Raft
	StateMachineHash() uint32
}

type EmbeddedClient interface {
	Join(ctx context.Context, peerID uint64, raftListenAddr string, isLearner bool) error
	Update(ctx context.Context, peerID uint64, raftListenAddr string) error
	Remove(ctx context.Context, peerID uint64) error
	PromoteLearner(ctx context.Context, peerID uint64) error
	TransferLeader(ctx context.Context, transferee uint64) error
}

type CreateEmbeddedApp func(votingPeersCfg *babuza.PeersConfiguration, config babuza.BabuzaConfig, restart bool,
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

func (a *appController) waitServiceStop() error {
	return <-a.appStopCh
}

type BabuzaCluster struct {
	clusterID          uint64
	storageRootDir     string
	config             babuza.BabuzaConfig
	createEmbeddedApp  CreateEmbeddedApp
	peersConfiguration *babuza.PeersConfiguration
	appControllers     map[uint64]*appController
	proxyNetwork       ibabuza.ProxyNetwork
	useProxyNetwork    bool // New flag to control proxy network usage
}

func CreateTestCluster(clusterID uint64, storageRootDir string, proxyNetwork ibabuza.ProxyNetwork,
	createEmbeddedApp CreateEmbeddedApp) *BabuzaCluster {
	config := babuza.DefaultBabuzaConfig(0, 0, "")
	useProxyNetwork := proxyNetwork != nil

	return &BabuzaCluster{
		clusterID:          clusterID,
		config:             config,
		storageRootDir:     storageRootDir,
		createEmbeddedApp:  createEmbeddedApp,
		peersConfiguration: babuza.NewPeersConfiguration(),
		appControllers:     make(map[uint64]*appController),
		proxyNetwork:       proxyNetwork,
		useProxyNetwork:    useProxyNetwork,
	}
}

func (c *BabuzaCluster) RaftElectionTimeout() time.Duration {
	return time.Duration(c.config.RaftConfig.LogicalTickMs*c.config.RaftConfig.ElectionTicks) * time.Millisecond
}

func (c *BabuzaCluster) IsUseProxyNetwork() bool {
	return c.useProxyNetwork
}

func (c *BabuzaCluster) GetAllRaft() map[uint64]*babuza.Raft {
	var result = make(map[uint64]*babuza.Raft, len(c.appControllers))
	for _, app := range c.appControllers {
		if app == nil {
			continue
		}
		r := app.app.Raft()
		result[app.app.Raft().Status().LocalPeerID] = r
	}
	return result
}

func (c *BabuzaCluster) MakeCluster(wait time.Duration, votingPeers []Peer) error {
	for _, peer := range votingPeers {
		if peer.IsPeerLearner() {
			return fmt.Errorf("test cluster: failed to make cluster. found a learner peer id(%d)", peer.ID())
		}
		raftListenAddr := peer.RaftListenAddress(c.useProxyNetwork)
		if err := c.peersConfiguration.AddPeer(peer.ID(), raftListenAddr, false); err != nil {
			return err
		}
	}
	connectedGroup := make([]uint64, 0, len(votingPeers))
	for _, peer := range votingPeers {
		cfg, peerRootDir, err := c.genPeerConfig(peer, false)
		connectedGroup = append(connectedGroup, peer.ID())
		app, err := c.createEmbeddedApp(c.peersConfiguration.Clone(), cfg, false, c.proxyNetwork, peerRootDir, peer.ApplicationServiceAddresses())
		if err != nil {
			return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(false) join(false). err=%s", peer, err)
		}

		if c.useProxyNetwork {
			proxyPeer, ok := peer.(ProxyPeer)
			if !ok {
				return fmt.Errorf("test cluster: failed to cast peer to ProxyPeer. peer(%v)", peer)
			}
			if err = c.proxyNetwork.AddProxy(proxyPeer.ProxyConfig()); err != nil {
				return err
			}

			if err = c.proxyNetwork.ConnectProxy(peer.ID()); err != nil {
				return err
			}
		}

		controller := &appController{
			app:                  app,
			appsServiceAddresses: peer.ApplicationServiceAddresses(),
			appStopCh:            make(chan error, 1),
		}
		c.appControllers[peer.ID()] = controller
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

func (c *BabuzaCluster) GetAppServiceAddresses(peerIDs []uint64) map[uint64][]string {
	result := make(map[uint64][]string)
	for _, id := range peerIDs {
		controller, ok := c.appControllers[id]
		if !ok {
			continue
		}
		if controller.appsServiceAddresses != nil {
			result[id] = controller.appsServiceAddresses
		} else {
			panic(fmt.Errorf("test cluster: app service address not found (id=%d)", id))
		}
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
		mu.Append(controller.waitServiceStop())
	}
	c.appControllers = make(map[uint64]*appController)
	return mu.Get()
}

func (c *BabuzaCluster) JoinPeerToCluster(wait time.Duration, client EmbeddedClient, peer Peer, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	raftListenAddr := peer.RaftListenAddress(c.useProxyNetwork)
	if err := client.Join(ctx, peer.ID(), raftListenAddr, peer.IsPeerLearner()); err != nil {
		return err
	}

	cfg, appStorageDir, err := c.genPeerConfig(peer, true)

	if err = c.peersConfiguration.AddPeer(peer.ID(), raftListenAddr, peer.IsPeerLearner()); err != nil {
		return fmt.Errorf("test cluster: failed to add peer to peersConfiguration (peerID=%d) (endpoint=%s). err=%s",
			peer.ID(), raftListenAddr, err)
	}

	if c.useProxyNetwork {
		proxyPeer, ok := peer.(ProxyPeer)
		if !ok {
			return fmt.Errorf("test cluster: failed to cast peer to ProxyPeer. peer(%v)", peer)
		}
		if err = c.proxyNetwork.AddProxy(proxyPeer.ProxyConfig()); err != nil {
			return err
		}

		if err = c.proxyNetwork.ConnectProxy(peer.ID()); err != nil {
			return err
		}

		if err = c.proxyNetwork.SetPartition(connectedGroup); err != nil {
			return err
		}
	}

	app, err := c.createEmbeddedApp(c.peersConfiguration.Clone(), cfg, false, c.proxyNetwork, appStorageDir, peer.ApplicationServiceAddresses())
	if err != nil {
		return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(false) join(true). err=%s", peer, err)
	}

	controller := &appController{
		app:                  app,
		appsServiceAddresses: peer.ApplicationServiceAddresses(),
		appStopCh:            make(chan error, 1),
	}
	c.appControllers[peer.ID()] = controller
	if err = <-app.PublishService(ctx); err != nil {
		return err
	}
	go controller.startService()
	return nil
}

func (c *BabuzaCluster) RemovePeerFromCluster(wait time.Duration, client EmbeddedClient, peerID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := client.Remove(ctx, peerID); err != nil {
		return err
	}

	if c.useProxyNetwork {
		if err := c.proxyNetwork.DeleteProxy(peerID); err != nil {
			return err
		}
	}

	c.peersConfiguration.RemovePeer(peerID)
	controller, ok := c.appControllers[peerID]
	me := multierror.New()
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerID)
	}
	me.Append(controller.stop())
	me.Append(controller.waitServiceStop())
	delete(c.appControllers, peerID)
	return me.Get()
}

func (c *BabuzaCluster) ShutdownPeer(peerID uint64) error {
	controller, ok := c.appControllers[peerID]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerID)
	}
	me := multierror.New()

	if c.useProxyNetwork {
		if err := c.proxyNetwork.DeleteProxy(peerID); err != nil {
			me.Append(err)
		}
	}

	me.Append(controller.stop())
	me.Append(controller.waitServiceStop())
	delete(c.appControllers, peerID)
	return me.Get()
}

func (c *BabuzaCluster) RestartPeer(wait time.Duration, peer Peer, connectedGroup []uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if _, ok := c.appControllers[peer.ID()]; ok {
		return fmt.Errorf("test cluster: found app (id=%d)", peer.ID())
	}

	cfg, storageDir, err := c.genPeerConfig(peer, false)
	app, err := c.createEmbeddedApp(c.peersConfiguration, cfg, true, c.proxyNetwork, storageDir, peer.ApplicationServiceAddresses())
	if err != nil {
		return fmt.Errorf("test cluster: failed to create embedded app. peer(%v) restart(true) join(false). err=%s", peer, err)
	}

	if c.useProxyNetwork {
		proxyPeer, ok := peer.(ProxyPeer)
		if !ok {
			return fmt.Errorf("test cluster: failed to cast peer to ProxyPeer. peer(%v)", peer)
		}
		if err = c.proxyNetwork.AddProxy(proxyPeer.ProxyConfig()); err != nil {
			return err
		}

		if err = c.proxyNetwork.ConnectProxy(peer.ID()); err != nil {
			return err
		}

		if err = c.proxyNetwork.SetPartition(connectedGroup); err != nil {
			return err
		}
	}

	controller := &appController{
		app:                  app,
		appsServiceAddresses: peer.ApplicationServiceAddresses(),
		appStopCh:            make(chan error, 1),
	}
	c.appControllers[peer.ID()] = controller
	if err = <-controller.app.PublishService(ctx); err != nil {
		return err
	}
	go controller.startService()
	return nil
}

func (c *BabuzaCluster) ExecutePeerRaftOperation(peerID uint64, raftOperation func(r *babuza.Raft) error) error {
	controller, ok := c.appControllers[peerID]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerID)
	}
	return raftOperation(controller.app.Raft())
}

func (c *BabuzaCluster) DisconnectPeer(peerID uint64) error {
	if c.useProxyNetwork {
		return c.proxyNetwork.DisconnectProxy(peerID)
	}
	return nil // No operation needed in direct connection mode
}

func (c *BabuzaCluster) ConnectPeer(peerID uint64) error {
	if c.useProxyNetwork {
		return c.proxyNetwork.ConnectProxy(peerID)
	}
	return nil // No operation needed in direct connection mode
}

func (c *BabuzaCluster) SetPartition(peerIDs []uint64) error {
	if c.useProxyNetwork {
		return c.proxyNetwork.SetPartition(peerIDs)
	}
	return nil // No operation needed in direct connection mode
}

func (c *BabuzaCluster) CheckPeerExists(ctx context.Context, ClusterID uint64, peer Peer) error {
	raftListenAddr := peer.RaftListenAddress(c.useProxyNetwork)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):

			controller, ok := c.appControllers[ClusterID]
			if !ok {
				return fmt.Errorf("test cluster: not found leader (id=%d)", ClusterID)
			}
			clusterCfg := controller.app.Raft().ClusterInfo()
			if clusterCfg.LeaderID != ClusterID {
				return fmt.Errorf("test cluster: leader id mismatch. expected=%d, actual=%d", ClusterID, clusterCfg.LeaderID)
			}
			for _, p := range clusterCfg.Peers {
				if p.RaftPeerAttr.PeerID == peer.ID() && p.RaftPeerAttr.IsLearner == peer.IsPeerLearner() &&
					p.RaftPeerAttr.RaftListenAddr == raftListenAddr {
					return nil
				}
			}
		}
	}
}

func (c *BabuzaCluster) CheckOneLeader(wait time.Duration, connectedGroup []uint64) (uint64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()

	for {
		select {
		case <-checkT.C:
			if leaderID, _, ok := c.findConsensusLeader(connectedGroup); ok {
				return leaderID, nil
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
			if c.areStateMachinesInSync(connectedGroup) {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("timeout while checking state machine consistency: %w", ctx.Err())
		}
	}
}

func (c *BabuzaCluster) CheckStatus(wait time.Duration, peerID uint64, matchFunc func(s babuza.Status) bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	checkT := time.NewTicker(c.getCheckTimeout())
	defer checkT.Stop()
	controller, ok := c.appControllers[peerID]
	if !ok {
		return fmt.Errorf("test cluster: not found app (id=%d)", peerID)
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

func (c *BabuzaCluster) genPeerConfig(peer Peer, join bool) (babuza.BabuzaConfig, string, error) {
	cfg := c.config
	cfg.ClusterID = c.clusterID
	cfg.LocalPeerID = peer.ID()
	cfg.RaftListenAddress = peer.RaftListenAddress(false)
	cfg.TLSConfig = peer.RaftTLSConfig()
	cfg.Join = join
	peerDir := filepath.Join(c.storageRootDir, fmt.Sprintf("%d-%d", c.clusterID, peer.ID()))
	return cfg, peerDir, nil
}

func (c *BabuzaCluster) findConsensusLeader(peerIDs []uint64) (uint64, uint64, bool) {
	var leaderID, term uint64

	for _, id := range peerIDs {
		controller, ok := c.appControllers[id]
		if !ok {
			return 0, 0, false
		}

		status := controller.app.Raft().Status()
		if status.LeaderID == babuza.None {
			return 0, 0, false
		}

		if term == 0 {
			term = status.RaftTerm
			leaderID = status.LeaderID
			continue
		}

		if term != status.RaftTerm || leaderID != status.LeaderID {
			return 0, 0, false
		}
	}
	for _, id := range peerIDs {
		if id == leaderID {
			return leaderID, term, true
		}
	}
	return 0, 0, false
}

func (c *BabuzaCluster) hasLeader(peerIDs []uint64) bool {
	for _, id := range peerIDs {
		controller, ok := c.appControllers[id]
		if !ok {
			continue
		}
		status := controller.app.Raft().Status()
		if status.LeaderID != babuza.None {
			for _, id = range peerIDs {
				if id == status.LeaderID {
					return true
				}
			}
		}
	}
	return false
}

func (c *BabuzaCluster) areStateMachinesInSync(peerIDs []uint64) bool {
	hashMap := c.collectStateMachineHashes(peerIDs)
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

func (c *BabuzaCluster) collectStateMachineHashes(peerIDs []uint64) map[uint64]uint32 {
	hashes := make(map[uint64]uint32, len(peerIDs))
	for _, id := range peerIDs {
		controller, ok := c.appControllers[id]
		if !ok {
			continue
		}
		hashes[id] = controller.app.StateMachineHash()
	}
	return hashes
}
