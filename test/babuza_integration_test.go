package test

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestBabuza_Single(t *testing.T) {
	dir, err := os.MkdirTemp("", "babuza")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	appConfig := KvStoreEmbeddedAppConfig{}
	appConfig.VotingPeersCfg = babuza.NewVotingPeersConfiguration()
	assert.Nil(t, appConfig.VotingPeersCfg.AddPeer(1, "127.0.0.1:24200"))
	appConfig.ServiceAddress = "127.0.0.1:10000"
	appConfig.BubuzaConfig = babuza.DefaultBabuzaConfig(100, 1, "127.0.0.1:14200")
	babuzaLogger := createDefaultLogger()
	babuzaDirs, err := createDirectories(dir)
	assert.Nil(t, err)
	bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
		appConfig.VotingPeersCfg, proxynetwork.New(), babuzaLogger)
	assert.Nil(t, err)
	app, err := CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
	assert.Nil(t, err)
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(appConfig.BubuzaConfig.RaftConfig.ElectionTicks*appConfig.BubuzaConfig.LogicalTickMs)*time.Millisecond*3)
		defer cancel()

		assert.Nil(t, <-app.PublishService(ctx))
		go app.StartService()
	}()
	time.Sleep(time.Second)
	assert.Equal(t, uint64(1), app.isLeader)
	c, err := createEmbeddedAppClient(map[uint64][]string{
		1: {appConfig.ServiceAddress},
	}, client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	res, err := c.Set(ctx, "foo", "bar")
	assert.Nil(t, err)
	assert.Equal(t, "bar", res.Value)
	res, err = c.Append(ctx, "foo", "bar")
	assert.Nil(t, err)
	assert.Equal(t, "barbar", res.Value)
	res, err = c.Get(ctx, "foo")
	assert.Nil(t, err)
	assert.Equal(t, "barbar", res.Value)
	_, err = c.Delete(ctx, "foo")
	assert.Nil(t, err)
	_, err = c.Get(ctx, "foo")
	assert.ErrorIs(t, err, kverror.ErrKeyNotFound)
	assert.Nil(t, app.Stop())
	s := app.babuza.Status()
	assert.Equal(t, babuza.StopState, s.State)
	assert.Equal(t, babuza.None, s.LeaderId)
}

func TestBabuza_Cluster(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool,
			pn ibabuza.ProxyNetwork, appDir string, appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
	assert.Nil(t, tc.Teardown())
}

func TestBabuza_Cluster_JoinVotingPeer(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 10
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	tp := makeSinglePeer(4, false)
	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	connectGroup.Add(tp.Id)
	assert.Nil(t, tc.JoinPeer(wait, c, tp, connectGroup.GetIds()))

	assert.Nil(t, runFuncWithContextTimeout(time.Second*3, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, tp)
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, leaderId, leaderId2)
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

	// failure
	tp.ProxyListenAddr = "127.0.0.1:100"
	assert.Equal(t, cluster.ErrPeerIDExists, tc.JoinPeer(wait, c, tp, connectGroup.GetIds()))
	leader := makeSinglePeer(leaderId, false)
	tp = makeSinglePeer(5, false)
	tp.ProxyListenAddr = leader.ProxyListenAddr
	assert.Equal(t, cluster.ErrPeerRaftListenAddrExists, tc.JoinPeer(wait, c, tp, connectGroup.GetIds()))
}

func TestBabuza_Cluster_JoinLearner(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))

	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	tp := makeSinglePeer(4, true)
	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	connectGroup.Add(tp.Id)
	assert.Nil(t, tc.JoinPeer(wait, c, tp, connectGroup.GetIds()))
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, tp)
	}))
	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, leaderId, leaderId2)
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

}

func TestBabuza_Cluster_UpdatePeer_RaftListenAddr(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	updateRaftPeer := makeSinglePeer((leaderId%3)+1, false)
	updateRaftPeer.RaftListenAddr = fmt.Sprintf("127.0.0.1:%d", 10000+updateRaftPeer.Id)
	updateRaftPeer.ProxyListenAddr = fmt.Sprintf("127.0.0.1:%d", 14200+updateRaftPeer.Id)
	updateRaftPeer.AppServiceAddresses = []string{fmt.Sprintf("127.0.0.1:%d", 24200+updateRaftPeer.Id)}
	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()

	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return c.Update(ctx, updateRaftPeer.Id, updateRaftPeer.ProxyListenAddr)
	}))

	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, updateRaftPeer)
	}))
	assert.Nil(t, tc.ShutdownPeer(updateRaftPeer.Id))
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		_, err = c.Set(ctx, "foo", "bar")
		return err
	}))
	assert.Nil(t, tc.RestartPeer(wait, updateRaftPeer, connectGroup.GetIds()))
	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, leaderId, leaderId2)
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))

	// failure
	assert.Equal(t, cluster.ErrPeerIDNotFound, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return c.Update(ctx, 100, updateRaftPeer.ProxyListenAddr)
	}))
}

func TestBabuza_Cluster_RemoveFollower(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	followerId := (leaderId % 3) + 1
	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	assert.Nil(t, tc.RemovePeer(wait, c, followerId))
	connectGroup.Remove(followerId)
	assert.Error(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, makeSinglePeer(followerId, false))
	}))

	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, leaderId, leaderId2)

	// failure
	connectGroup.Add(followerId)
	assert.Equal(t, cluster.ErrPeerIDRemoved, tc.JoinPeer(wait, c, makeSinglePeer(followerId, false), connectGroup.GetIds()))
	assert.Equal(t, cluster.ErrPeerIDNotFound, tc.RemovePeer(wait, c, 100))
}

func TestBabuza_Cluster_RemoveLeader(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()

	assert.Nil(t, tc.RemovePeer(wait, c, leaderId))
	connectGroup.Remove(leaderId)
	assert.Error(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, makeSinglePeer(leaderId, false))
	}))
	time.Sleep(tc.RaftElectionTimeout())
	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.NotEqual(t, leaderId, leaderId2)
}

func TestBabuza_Cluster_PromoteLearner(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()

	for i := 0; i < 64; i++ {
		s := strconv.Itoa(i)
		assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
			_, err = c.Set(ctx, s, s)
			return err
		}))
	}
	learner := makeSinglePeer(4, true)
	connectGroup.Add(learner.Id)
	assert.Nil(t, tc.JoinPeer(wait, c, learner, connectGroup.GetIds()))
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, learner)
	}))

	time.Sleep(time.Second) //wait for replication
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return c.PromoteLearner(ctx, 4)
	}))

	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, makeSinglePeer(4, false))
	}))

	// failure
	assert.Equal(t, cluster.ErrPeerIDNotFound, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return c.PromoteLearner(ctx, 100)
	}))
	assert.Equal(t, babuza.ErrVotingMemberCanNotPromote, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return c.PromoteLearner(ctx, leaderId)
	}))

	//TODO: delay sync
	//learner = makeSinglePeer(5, true)
	//assert.Nil(t, tc.JoinPeer(wait, c, learner))
	//assert.Equal(t, babuza.ErrLearnerNotReady, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
	//	return c.PromoteLearner(ctx, 5)
	//}))
}

func TestBabuza_Cluster_TransferLeader(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()

	transferLeaderId := (leaderId % 3) + 1
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), wait*2)
		defer cancel()
		assert.Nil(t, c.TransferLeader(ctx, transferLeaderId))
	}()
	leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, transferLeaderId, leaderId2)

	learner := makeSinglePeer(4, true)
	connectGroup.Add(learner.Id)
	assert.Nil(t, tc.JoinPeer(wait, c, learner, connectGroup.GetIds()))
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, learner)
	}))
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		assert.Equal(t, babuza.ErrLearnerCanNotSwitchLeaderShip, c.TransferLeader(ctx, 4))
	}()
	for i := 0; i < 64; i++ {
		func(v int) {
			ctx, cancel := context.WithTimeout(context.Background(), wait)
			defer cancel()
			s := strconv.Itoa(i)
			_, err = c.Set(ctx, s, s)
			assert.Nil(t, err)
		}(i)
	}

	follower := makeSinglePeer(5, false)
	connectGroup.Add(follower.Id)
	assert.Nil(t, tc.JoinPeer(wait, c, follower, connectGroup.GetIds()))
	assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
		return peerConfigExists(ctx, c, follower)
	}))
	// follower waits for replication then to be leader
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		assert.Nil(t, c.TransferLeader(ctx, 5))
	}()
	leaderId, err = tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, uint64(5), leaderId)
}

func TestBabuza_Cluster_FollowerForwardProposal(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			config.DisableProposalForwarding = false
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	totalPeers := 3
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))

	defer tc.Teardown()
	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	peerId := leaderId
	for i := 0; i < 64; i++ {
		if peerId == leaderId {
			peerId = (leaderId % uint64(totalPeers)) + 1
		}
		assert.NotEqual(t, leaderId, peerId)
		s := strconv.Itoa(i)
		assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
			_, err = c.DirectKvStore(ctx, peerId, client.Set, s, s)
			return err
		}))
		if peerId == uint64(totalPeers) {
			peerId = 1
		} else {
			peerId++
		}
	}
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestBabuza_Cluster_MultiClientProposal(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	clients := 16
	doneCh := make(chan int, clients)

	for i := 0; i < clients; i++ {
		c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
		assert.Nil(t, err)
		go func(id int, cl *client.KvStoreClient) {
			defer cl.Close()
			for index := 0; index < 256; index++ {
				s := strconv.Itoa(i)
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					_, cErr := cl.Set(ctx, s, s)
					return cErr
				}))
			}
			doneCh <- id
		}(i, c)
	}
	doneCount := 0
WaitLoop:
	for {
		select {
		case _ = <-doneCh:
			doneCount++
			if doneCount == clients {
				break WaitLoop
			}
		}
	}
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestBabuza_Cluster_MultiClient_FollowerForwardProposal(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			config.DisableProposalForwarding = false
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)
		})

	wait := tc.RaftElectionTimeout() * 3
	totalPeers := 3
	peers, connectGroup := makeVotingPeers(totalPeers)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()

	leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	clients := 16
	doneCh := make(chan int, clients)
	for i := 0; i < clients; i++ {
		c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
		assert.Nil(t, err)
		go func(cid int, leader uint64, cl *client.KvStoreClient) {
			peerId := leader
			for index := 0; index < 256; index++ {
				if peerId == leader {
					peerId = (leader % uint64(totalPeers)) + 1
				}
				assert.NotEqual(t, leader, peerId)
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					_, cErr := c.DirectKvStore(ctx, peerId, client.Set, fmt.Sprintf("%d-%d", cid, index),
						fmt.Sprintf("%d", index))
					return cErr
				}))
				if peerId == uint64(totalPeers) {
					peerId = 1
				} else {
					peerId++
				}
			}
			doneCh <- cid
		}(i, leaderId, c)
	}
	doneCount := 0
WaitLoop:
	for {
		select {
		case _ = <-doneCh:
			doneCount++
			if doneCount == clients {
				break WaitLoop
			}
		}
	}
	assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestBabuza_Cluster_ClientRegisterSession(t *testing.T) {
	t.Run("LRUSessionManager", func(t *testing.T) {
		rootDir, _ := os.MkdirTemp("", "babuza")
		defer os.RemoveAll(rootDir)
		maxSessions := int64(5)
		tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
			func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
				appServiceAddresses []string) (EmbeddedApp, error) {
				config.DisableProposalForwarding = false
				appConfig := KvStoreEmbeddedAppConfig{
					BubuzaConfig:   config,
					VotingPeersCfg: votingPeersCfg,
					ServiceAddress: appServiceAddresses[0],
				}
				babuzaLogger := createDefaultLogger()
				babuzaDirs, err := createDirectories(appDir)
				assert.Nil(t, err)
				bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
					appConfig.VotingPeersCfg, pn, babuzaLogger)
				assert.Nil(t, err)
				bootstrap.SetSessionManager(session.NewLruManager(babuzaLogger, session.SetLruMgrOptionsWithMaxSessions(maxSessions)))
				return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStoreWithSession(), bootstrap)
			})
		wait := tc.RaftElectionTimeout() * 3
		totalPeers := 3
		peers, connectGroup := makeVotingPeers(totalPeers)
		assert.Nil(t, tc.MakeCluster(wait, peers))
		defer tc.Teardown()

		_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
		assert.Nil(t, err)
		c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
		assert.Nil(t, err)

		for i := int64(0); i < maxSessions; i++ {
			tempC, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
			assert.Nil(t, err)
			assert.Nil(t, tempC.Close())
		}
		assert.ErrorIs(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
			_, cErr := c.Set(ctx, "foo", "bar")
			return cErr
		}), session.ErrSessionExpired)
	})
	t.Run("ExpiredSessionManager", func(t *testing.T) {
		rootDir, _ := os.MkdirTemp("", "babuza")
		defer os.RemoveAll(rootDir)
		sessionExpiredDuration := time.Second
		tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
			func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
				appServiceAddresses []string) (EmbeddedApp, error) {
				config.DisableProposalForwarding = false
				appConfig := KvStoreEmbeddedAppConfig{
					BubuzaConfig:   config,
					VotingPeersCfg: votingPeersCfg,
					ServiceAddress: appServiceAddresses[0],
				}
				babuzaLogger := createDefaultLogger()
				babuzaDirs, err := createDirectories(appDir)
				assert.Nil(t, err)
				bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
					appConfig.VotingPeersCfg, pn, babuzaLogger)
				assert.Nil(t, err)
				bootstrap.SetSessionManager(session.NewExpiredManager(babuzaLogger, session.SetExpiredMgrOptionsWithExpiredNanoseconds(int64(sessionExpiredDuration))))
				return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStoreWithSession(), bootstrap)
			})
		wait := tc.RaftElectionTimeout() * 3
		peers, connectGroup := makeVotingPeers(3)
		assert.Nil(t, tc.MakeCluster(wait, peers))
		defer tc.Teardown()
		_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
		assert.Nil(t, err)

		c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
		assert.Nil(t, err)
		time.Sleep(sessionExpiredDuration)

		assert.ErrorIs(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
			_, cErr := c.Set(ctx, "foo", "bar")
			return cErr
		}), session.ErrSessionExpired)
	})
}

func TestBabuza_Cluster_ClientRequest_Idempotency(t *testing.T) {
	for _, ca := range []struct {
		caseName              string
		stateMachineFactory   func(string) ibabuza.BaseStateMachine
		sessionManagerFactory func(ibabuza.Logger) ibabuza.SessionManager
	}{
		{
			caseName: "[LRU-Session-Manager] memory store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] memory store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName: "[LRU-Session-Manager] memory store with session and concurrent snapshot",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] memory store with session and concurrent snapshot",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName: "[LRU-Session-Manager] disk store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] disk store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
	} {
		t.Log("test case:", ca.caseName)
		func(stateMachine CreateStateMachine, sessionMgr CreateSessionMgr) {
			rootDir, _ := os.MkdirTemp("", "babuza")
			defer os.RemoveAll(rootDir)
			tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
					appServiceAddresses []string) (EmbeddedApp, error) {
					appConfig := KvStoreEmbeddedAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					babuzaLogger := createDefaultLogger()
					babuzaDirs, err := createDirectories(appDir)
					assert.Nil(t, err)
					bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
						appConfig.VotingPeersCfg, pn, babuzaLogger)
					assert.Nil(t, err)
					bootstrap.SetSessionManager(sessionMgr(babuzaLogger))
					return CreateKvEmbeddedApp(appConfig, stateMachine(babuzaDirs.stateMachineDir), bootstrap)
				})
			wait := tc.RaftElectionTimeout() * 3
			peers, connectGroup := makeVotingPeers(3)
			assert.Nil(t, tc.MakeCluster(wait, peers))
			defer tc.Teardown()
			_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)
			ms := client.NewManualIncrementSession()
			c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), ms)
			assert.Nil(t, err)
			defer c.Close()
			ms.SetSequenceNumber(1)
			assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
				res, cErr := c.Set(ctx, "foo", "bar")
				assert.Equal(t, res.Key, "foo")
				assert.Equal(t, res.Value, "bar")
				return cErr
			}))
			ms.SetSequenceNumber(2)
			//repeat SequenceNumber
			for i := 0; i < 5; i++ {
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					res, cErr := c.Append(ctx, "foo", "bar")
					assert.Equal(t, res.Key, "foo")
					assert.Equal(t, res.Value, "barbar")
					return cErr
				}))
			}
		}(ca.stateMachineFactory, ca.sessionManagerFactory)
	}
}

func TestBabuza_Cluster_LeaderShutdown_ClientSessionValid(t *testing.T) {
	for _, ca := range []struct {
		caseName              string
		stateMachineFactory   func(string) ibabuza.BaseStateMachine
		sessionManagerFactory func(ibabuza.Logger) ibabuza.SessionManager
	}{
		{
			caseName: "[LRU-Session-Manager] memory store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] memory store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName: "[LRU-Session-Manager] memory store with session and concurrent snapshot",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] memory store with session and concurrent snapshot",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName: "[LRU-Session-Manager] disk store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName: "[Expired-Session-Manager] disk store with session",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
	} {
		t.Log("test case:", ca.caseName)
		func(stateMachine CreateStateMachine, sessionMgr CreateSessionMgr) {
			rootDir, _ := os.MkdirTemp("", "babuza")
			defer os.RemoveAll(rootDir)
			tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
					appServiceAddresses []string) (EmbeddedApp, error) {
					appConfig := KvStoreEmbeddedAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					babuzaLogger := createDefaultLogger()
					babuzaDirs, err := createDirectories(appDir)
					assert.Nil(t, err)
					bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
						appConfig.VotingPeersCfg, pn, babuzaLogger)
					assert.Nil(t, err)
					bootstrap.SetSessionManager(sessionMgr(babuzaLogger))
					return CreateKvEmbeddedApp(appConfig, stateMachine(babuzaDirs.stateMachineDir), bootstrap)

				})
			wait := tc.RaftElectionTimeout() * 3
			peers, connectGroup := makeVotingPeers(3)
			assert.Nil(t, tc.MakeCluster(wait, peers))
			defer tc.Teardown()
			leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)
			c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewAutoIncrementSession())
			assert.Nil(t, err)
			defer c.Close()

			stopLeaderCh := make(chan struct{})
			doneCh := make(chan struct{})
			count := uint64(36)
			go func(kvCounts uint64) {
				for i := uint64(0); i < kvCounts; i++ {
					assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
						v := fmt.Sprintf("%d", i)
						_, cErr := c.Append(ctx, v, v)
						if cErr != nil {
							t.Log(cErr.Error())
						}
						return cErr
					}))
					if i == kvCounts/2 {
						stopLeaderCh <- struct{}{}
					}
				}
				doneCh <- struct{}{}
			}(count)
			<-stopLeaderCh
			assert.Nil(t, tc.ShutdownPeer(leaderId))
			time.Sleep(wait) //wait for election
			connectGroup.Remove(leaderId)
			leaderId1, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)
			assert.NotEqual(t, leaderId, leaderId1)
			<-doneCh
			for i := 0; i < int(count); i++ {
				v := strconv.Itoa(i)
				res, err := c.Get(context.Background(), v)
				assert.Nil(t, err)
				assert.Equal(t, res.Key, v)
				assert.Equal(t, res.Value, v)
			}
		}(ca.stateMachineFactory, ca.sessionManagerFactory)
	}
}

//func TestBabuza_ApplyStateMachineCommand_ExpiredResponse(t *testing.T) {
//	//TODO:
//}
//

func TestBabuza_RestartNode_RestoreFromSnapshot(t *testing.T) {

	for _, ca := range []struct {
		caseName              string
		supportSession        bool
		stateMachineFactory   func(string) ibabuza.BaseStateMachine
		sessionManagerFactory func(ibabuza.Logger) ibabuza.SessionManager
	}{
		{
			caseName: "memory store",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStore()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewNoOpManager(logger)
			},
		},
		{
			caseName: "disk store",
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewNoOpManager(logger)
			},
		},
		{
			caseName:       "[LRU-Session-Manager] memory store with session",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName:       "[Expired-Session-Manager] memory store with session",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName:       "[LRU-Session-Manager] memory store with session and concurrent snapshot",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName:       "[Expired-Session-Manager] memory store with session and concurrent snapshot",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshotAndSession()
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
		{
			caseName:       "[LRU-Session-Manager] disk store with session",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewLruManager(logger)
			},
		},
		{
			caseName:       "[Expired-Session-Manager] disk store with session",
			supportSession: true,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDiskStoreWithSession(s)
			},
			sessionManagerFactory: func(logger ibabuza.Logger) ibabuza.SessionManager {
				return session.NewExpiredManager(logger)
			},
		},
	} {
		t.Log("test case:", ca.caseName)
		var clientSession client.ISession
		if ca.supportSession {
			clientSession = client.NewAutoIncrementSession()
		} else {
			clientSession = client.NewNoOpSession()
		}
		func(stateMachine CreateStateMachine, sessionMgr CreateSessionMgr, iSession client.ISession) {
			rootDir, _ := os.MkdirTemp("", "babuza")
			defer os.RemoveAll(rootDir)
			snapshotCount := uint64(50)
			tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
					appServiceAddresses []string) (EmbeddedApp, error) {
					config.SnapshotCount = snapshotCount
					appConfig := KvStoreEmbeddedAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					babuzaLogger := createDefaultLogger()
					babuzaDirs, err := createDirectories(appDir)
					assert.Nil(t, err)
					bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
						appConfig.VotingPeersCfg, pn, babuzaLogger)
					assert.Nil(t, err)
					bootstrap.SetSessionManager(sessionMgr(babuzaLogger))
					return CreateKvEmbeddedApp(appConfig, stateMachine(babuzaDirs.stateMachineDir), bootstrap)

				})
			wait := tc.RaftElectionTimeout() * 3
			peers, connectGroup := makeVotingPeers(3)
			assert.Nil(t, tc.MakeCluster(wait, peers))
			defer tc.Teardown()
			leaderId, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)

			c, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), iSession)
			assert.Nil(t, err)
			defer c.Close()

			for i := uint64(0); i < snapshotCount+10; i++ {
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					v := fmt.Sprintf("%d", i)
					res, cErr := c.Set(ctx, v, v)
					assert.Equal(t, v, res.Key)
					assert.Equal(t, v, res.Value)
					return cErr
				}))
			}
			assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
			lastSnapshotIndex := uint64(0)
			lastSnapshotTerm := uint64(0)
			assert.Nil(t, tc.CheckStatus(wait, leaderId, func(s babuza.Status) bool {
				lastSnapshotIndex = s.LastSnapshotIndex
				lastSnapshotTerm = s.LastSnapshotTerm
				return s.LastSnapshotIndex >= snapshotCount &&
					s.LastSnapshotTerm > 0
			}))
			assert.Nil(t, tc.ShutdownPeer(leaderId))
			time.Sleep(wait) // wait for election
			connectGroup.Remove(leaderId)
			leaderId2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)
			for i := uint64(100); i < 108; i++ {
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					v := fmt.Sprintf("%d", i)
					res, cErr := c.Set(ctx, v, v)
					assert.Equal(t, v, res.Key)
					assert.Equal(t, v, res.Value)
					return cErr
				}))
			}
			connectGroup.Add(leaderId)
			assert.Nil(t, tc.RestartPeer(wait, makeSinglePeer(leaderId, false), connectGroup.GetIds()))
			assert.Nil(t, tc.CheckStatus(wait, leaderId, func(s babuza.Status) bool {
				return s.LastSnapshotIndex == lastSnapshotIndex &&
					s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
			}))
			assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
			assert.NotEqual(t, leaderId, leaderId2)

			//shutdown follower
			followerId := leaderId
			assert.Nil(t, tc.ShutdownPeer(followerId))
			for i := uint64(1000); i < 1008; i++ {
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					v := fmt.Sprintf("%d", i)
					res, cErr := c.Set(ctx, v, v)
					assert.Equal(t, v, res.Key)
					assert.Equal(t, v, res.Value)
					return cErr
				}))
			}
			//restart follower
			assert.Nil(t, tc.RestartPeer(wait, makeSinglePeer(followerId, false), connectGroup.GetIds()))
			assert.Nil(t, tc.CheckStatus(wait, followerId, func(s babuza.Status) bool {
				return s.LastSnapshotIndex == lastSnapshotIndex &&
					s.LastSnapshotTerm == lastSnapshotTerm && s.State == babuza.FollowerState
			}))
			assert.Nil(t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
		}(ca.stateMachineFactory, ca.sessionManagerFactory, clientSession)
	}
}

func TestBabuza_Snapshot_ManualTrigger(t *testing.T) {
	for _, c := range []struct {
		caseName            string
		fileTag             string
		stateMachineFactory func(string) ibabuza.BaseStateMachine
	}{
		{
			caseName: "memory store",
			fileTag:  kvstore.MemorySnapshotTag,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStore()
			},
		},
		{
			caseName: " memory store with concurrent snapshot",
			fileTag:  kvstore.BadgerDBSnapshotTag,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewMemoryStoreWithConcurrentSnapshot()
			},
		},
		{
			caseName: "disk store",
			fileTag:  kvstore.BadgerDBSnapshotTag,
			stateMachineFactory: func(s string) ibabuza.BaseStateMachine {
				return kvstore.NewDisk(s)
			},
		},
	} {
		t.Log("test case:", c.caseName)
		func(stateMachine CreateStateMachine) {
			rootDir, _ := os.MkdirTemp("", "babuza")
			defer os.RemoveAll(rootDir)
			tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
				func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
					appServiceAddresses []string) (EmbeddedApp, error) {
					appConfig := KvStoreEmbeddedAppConfig{
						BubuzaConfig:   config,
						VotingPeersCfg: votingPeersCfg,
						ServiceAddress: appServiceAddresses[0],
					}
					babuzaLogger := createDefaultLogger()
					babuzaDirs, err := createDirectories(appDir)
					assert.Nil(t, err)
					bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
						appConfig.VotingPeersCfg, pn, babuzaLogger)
					assert.Nil(t, err)
					return CreateKvEmbeddedApp(appConfig, stateMachine(babuzaDirs.stateMachineDir), bootstrap)

				})
			wait := tc.RaftElectionTimeout() * 3
			peers, connectGroup := makeVotingPeers(3)
			assert.Nil(t, tc.MakeCluster(wait, peers))
			defer tc.Teardown()
			leader, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
			assert.Nil(t, err)

			appClient, err := createEmbeddedAppClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
			assert.Nil(t, err)
			defer appClient.Close()
			for i := 0; i < 8; i++ {
				assert.Nil(t, runFuncWithContextTimeout(wait, func(ctx context.Context) error {
					v := fmt.Sprintf("%d", i)
					res, cErr := appClient.Set(ctx, v, v)
					assert.Equal(t, v, res.Key)
					assert.Equal(t, v, res.Value)
					return cErr
				}))
			}
			ctx, cancel := context.WithTimeout(context.Background(), wait)
			defer cancel()

			assert.Nil(t, tc.ExecutePeerRaftOperation(leader, func(r *babuza.Raft) error {
				result := r.ManualSnapshot(ctx)
				err = result.Wait()
				if err != nil {
					return err
				}
				//verify snapshot
				assert.Equal(t, result.SnapshotMetadata().Snapshot.Metadata.Index, r.Status().LastSnapshotIndex)
				assert.Equal(t, result.SnapshotMetadata().Snapshot.Metadata.Term, r.Status().LastSnapshotTerm)
				tagMap := map[babuzapb.SnapshotFileType]struct{}{
					babuzapb.SnapshotFileType_Cluster:      {},
					babuzapb.SnapshotFileType_StateMachine: {},
					babuzapb.SnapshotFileType_Session:      {},
				}
				for _, tag := range result.SnapshotMetadata().Files {
					delete(tagMap, tag.FileType)
				}
				assert.Equal(t, 0, len(tagMap))
				//check snapshot file content
				snapReader, err := result.SnapshotFileReader()
				assert.Nil(t, err)
				fs, _, err := snapReader.Open(c.fileTag)
				assert.Nil(t, err)
				om, err := newKvOperationOrderMap(fs)
				assert.Nil(t, err)
				for i := 0; i < 8; i++ {
					k := fmt.Sprintf("%d", i)
					v, ok := om.Get(k)
					assert.True(t, ok)
					assert.Equal(t, k, v)
				}
				return nil
			}))
		}(c.stateMachineFactory)
	}
}

//func TestBabuza_SendSnapshotToFollower(t *testing.T) {
//
//	dir, _ := os.MkdirTemp("", "babuzawal")
//	defer os.RemoveAll(dir)
//	pn := NewProxyNetwork()
//	peers, raftConfig := makeTesterClusterConfig(100, 3, pn)
//	tc := CreateTestCluster(dir, raftConfig, getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork)), pn)
//	assert.Nil(t, tc.MakeCluster(peers))
//	assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//	defer tc.Teardown()
//
//	wait := tc.RaftElectionTimeout() * 3
//	replicateStateMachineCommand := func(r *babuzawal, command []byte) error {
//		ctx, cancel := context.WithTimeout(context.Background(), wait)
//		defer cancel()
//		return r.Propose(ctx, ClientSession{}, command).Wait()
//	}
//	leaderId, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//	leader, err := tc.GetRaft(leaderId)
//	assert.Nil(t, err)
//	for i := uint64(0); i < raftConfig.SnapshotCount; i++ {
//		req := request.KvRequest{}
//		command, err := req.Set([]byte(fmt.Sprintf("foo-%d", i)), []byte("foo"))
//		assert.Nil(t, err)
//		assert.Nil(t, replicateStateMachineCommand(leader, command))
//	}
//	assert.Nil(t, tc.CheckStatus(wait, leader, func(s RaftStatus) bool {
//		return s.LastSnapshotIndex >= raftConfig.SnapshotCount &&
//			s.LastSnapshotTerm > 0
//	}))
//
//	newFollower := uint64(4)
//	tp := pn.GenClusterVotingPeer(newFollower)
//	ctx, cancel := context.WithTimeout(context.Background(), wait)
//	defer cancel()
//	assert.Nil(t, tc.JoinPeer(ctx, leader, ClientSession{}, tp))
//	leaderId2, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//	assert.Equal(t, leaderId, leaderId2)
//	assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait))
//	nf, err := tc.GetRaft(newFollower)
//	assert.Nil(t, err)
//	assert.Nil(t, tc.CheckStatus(wait, nf, func(s RaftStatus) bool {
//		status := leader.Status()
//		return s.LastSnapshotIndex == status.LastSnapshotIndex &&
//			s.LastSnapshotTerm == status.LastSnapshotTerm
//	}))
//}
//
//func TestBabuza_LinearizableRead(t *testing.T) {
//	dir, _ := os.MkdirTemp("", "babuzawal")
//	defer os.RemoveAll(dir)
//	pn := NewProxyNetwork()
//	peers, raftConfig := makeTesterClusterConfig(100, 3, pn)
//	tc := CreateTestCluster(dir, raftConfig, getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork)), pn)
//	assert.Nil(t, tc.MakeCluster(peers))
//	assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//	defer tc.Teardown()
//
//	wait := tc.RaftElectionTimeout() * 3
//	replicateStateMachineCommand := func(r *babuzawal, command []byte) error {
//		ctx, cancel := context.WithTimeout(context.Background(), wait)
//		defer cancel()
//		return r.Propose(ctx, ClientSession{}, command).Wait()
//	}
//	leaderId, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//	leader, err := tc.GetRaft(leaderId)
//	assert.Nil(t, err)
//	for i := uint64(0); i < raftConfig.SnapshotCount; i++ {
//		req := request.KvRequest{}
//		command, err := req.Set([]byte(fmt.Sprintf("foo-%d", i)), []byte(fmt.Sprintf("bar-%d", i)))
//		assert.Nil(t, err)
//		assert.Nil(t, replicateStateMachineCommand(leader, command))
//	}
//
//	newFollower := uint64(4)
//	tp := pn.GenClusterVotingPeer(newFollower)
//	ctx, cancel := context.WithTimeout(context.Background(), wait)
//	defer cancel()
//	assert.Nil(t, tc.JoinPeer(ctx, leader, ClientSession{}, tp))
//	time.Sleep(tc.RaftElectionTimeout()) //waiting for election
//	nf, err := tc.GetRaft(newFollower)
//	assert.Nil(t, err)
//	assert.Nil(t, nf.LinearizableRead(context.Background()))
//	lastIndex := raftConfig.SnapshotCount - 1
//	result, exist := nf.adaptor.GetStateMachine().(*stateMachine.MemoryStore).Load([]byte(fmt.Sprintf("foo-%d", lastIndex)))
//	assert.Equal(t, true, exist)
//	assert.Equal(t, []byte(fmt.Sprintf("bar-%d", lastIndex)), result)
//}
//
//func TestBabuza_Linearizability(t *testing.T) {
//}
//

func TestBabuza_ReElection(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)

		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leader1, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	assert.Nil(t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader1)
	time.Sleep(wait) //wait for election
	leader2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Nil(t, tc.ConnectPeer(leader1))
	connectGroup.Add(leader1)
	assert.NotEqual(t, leader2, leader1)

	//lose quorum
	assert.Nil(t, tc.DisconnectPeer(leader2))
	assert.Nil(t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader1)
	connectGroup.Remove(leader2)
	assert.Nil(t, tc.CheckNoLeader(wait, connectGroup.GetIds()))
	assert.Nil(t, tc.ConnectPeer(leader2))
	connectGroup.Add(leader2)
	time.Sleep(wait) //wait for election
	leader3, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Nil(t, tc.ConnectPeer(leader1))
	connectGroup.Add(leader1)
	lastLeader, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.Equal(t, leader3, lastLeader)
}

func TestBabuza_ReElection_LeaderRestart(t *testing.T) {
	rootDir, _ := os.MkdirTemp("", "babuza")
	defer os.RemoveAll(rootDir)
	tc := CreateTestCluster(100, rootDir, proxynetwork.New(),
		func(votingPeersCfg *babuza.VotingPeersConfiguration, config babuza.BabuzaConfig, restart bool, pn ibabuza.ProxyNetwork, appDir string,
			appServiceAddresses []string) (EmbeddedApp, error) {
			appConfig := KvStoreEmbeddedAppConfig{
				BubuzaConfig:   config,
				VotingPeersCfg: votingPeersCfg,
				ServiceAddress: appServiceAddresses[0],
			}
			babuzaLogger := createDefaultLogger()
			babuzaDirs, err := createDirectories(appDir)
			assert.Nil(t, err)
			bootstrap, err := defaultBootstrapBuilder(&appConfig.BubuzaConfig, babuzaDirs,
				appConfig.VotingPeersCfg, pn, babuzaLogger)
			assert.Nil(t, err)
			return CreateKvEmbeddedApp(appConfig, kvstore.NewMemoryStore(), bootstrap)

		})
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingPeers(3)
	assert.Nil(t, tc.MakeCluster(wait, peers))
	defer tc.Teardown()
	leader1, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)

	//restart leader 1
	assert.Nil(t, tc.ShutdownPeer(leader1))
	time.Sleep(wait) //leader change
	assert.Nil(t, tc.RestartPeer(wait, makeSinglePeer(leader1, false), connectGroup.GetIds()))
	leader2, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
	assert.NotEqual(t, leader2, leader1)
	//restart leader 2
	assert.Nil(t, tc.ShutdownPeer(leader2))
	assert.Nil(t, tc.DisconnectPeer(leader1))
	connectGroup.Remove(leader2)
	connectGroup.Remove(leader1)
	assert.Nil(t, tc.CheckNoLeader(wait, connectGroup.GetIds()))
	//rejoin leader 2,leader 1
	connectGroup.Add(leader2)
	connectGroup.Add(leader1)
	assert.Nil(t, tc.ConnectPeer(leader1))
	assert.Nil(t, tc.RestartPeer(wait, makeSinglePeer(leader2, false), connectGroup.GetIds()))
	_, err = tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(t, err)
}

//func TestBabuza_OneFollowerDisconnection(t *testing.T) {
//	dir, _ := os.MkdirTemp("", "babuzawal")
//	defer os.RemoveAll(dir)
//	pn := NewProxyNetwork()
//	peers, raftConfig := makeTesterClusterConfig(100, 3, pn)
//	tc := CreateTestCluster(dir, raftConfig, getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork)), pn)
//	assert.Nil(t, tc.MakeCluster(peers))
//	assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//	defer tc.Teardown()
//
//	wait := tc.RaftElectionTimeout() * 3
//	leaderId1, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//	// disconnect follower
//	followerId := (leaderId1 % 3) + 1
//	assert.Nil(t, tc.DisconnectPeer(followerId))
//
//	replicateStateMachineCommand := func(r *babuzawal, command []byte) error {
//		ctx, cancel := context.WithTimeout(context.Background(), wait)
//		defer cancel()
//		return r.Propose(ctx, ClientSession{}, command).Wait()
//	}
//	leader1Raft, err := tc.GetRaft(leaderId1)
//	for i := 0; i < 8; i++ {
//		var req request.KvRequest
//		command, err := req.Set([]byte(fmt.Sprintf("foo-%d", i)), []byte("foo"))
//		assert.Nil(t, err)
//		assert.Nil(t, replicateStateMachineCommand(leader1Raft, command))
//	}
//	// re-connect follower
//	assert.Nil(t, tc.ConnectPeer(followerId))
//	lastLeaderId, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//	assert.Equal(t, leaderId1, lastLeaderId)
//	for i := 8; i < 16; i++ {
//		var req request.KvRequest
//		command, err := req.Set([]byte(fmt.Sprintf("foo-%d", i)), []byte("foo"))
//		assert.Nil(t, err)
//		assert.Nil(t, replicateStateMachineCommand(leader1Raft, command))
//	}
//	assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait))
//}
//
//func TestBabuza_QuorumFollowerDisconnection(t *testing.T) {
//	for _, testCase := range []struct {
//		checkQuorum bool
//		preVote     bool
//	}{
//		{
//			checkQuorum: true,
//			preVote:     true,
//		},
//		{
//			checkQuorum: false,
//			preVote:     false,
//		},
//	} {
//		func(checkQuorum, preVote bool) {
//			t.Logf("test condition: raft checkQuorum=%v preVote=%v", checkQuorum, preVote)
//			dir, _ := os.MkdirTemp("", "babuzawal")
//			defer os.RemoveAll(dir)
//			pn := NewProxyNetwork()
//			peers, raftConfig := makeTesterClusterConfig(100, 5, pn)
//			raftConfig.CheckQuorum = checkQuorum
//			raftConfig.PreVote = preVote
//			tc := CreateTestCluster(dir, raftConfig, getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork)), pn)
//			assert.Nil(t, tc.MakeCluster(peers))
//			assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//			defer tc.Teardown()
//
//			wait := tc.RaftElectionTimeout() * 3
//			leaderId1, err := tc.CheckOneLeader(wait)
//			assert.Nil(t, err)
//			leader1Raft, err := tc.GetRaft(leaderId1)
//			assert.Nil(t, err)
//			// disconnect follower
//			follower1Id := (leaderId1 % 5) + 1
//			follower2Id := (follower1Id % 5) + 1
//			follower3Id := (follower2Id % 5) + 1
//			assert.Nil(t, tc.DisconnectPeer(follower1Id))
//			assert.Nil(t, tc.DisconnectPeer(follower2Id))
//			assert.Nil(t, tc.DisconnectPeer(follower3Id))
//			replicateStateMachineCommand := func(r *babuzawal, command []byte) error {
//				ctx, cancel := context.WithTimeout(context.Background(), wait)
//				defer cancel()
//				return r.Propose(ctx, ClientSession{}, command).Wait()
//			}
//			var req request.KvRequest
//			command, err := req.Set([]byte("foo"), []byte("foo"))
//			assert.Nil(t, err)
//			assert.Error(t, replicateStateMachineCommand(leader1Raft, command))
//
//			// re-connect follower
//			assert.Nil(t, tc.ConnectPeer(follower1Id))
//			assert.Nil(t, tc.ConnectPeer(follower2Id))
//			assert.Nil(t, tc.ConnectPeer(follower3Id))
//
//			lastLeaderId, err := tc.CheckOneLeader(wait)
//			assert.Nil(t, err)
//			lastLeaderRaft, err := tc.GetRaft(lastLeaderId)
//			assert.Nil(t, err)
//			command, err = req.Set([]byte("bar"), []byte("bar"))
//			assert.Nil(t, err)
//			assert.Nil(t, replicateStateMachineCommand(lastLeaderRaft, command))
//			command, err = req.Set([]byte("foobar"), []byte("bar"))
//			assert.Nil(t, err)
//			assert.Nil(t, replicateStateMachineCommand(lastLeaderRaft, command))
//			assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait))
//		}(testCase.checkQuorum, testCase.preVote)
//	}
//}
//
//func TestBabuza_Two_Partition(t *testing.T) {
//	for _, testCase := range []struct {
//		checkQuorum bool
//		preVote     bool
//	}{
//		{
//			checkQuorum: true,
//			preVote:     true,
//		},
//		{
//			checkQuorum: false,
//			preVote:     false,
//		},
//	} {
//		func(checkQuorum, preVote bool) {
//			t.Logf("test condition: raft checkQuorum=%v preVote=%v", checkQuorum, preVote)
//			dir, _ := os.MkdirTemp("", "babuzawal")
//			defer os.RemoveAll(dir)
//			pn := NewProxyNetwork()
//			peers, raftConfig := makeTesterClusterConfig(100, 5, pn)
//			raftConfig.CheckQuorum = checkQuorum
//			raftConfig.PreVote = preVote
//			tc := CreateTestCluster(dir, raftConfig, getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork)), pn)
//			assert.Nil(t, tc.MakeCluster(peers))
//			partition1 := []uint64{1, 2, 3}
//			partition2 := []uint64{4, 5}
//			assert.Nil(t, tc.ConnectPeers(partition1))
//			assert.Nil(t, tc.ConnectPeers(partition2))
//			defer tc.Teardown()
//
//			wait := tc.RaftElectionTimeout() * 3
//			partition1LeaderId, err := tc.CheckOneLeader(wait, partition1...)
//			assert.Nil(t, err)
//			leader1Raft, err := tc.GetRaft(partition1LeaderId)
//			assert.Nil(t, err)
//			findLeader := false
//			for _, id := range partition1 {
//				if id == partition1LeaderId {
//					findLeader = true
//					break
//				}
//			}
//			assert.Equal(t, true, findLeader)
//			assert.Nil(t, tc.CheckNoLeader(wait, partition2...))
//			replicateStateMachineCommand := func(r *babuzawal, command []byte) error {
//				ctx, cancel := context.WithTimeout(context.Background(), wait)
//				defer cancel()
//				return r.Propose(ctx, ClientSession{}, command).Wait()
//			}
//
//			for i := 0; i < 8; i++ {
//				var req request.KvRequest
//				command, err := req.Set([]byte(fmt.Sprintf("foo-%d", i)), []byte("foo"))
//				assert.Nil(t, err)
//				assert.Nil(t, replicateStateMachineCommand(leader1Raft, command))
//			}
//			time.Sleep(time.Second) // wait for replication
//			assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait, partition1...))
//
//			//connect all
//			assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//			time.Sleep(time.Second) // wait for replication
//			assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait))
//			lastLeader, err := tc.CheckOneLeader(wait)
//			assert.Nil(t, err)
//			leaderInPartition1 := false
//			for _, id := range partition1 {
//				if id == lastLeader {
//					leaderInPartition1 = true
//					break
//				}
//			}
//			assert.Equal(t, true, leaderInPartition1)
//
//			// re-partition
//			partition1 = []uint64{1, 5}
//			partition2 = []uint64{2, 3, 4}
//			assert.Nil(t, tc.ConnectPeers(partition1))
//			assert.Nil(t, tc.ConnectPeers(partition2))
//			partition2LeaderId, err := tc.CheckOneLeader(wait, partition2...)
//			assert.Nil(t, err)
//			findLeader = false
//			for _, id := range partition2 {
//				if id == partition2LeaderId {
//					findLeader = true
//					break
//				}
//			}
//			assert.Equal(t, true, findLeader)
//			//connect all
//			assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//			assert.Nil(t, tc.CheckPeersStateMachineConsistency(wait))
//		}(testCase.checkQuorum, testCase.preVote)
//	}
//}
//
//func TestBabuza_IntegrateLinearizabilityWithKvStore(t *testing.T) {
//	IntegrateLinearizabilityWithKvStore(t, 3, 5)
//}
//
//func IntegrateLinearizabilityWithKvStore(t *testing.T, clients int, totalKvStores int) {
//	dir, _ := os.MkdirTemp("", "babuzawal")
//	defer os.RemoveAll(dir)
//	pn := NewProxyNetwork()
//	peers, raftConfig := makeTesterClusterConfig(100, totalKvStores, pn)
//	raftConfig.SnapshotCount = 1024
//	f := getTestBabuzaComponent(t, pn.ProxyNetwork.(ibabuza.TcpNetwork))
//	f.CreateStateMachine = func(dataDir string) ibabuza.BaseStateMachine {
//		return stateMachine.NewMemoryExactlyOnce()
//	}
//	maxSessions := int64(clients)
//	f.CreateSessionManager = func(logger ibabuza.Logger) ibabuza.SessionManager {
//		return NewLRUSessionManager(logger, LRUSessionMangerOptionsWithSessionSize(maxSessions))
//	}
//	tc := CreateTestCluster(dir, raftConfig, f, pn)
//	assert.Nil(t, tc.MakeCluster(peers))
//	assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//	defer tc.Teardown()
//	wait := tc.RaftElectionTimeout() * 3
//	_, err := tc.CheckOneLeader(wait)
//	assert.Nil(t, err)
//
//	var operations []porcupine.Operation
//	var opMu sync.Mutex
//
//	for i := 0; i < 3; i++ {
//		clientStopCh := make(chan struct{})
//		partitionStopCh := make(chan struct{})
//		wg := sync.WaitGroup{}
//		for c := 0; c < clients; c++ {
//			wg.Add(1)
//			go func(clientId int, clusterPeers []*babuzawal) {
//				defer wg.Done()
//				mKvClient := NewMockKvClient(clusterPeers)
//				it := 0
//				for {
//					select {
//					case <-clientStopCh:
//						return
//					default:
//					}
//					var input KvStoreInput
//					var output KvStoreOutput
//					key := strconv.Itoa(rand.Int() % clients)
//					value := fmt.Sprintf("c:%d v:%d ", clientId, it)
//					start := time.Now().UnixNano()
//					if (rand.Int() % 1000) < 200 {
//						mKvClient.Set([]byte(key), []byte(value))
//						input = KvStoreInput{Command: 1, Key: key, Value: value}
//					} else if (rand.Int() % 1000) < 400 {
//						mKvClient.Append([]byte(key), []byte(value))
//						input = KvStoreInput{Command: 2, Key: key, Value: value}
//					} else {
//						v := mKvClient.Get([]byte(key))
//						input = KvStoreInput{Command: 0, Key: key}
//						output = KvStoreOutput{Value: string(v)}
//					}
//					end := time.Now().UnixNano()
//					op := porcupine.Operation{Input: input, Call: start, Output: output, Return: end, ClientId: clientId}
//					opMu.Lock()
//					operations = append(operations, op)
//					opMu.Unlock()
//					it++
//				}
//			}(c+1, tc.GetAllRaft())
//		}
//		partitionDoneCh := make(chan struct{})
//		go func() {
//			time.Sleep(time.Second)
//			for {
//				select {
//				case <-partitionStopCh:
//					close(partitionDoneCh)
//					return
//				default:
//				}
//				partitions := make(map[uint64]int)
//				for _, id := range tc.GetAllPeerIds() {
//					partitions[id] = rand.Int() % 2
//				}
//				connect1Ids := make([]uint64, 0, totalKvStores)
//				connect2Ids := make([]uint64, 0, totalKvStores)
//				for k, v := range partitions {
//					if v == 0 {
//						connect1Ids = append(connect1Ids, k)
//					} else {
//						connect2Ids = append(connect2Ids, k)
//					}
//				}
//				assert.Nil(t, tc.ConnectPeers(connect1Ids))
//				assert.Nil(t, tc.ConnectPeers(connect2Ids))
//				time.Sleep(wait)
//			}
//		}()
//		time.Sleep(9 * time.Second)
//		close(partitionStopCh)
//		<-partitionDoneCh
//		assert.Nil(t, tc.ConnectPeers(tc.GetAllPeerIds()))
//		time.Sleep(wait)                 // wait for election
//		_, err = tc.CheckOneLeader(wait) // new leader
//		assert.Nil(t, err)
//		close(clientStopCh)
//		wg.Wait()
//	}
//	res, info := porcupine.CheckOperationsVerbose(KvStoreModel, operations, time.Second*3)
//	if res == porcupine.Illegal {
//		file, err := ioutil.TempFile("", "*.html")
//		if err != nil {
//			t.Logf("info: failed to create temp file for visualization\n")
//		} else {
//			err = porcupine.Visualize(KvStoreModel, info, file)
//			if err != nil {
//				t.Logf("info: failed to write history visualization to %s\n", file.Name())
//			} else {
//				t.Logf("info: wrote history visualization to %s\n", file.Name())
//			}
//		}
//		t.Fatal("history is not linearizable")
//	} else if res == porcupine.Unknown {
//		t.Logf("info: linearizability check timed out, assuming history is ok\n")
//	}
//}
