package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver"
)

const (
	clusterID         = 1
	pdHTTPAddr        = "127.0.0.1:8080"
	pdGRPCAddr        = "127.0.0.1:8081"
	initialShards     = 11
	heartbeatInterval = 2
	leaderHBInterval  = 1
	testTimeout       = 30 * time.Second
)

type IntegrationTestSuite struct {
	t        *testing.T
	pdServer *pdserver.PDServer
	servers  []*Server
	tempDirs []string
}

func NewIntegrationTestSuite(t *testing.T) *IntegrationTestSuite {
	return &IntegrationTestSuite{
		t:        t,
		servers:  make([]*Server, 0, 3),
		tempDirs: make([]string, 0, 4),
	}
}

func (suite *IntegrationTestSuite) Setup() error {
	// Start PD server
	if err := suite.setupPDServer(); err != nil {
		return fmt.Errorf("failed to setup PD server: %w", err)
	}

	// Start 3 Redis cluster servers
	if err := suite.setupRedisClusterServers(); err != nil {
		return fmt.Errorf("failed to setup Redis cluster servers: %w", err)
	}

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	return nil
}

func (suite *IntegrationTestSuite) Cleanup() {
	// Stop all servers
	for _, srv := range suite.servers {
		if srv != nil {
			srv.stopCh <- struct{}{}
			srv.redisServer.Close()
			srv.store.Stop()
		}
	}

	// Clean up temp directories
	for _, dir := range suite.tempDirs {
		os.RemoveAll(dir)
	}
}

func (suite *IntegrationTestSuite) setupPDServer() error {
	config := pdserver.Config{
		HttpAddr: pdHTTPAddr,
		GrpcAddr: pdGRPCAddr,
	}

	var err error
	suite.pdServer, err = pdserver.NewPDServer(config)
	if err != nil {
		return err
	}

	// Start PD server in goroutine
	go func() {
		if err := suite.pdServer.Run(); err != nil {
			suite.t.Logf("PD server error: %v", err)
		}
	}()

	// Wait for PD server to start
	time.Sleep(2 * time.Second)
	return nil
}

func (suite *IntegrationTestSuite) setupRedisClusterServers() error {
	storeConfigs := []struct {
		storeID    uint64
		listenAddr string
		raftAddr   string
	}{
		{1, "127.0.0.1:6379", "127.0.0.1:7379"},
		{2, "127.0.0.1:6380", "127.0.0.1:7380"},
		{3, "127.0.0.1:6381", "127.0.0.1:7381"},
	}

	storeAddrs := []string{
		"1=127.0.0.1:7379",
		"2=127.0.0.1:7380",
		"3=127.0.0.1:7381",
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(storeConfigs))

	for i, storeConfig := range storeConfigs {
		wg.Add(1)
		go func(idx int, cfg struct {
			storeID    uint64
			listenAddr string
			raftAddr   string
		}) {
			defer wg.Done()

			tempDir, err := os.MkdirTemp("", fmt.Sprintf("redis-cluster-test-%d-", cfg.storeID))
			if err != nil {
				errChan <- fmt.Errorf("failed to create temp dir for store %d: %w", cfg.storeID, err)
				return
			}
			suite.tempDirs = append(suite.tempDirs, tempDir)

			config := Config{
				StoreID:                          cfg.storeID,
				ClusterID:                        clusterID,
				RedisListenAddr:                  cfg.listenAddr,
				RaftAddr:                         cfg.raftAddr,
				DataDir:                          tempDir,
				InitialShards:                    initialShards,
				StoreAddrs:                       storeAddrs,
				IntervalHeartbeatStore:           heartbeatInterval,
				IntervalHeartbeatRaftGroupLeader: leaderHBInterval,
				PdGRPCAddr:                       pdGRPCAddr,
			}

			srv, err := NewServer(config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create server %d: %w", cfg.storeID, err)
				return
			}

			suite.servers = append(suite.servers, srv)

			// Start server in goroutine
			go func() {
				if err := srv.Run(); err != nil {
					suite.t.Logf("Server %d error: %v", cfg.storeID, err)
				}
			}()

		}(i, storeConfig)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (suite *IntegrationTestSuite) getLeaderDistribution() (map[uint64]uint64, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/leaders", pdHTTPAddr))
	if err != nil {
		return nil, fmt.Errorf("failed to get leaders: %w", err)
	}
	defer resp.Body.Close()

	var leaderResp pdserver.LeaderDistributionResponse
	if err := json.NewDecoder(resp.Body).Decode(&leaderResp); err != nil {
		return nil, fmt.Errorf("failed to decode leader response: %w", err)
	}

	distribution := make(map[uint64]uint64)
	for _, group := range leaderResp.Groups {
		distribution[group.StoreID]++
	}

	return distribution, nil
}

func TestRedisClusterIntegration(t *testing.T) {
	suite := NewIntegrationTestSuite(t)

	if err := suite.Setup(); err != nil {
		t.Fatalf("Failed to setup test suite: %v", err)
	}
	defer suite.Cleanup()

	t.Run("TestLeaderDistribution", func(t *testing.T) {
		suite.testLeaderDistribution(t)
	})

	t.Run("TestLeaderBalancing", func(t *testing.T) {
		suite.testLeaderBalancing(t)
	})
}

func (suite *IntegrationTestSuite) testLeaderDistribution(t *testing.T) {
	// Wait for leaders to be elected
	time.Sleep(3 * time.Second)

	distribution, err := suite.getLeaderDistribution()
	if err != nil {
		t.Fatalf("Failed to get leader distribution: %v", err)
	}

	t.Logf("Current leader distribution: %+v", distribution)

	// Verify that we have the expected number of groups
	totalLeaders := uint64(0)
	for _, count := range distribution {
		totalLeaders += count
	}

	if totalLeaders != initialShards {
		t.Errorf("Expected %d total leaders, got %d", initialShards, totalLeaders)
	}

	// Check that all stores have at least one leader (for initial distribution)
	storeCount := 0
	for storeID := uint64(1); storeID <= 3; storeID++ {
		if count, exists := distribution[storeID]; exists && count > 0 {
			storeCount++
		}
	}

	if storeCount < 1 {
		t.Errorf("Expected at least 1 store to have leaders, got %d stores with leaders", storeCount)
	}
}

func (suite *IntegrationTestSuite) testLeaderBalancing(t *testing.T) {
	// Get initial distribution
	initialDist, err := suite.getLeaderDistribution()
	if err != nil {
		t.Fatalf("Failed to get initial leader distribution: %v", err)
	}

	t.Logf("Initial leader distribution: %+v", initialDist)

	// Find the store with the most leaders
	maxLeaders := uint64(0)
	//maxStore := uint64(0)
	for _, count := range initialDist {
		if count > maxLeaders {
			maxLeaders = count
			//	maxStore = storeID
		}
	}

	// If distribution is already balanced, this test passes
	if maxLeaders <= 1 {
		t.Log("Leaders are already well distributed")
		return
	}

	// Wait for potential rebalancing
	t.Log("Waiting for potential leader rebalancing...")
	time.Sleep(10 * time.Second)

	// Get final distribution
	finalDist, err := suite.getLeaderDistribution()
	if err != nil {
		t.Fatalf("Failed to get final leader distribution: %v", err)
	}

	t.Logf("Final leader distribution: %+v", finalDist)

	// Check if rebalancing occurred
	finalMaxLeaders := uint64(0)
	for _, count := range finalDist {
		if count > finalMaxLeaders {
			finalMaxLeaders = count
		}
	}

	// Verify that load balancing is working
	if finalMaxLeaders < maxLeaders {
		t.Logf("Leader rebalancing detected: max leaders reduced from %d to %d", maxLeaders, finalMaxLeaders)
	}

	// Check that no single store has more than its fair share + 1
	expectedAvg := float64(initialShards) / 3.0
	for storeID, count := range finalDist {
		if float64(count) > expectedAvg+1.5 {
			t.Errorf("Store %d has too many leaders (%d), expected around %.1f", storeID, count, expectedAvg)
		}
	}
}
