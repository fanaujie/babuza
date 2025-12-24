// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcase

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/networkio/proxynetwork"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
)

// FaultInjectionCluster tests that the cluster remains operational
// under network fault conditions (delay, packet loss, reorder) and recovers
// when faults are cleared. Tests each fault type separately and combined.
type FaultInjectionCluster struct {
	t *testing.T
}

func (c *FaultInjectionCluster) Log(s string) {
	c.t.Log(s)
}

func (c *FaultInjectionCluster) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents(true, true)
}

func (c *FaultInjectionCluster) Run(tc *testcluster.BabuzaCluster, a any) {
	// Set wait time to 3 times the Raft election timeout
	wait := tc.RaftElectionTimeout() * 3

	// Create 3 voting proxy peers
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	// Check initial leader election
	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	c.t.Logf("Initial leader: %d", leaderID)

	// Create a client to the cluster
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()

	// Verify initial operation without fault
	assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
		_, err := kvClient.Set(ctx, "init-key", "init-value")
		return err
	}))

	keyIndex := 0
	executeCommands := func(count int, timeout time.Duration) {
		for i := 0; i < count; i++ {
			keyIndex++
			assert.Nil(c.t, runWithCtxTimeout(timeout, func(ctx context.Context) error {
				key := fmt.Sprintf("test-key-%d", keyIndex)
				_, err := kvClient.Set(ctx, key, fmt.Sprintf("value-%d", keyIndex))
				return err
			}))
		}
	}

	// Test 1: Delay fault on peer 1
	c.t.Log("=== Test 1: Delay fault on peer 1 ===")
	assert.Nil(c.t, tc.SetPeerFault(1, proxynetwork.FaultConfig{
		DelayMin: 30 * time.Millisecond,
		DelayMax: 50 * time.Millisecond,
	}))
	executeCommands(3, wait*2)
	assert.Nil(c.t, tc.ClearPeerFault(1))
	c.t.Log("Delay fault test passed")

	// Test 2: Packet loss fault on peer 2
	c.t.Log("=== Test 2: Packet loss fault on peer 2 ===")
	assert.Nil(c.t, tc.SetPeerFault(2, proxynetwork.FaultConfig{
		LossRate: 0.3, // 30% packet loss
	}))
	executeCommands(3, wait*2)
	assert.Nil(c.t, tc.ClearPeerFault(2))
	c.t.Log("Packet loss fault test passed")

	// Test 3: Reorder fault on peer 3
	c.t.Log("=== Test 3: Reorder fault on peer 3 ===")
	assert.Nil(c.t, tc.SetPeerFault(3, proxynetwork.FaultConfig{
		ReorderBufferSize:    5,
		ReorderFlushInterval: 100 * time.Millisecond,
	}))
	executeCommands(3, wait*2)
	assert.Nil(c.t, tc.ClearPeerFault(3))
	c.t.Log("Reorder fault test passed")

	// Test 4: Combined faults on all peers
	c.t.Log("=== Test 4: Combined faults on all peers ===")
	combinedConfig := proxynetwork.FaultConfig{
		LossRate:             0.5,
		DelayMin:             100 * time.Millisecond,
		DelayMax:             400 * time.Millisecond,
		ReorderBufferSize:    3,
		ReorderFlushInterval: 100 * time.Millisecond,
	}
	assert.Nil(c.t, tc.SetPeerFault(1, combinedConfig))
	assert.Nil(c.t, tc.SetPeerFault(2, combinedConfig))
	assert.Nil(c.t, tc.SetPeerFault(3, combinedConfig))
	executeCommands(5, wait*3)
	assert.Nil(c.t, tc.ClearPeerFault(1))
	assert.Nil(c.t, tc.ClearPeerFault(2))
	assert.Nil(c.t, tc.ClearPeerFault(3))
	c.t.Log("Combined faults test passed")

	// Verify cluster still has a leader
	_, err = tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	// Execute commands after clearing all faults
	executeCommands(3, wait)

	// Verify cluster consistency
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
	c.t.Log("Cluster consistency verified after all fault injection tests")
}

func TestFaultInjection(t *testing.T) {
	RunTests(&FaultInjectionCluster{t: t})
}
