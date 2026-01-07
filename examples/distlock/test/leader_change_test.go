package test

import (
	"context"
	"testing"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
)

type LeaderChangeTest struct {
	t *testing.T
}

func (c *LeaderChangeTest) Log(s string) {
	c.t.Log(s)
}

func (c *LeaderChangeTest) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents()
}

func (c *LeaderChangeTest) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	c.t.Logf("Initial leader: %d", leaderID)

	client := embedapp.NewDistLockClient(tc.GetAllAppServiceAddressesFlat())
	defer client.Close()

	ctx := context.Background()

	// Grant lease and acquire lock
	c.t.Log("Granting lease and acquiring lock before leader change...")
	lease1, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)
	c.t.Logf("Lease granted: %d", lease1.LeaseID)

	lockResult, err := client.Acquire(ctx, "leader-test-lock", "owner-1", lease1.LeaseID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	token1 := lockResult.FencingToken
	c.t.Logf("Lock acquired with token: %d", token1)

	// Disconnect leader to force leader change
	c.t.Logf("Disconnecting leader %d to force leader change...", leaderID)
	assert.Nil(c.t, tc.DisconnectPeer(leaderID))

	// Wait for new leader
	time.Sleep(wait * 2)

	// Verify new leader
	newLeaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDsExclude(leaderID))
	assert.Nil(c.t, err)
	assert.NotEqual(c.t, leaderID, newLeaderID)
	c.t.Logf("New leader: %d", newLeaderID)

	// Verify lock state is preserved
	c.t.Log("Verifying lock state is preserved after leader change...")
	status, err := client.GetLockStatus(ctx, "leader-test-lock")
	assert.Nil(c.t, err)
	assert.True(c.t, status.Acquired)
	assert.Equal(c.t, "owner-1", status.OwnerID)
	assert.Equal(c.t, token1, status.FencingToken)
	c.t.Log("Lock state correctly preserved after leader change")

	// Lease keepalive should work on new leader
	c.t.Log("Testing lease keepalive on new leader...")
	keepAliveResult, err := client.LeaseKeepAlive(ctx, lease1.LeaseID)
	assert.Nil(c.t, err)
	assert.Equal(c.t, lease1.LeaseID, keepAliveResult.LeaseID)
	c.t.Log("Lease keepalive successful on new leader")

	// Release lock on new leader
	c.t.Log("Releasing lock on new leader...")
	_, err = client.Release(ctx, "leader-test-lock", "owner-1", token1)
	assert.Nil(c.t, err)

	// Verify lock is released
	status, err = client.GetLockStatus(ctx, "leader-test-lock")
	assert.Nil(c.t, err)
	assert.False(c.t, status.Acquired)
	c.t.Log("Lock successfully released on new leader")

	// Reconnect old leader
	c.t.Logf("Reconnecting old leader %d...", leaderID)
	assert.Nil(c.t, tc.ConnectPeer(leaderID))
	time.Sleep(wait)

	// Verify consistency across all nodes
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
	c.t.Log("All peers are consistent")
}

func TestLeaderChange(t *testing.T) {
	RunTests(&LeaderChangeTest{t: t})
}

type WaitQueueLeaderChangeTest struct {
	t *testing.T
}

func (c *WaitQueueLeaderChangeTest) Log(s string) {
	c.t.Log(s)
}

func (c *WaitQueueLeaderChangeTest) CreateTestComponents() []BabuzaComponent {
	return proxyClusterComponents()
}

func (c *WaitQueueLeaderChangeTest) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingProxyPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))

	leaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)
	c.t.Logf("Initial leader: %d", leaderID)

	client := embedapp.NewDistLockClient(tc.GetAllAppServiceAddressesFlat())
	defer client.Close()

	ctx := context.Background()

	// Owner-1 acquires lock
	c.t.Log("Owner-1 acquiring lock...")
	lease1, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)

	lockResult, err := client.Acquire(ctx, "wait-leader-lock", "owner-1", lease1.LeaseID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	token1 := lockResult.FencingToken
	c.t.Logf("Owner-1 acquired lock with token: %d", token1)

	// Owner-2 grants lease
	c.t.Log("Owner-2 granting lease...")
	lease2, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)

	requestID := "test-request-id-123"

	// Owner-2 starts waiting in goroutine
	waitDone := make(chan *embedapp.WaitResult, 1)
	go func() {
		result, err := client.AcquireWithWait(ctx, "wait-leader-lock", "owner-2", lease2.LeaseID, 30, requestID)
		waitDone <- &embedapp.WaitResult{Result: result, Err: err}
	}()

	// Wait for owner-2 to enter queue
	time.Sleep(time.Second)

	// Force leader change
	c.t.Logf("Disconnecting leader %d to force leader change...", leaderID)
	assert.Nil(c.t, tc.DisconnectPeer(leaderID))

	time.Sleep(wait * 2)

	newLeaderID, err := tc.CheckOneLeader(wait, connectGroup.GetIDsExclude(leaderID))
	assert.Nil(c.t, err)
	c.t.Logf("New leader: %d", newLeaderID)

	// Reconnect old leader
	assert.Nil(c.t, tc.ConnectPeer(leaderID))
	time.Sleep(wait)

	// The original wait request might fail due to leader change, drain the channel
	select {
	case wr := <-waitDone:
		if wr.Err != nil {
			c.t.Logf("Original wait failed as expected during leader change: %v", wr.Err)
		}
	default:
	}

	// Owner-1 releases lock
	c.t.Log("Owner-1 releasing lock after leader change...")
	_, err = client.Release(ctx, "wait-leader-lock", "owner-1", token1)
	assert.Nil(c.t, err)

	// Owner-2 retries wait after leader change (with same requestID for idempotency)
	c.t.Log("Owner-2 retrying wait after leader change...")
	lockResult, err = client.AcquireWithWait(ctx, "wait-leader-lock", "owner-2", lease2.LeaseID, 5, requestID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	assert.True(c.t, lockResult.FencingToken > token1)
	c.t.Logf("Owner-2 acquired lock after retry, token: %d", lockResult.FencingToken)

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
	c.t.Log("Wait queue correctly preserved across leader change")
}

func TestWaitQueueLeaderChange(t *testing.T) {
	RunTests(&WaitQueueLeaderChangeTest{t: t})
}
