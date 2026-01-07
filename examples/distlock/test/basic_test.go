package test

import (
	"context"
	"testing"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type BasicDistLockTest struct {
	t *testing.T
}

func (c *BasicDistLockTest) Log(s string) {
	c.t.Log(s)
}

func (c *BasicDistLockTest) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *BasicDistLockTest) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	client := embedapp.NewDistLockClient(tc.GetAllAppServiceAddressesFlat())
	defer client.Close()

	ctx := context.Background()

	// Test 1: Lease Grant
	c.t.Log("Test 1: Lease Grant")
	leaseResult, err := client.LeaseGrant(ctx, 30)
	assert.Nil(c.t, err)
	assert.True(c.t, leaseResult.LeaseID > 0)
	assert.Equal(c.t, int64(30), leaseResult.TTL)
	leaseID1 := leaseResult.LeaseID
	c.t.Logf("Lease granted: %d", leaseID1)

	// Test 2: Acquire Lock
	c.t.Log("Test 2: Acquire Lock")
	lockResult, err := client.Acquire(ctx, "test-lock", "owner-1", leaseID1)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	assert.True(c.t, lockResult.FencingToken > 0)
	token1 := lockResult.FencingToken
	c.t.Logf("Lock acquired with token: %d", token1)

	// Test 3: Lock Status
	c.t.Log("Test 3: Lock Status")
	status, err := client.GetLockStatus(ctx, "test-lock")
	assert.Nil(c.t, err)
	assert.True(c.t, status.Acquired)
	assert.Equal(c.t, "owner-1", status.OwnerID)
	assert.Equal(c.t, token1, status.FencingToken)

	// Test 4: Acquire Same Lock by Different Owner (should fail)
	c.t.Log("Test 4: Acquire Same Lock by Different Owner")
	lease2, err := client.LeaseGrant(ctx, 30)
	assert.Nil(c.t, err)
	lockResult, err = client.Acquire(ctx, "test-lock", "owner-2", lease2.LeaseID)
	assert.Nil(c.t, err)
	assert.False(c.t, lockResult.Acquired)
	c.t.Log("Lock acquisition by owner-2 correctly rejected")

	// Test 5: Reentrant Lock (same owner re-acquire)
	c.t.Log("Test 5: Reentrant Lock")
	lockResult, err = client.Acquire(ctx, "test-lock", "owner-1", leaseID1)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	assert.True(c.t, lockResult.FencingToken > token1) // New token
	token2 := lockResult.FencingToken
	c.t.Logf("Reentrant lock acquired with new token: %d", token2)

	// Test 6: Release Lock
	c.t.Log("Test 6: Release Lock")
	_, err = client.Release(ctx, "test-lock", "owner-1", token2)
	assert.Nil(c.t, err)
	c.t.Log("Lock released")

	// Verify lock is released
	status, err = client.GetLockStatus(ctx, "test-lock")
	assert.Nil(c.t, err)
	assert.False(c.t, status.Acquired)

	// Test 7: Lease KeepAlive
	c.t.Log("Test 7: Lease KeepAlive")
	keepAliveResult, err := client.LeaseKeepAlive(ctx, leaseID1)
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaseID1, keepAliveResult.LeaseID)
	c.t.Log("Lease keepalive successful")

	// Test 8: Lease Revoke
	c.t.Log("Test 8: Lease Revoke")
	// First acquire a lock
	lockResult, err = client.Acquire(ctx, "revoke-test-lock", "owner-1", leaseID1)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)

	// Revoke lease - should release the lock
	revokeResult, err := client.LeaseRevoke(ctx, leaseID1)
	assert.Nil(c.t, err)
	assert.Equal(c.t, leaseID1, revokeResult.LeaseID)
	c.t.Logf("Lease revoked, released locks: %v", revokeResult.ReleasedLocks)

	// Verify lock is released
	status, err = client.GetLockStatus(ctx, "revoke-test-lock")
	assert.Nil(c.t, err)
	assert.False(c.t, status.Acquired)

	// Test 9: Multiple fencing tokens are monotonically increasing
	c.t.Log("Test 9: Fencing Token Monotonicity")
	lease3, err := client.LeaseGrant(ctx, 30)
	assert.Nil(c.t, err)

	var tokens []uint64
	for i := 0; i < 5; i++ {
		lockResult, err = client.Acquire(ctx, "mono-lock", "owner-1", lease3.LeaseID)
		assert.Nil(c.t, err)
		assert.True(c.t, lockResult.Acquired)
		tokens = append(tokens, lockResult.FencingToken)
		_, _ = client.Release(ctx, "mono-lock", "owner-1", lockResult.FencingToken)
	}

	for i := 1; i < len(tokens); i++ {
		assert.True(c.t, tokens[i] > tokens[i-1], "Token should be monotonically increasing")
	}
	c.t.Logf("Fencing tokens: %v", tokens)

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestBasicDistLock(t *testing.T) {
	RunTests(&BasicDistLockTest{t: t})
}

type WaitQueueTest struct {
	t *testing.T
}

func (c *WaitQueueTest) Log(s string) {
	c.t.Log(s)
}

func (c *WaitQueueTest) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *WaitQueueTest) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	client := embedapp.NewDistLockClient(tc.GetAllAppServiceAddressesFlat())
	defer client.Close()

	ctx := context.Background()

	// Test Wait Queue
	c.t.Log("Test: Wait Queue - owner-1 holds lock, owner-2 waits")

	// Owner-1 grants lease and acquires lock
	lease1, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)

	lockResult, err := client.Acquire(ctx, "wait-lock", "owner-1", lease1.LeaseID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	token1 := lockResult.FencingToken
	c.t.Logf("Owner-1 acquired lock with token: %d", token1)

	// Owner-2 grants lease and waits for lock
	lease2, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)

	requestID := uuid.New().String()

	// Start waiting in goroutine
	waitDone := make(chan *embedapp.WaitResult, 1)
	go func() {
		result, err := client.AcquireWithWait(ctx, "wait-lock", "owner-2", lease2.LeaseID, 10, requestID)
		waitDone <- &embedapp.WaitResult{Result: result, Err: err}
	}()

	// Give time for owner-2 to enter wait queue
	time.Sleep(500 * time.Millisecond)

	// Owner-1 releases lock
	c.t.Log("Owner-1 releasing lock...")
	_, err = client.Release(ctx, "wait-lock", "owner-1", token1)
	assert.Nil(c.t, err)

	// Owner-2 should acquire lock
	select {
	case wr := <-waitDone:
		assert.Nil(c.t, wr.Err)
		assert.True(c.t, wr.Result.Acquired)
		assert.True(c.t, wr.Result.FencingToken > token1)
		c.t.Logf("Owner-2 acquired lock with token: %d", wr.Result.FencingToken)
	case <-time.After(5 * time.Second):
		c.t.Fatal("Owner-2 wait timed out")
	}

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestWaitQueue(t *testing.T) {
	RunTests(&WaitQueueTest{t: t})
}

type LeaseExpirationTest struct {
	t *testing.T
}

func (c *LeaseExpirationTest) Log(s string) {
	c.t.Log(s)
}

func (c *LeaseExpirationTest) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents()
}

func (c *LeaseExpirationTest) Run(tc *testcluster.BabuzaCluster, a any) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIDs())
	assert.Nil(c.t, err)

	client := embedapp.NewDistLockClient(tc.GetAllAppServiceAddressesFlat())
	defer client.Close()

	ctx := context.Background()

	// Test Lease Expiration
	c.t.Log("Test: Lease Expiration - lock should be released when lease expires")

	// Grant a short lease (3 seconds)
	lease1, err := client.LeaseGrant(ctx, 3)
	assert.Nil(c.t, err)
	c.t.Logf("Granted short lease: %d (TTL=3s)", lease1.LeaseID)

	// Acquire lock
	lockResult, err := client.Acquire(ctx, "expire-lock", "owner-1", lease1.LeaseID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	c.t.Logf("Lock acquired with token: %d", lockResult.FencingToken)

	// Wait for lease to expire (3s TTL + 2s buffer for tick)
	c.t.Log("Waiting for lease to expire...")
	time.Sleep(6 * time.Second)

	// Lock should be released
	status, err := client.GetLockStatus(ctx, "expire-lock")
	assert.Nil(c.t, err)
	assert.False(c.t, status.Acquired, "Lock should be released after lease expiration")
	c.t.Log("Lock correctly released after lease expiration")

	// Test Waiter Gets Lock When Holder's Lease Expires
	c.t.Log("Test: Waiter Gets Lock When Holder's Lease Expires")

	// Owner-1 grants short lease and acquires lock
	shortLease, err := client.LeaseGrant(ctx, 3)
	assert.Nil(c.t, err)

	lockResult, err = client.Acquire(ctx, "expire-wait-lock", "owner-1", shortLease.LeaseID)
	assert.Nil(c.t, err)
	assert.True(c.t, lockResult.Acquired)
	token1 := lockResult.FencingToken
	c.t.Logf("Owner-1 acquired lock with short lease (TTL=3s), token: %d", token1)

	// Owner-2 grants long lease and waits
	longLease, err := client.LeaseGrant(ctx, 60)
	assert.Nil(c.t, err)

	requestID := uuid.New().String()

	waitDone := make(chan *embedapp.WaitResult, 1)
	go func() {
		result, err := client.AcquireWithWait(ctx, "expire-wait-lock", "owner-2", longLease.LeaseID, 30, requestID)
		waitDone <- &embedapp.WaitResult{Result: result, Err: err}
	}()

	// Owner-2 should acquire lock when owner-1's lease expires
	select {
	case wr := <-waitDone:
		assert.Nil(c.t, wr.Err)
		assert.True(c.t, wr.Result.Acquired)
		assert.True(c.t, wr.Result.FencingToken > token1)
		c.t.Logf("Owner-2 acquired lock after owner-1's lease expired, token: %d", wr.Result.FencingToken)
	case <-time.After(10 * time.Second):
		c.t.Fatal("Owner-2 wait timed out")
	}

	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIDs()))
}

func TestLeaseExpiration(t *testing.T) {
	RunTests(&LeaseExpirationTest{t: t})
}
