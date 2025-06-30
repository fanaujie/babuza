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


package experimental

import (
	"context"
	"github.com/fanaujie/babuza/pkg/status"
	"sync"
	"testing"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
)

func TestNewLeaderTransferChecker(t *testing.T) {
	closer := syncutil.NewCloser()
	defer closer.Close()

	checkInterval := time.Millisecond * 100
	ltc := newLeaderTransferChecker(2, checkInterval, closer)

	assert.NotNil(t, ltc)
	assert.Equal(t, checkInterval, ltc.checkInterval)
	assert.NotNil(t, ltc.closer)
	assert.Equal(t, 2, ltc.numShards)
	assert.Equal(t, 2, len(ltc.shards))

	for i, shard := range ltc.shards {
		assert.NotNil(t, shard, "shard %d should not be nil", i)
		assert.NotNil(t, shard.requests)
		assert.False(t, shard.started)
		assert.NotNil(t, shard.startCh)
	}
}

func TestLeaderTransferChecker_AddTransfer(t *testing.T) {
	t.Run("successful add", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Second * 10
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started
		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(1),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		shardIndex := ltc.getShardIndex(ibabuza.RaftGroupID(1))
		shard := ltc.shards[shardIndex]
		shard.Lock()
		assert.True(t, shard.started)
		assert.Contains(t, shard.requests, ibabuza.RaftGroupID(1))
		shard.Unlock()
	})

	t.Run("add duplicate group ID", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started
		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(1),
			status:           mockStatus,
			expectedLeaderID: 3,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.Nil(t, err)
		assert.Error(t, ltc.AddTransfer(req))
	})

	t.Run("add nil request", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started
		err := ltc.AddTransfer(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transfer request cannot be nil")
	})
}

func TestLeaderTransferChecker_TransferCompletion(t *testing.T) {
	t.Run("successful transfer", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started
		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(10),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		mockStatus.SetSoftState(raft.SoftState{
			Lead: 2,
		})

		select {
		case result := <-resultChan:
			assert.NoError(t, result)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for transfer completion")
		}

		shardIndex := ltc.getShardIndex(ibabuza.RaftGroupID(10))
		shard := ltc.shards[shardIndex]
		shard.Lock()
		assert.NotContains(t, shard.requests, ibabuza.RaftGroupID(10))
		shard.Unlock()
	})

	t.Run("transfer with context cancellation", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started
		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(11),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		cancel()

		select {
		case result := <-resultChan:
			assert.Error(t, result)
			assert.Contains(t, result.Error(), "context canceled")
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for context cancellation")
		}

		shardIndex := ltc.getShardIndex(ibabuza.RaftGroupID(11))
		shard := ltc.shards[shardIndex]
		shard.Lock()
		assert.NotContains(t, shard.requests, ibabuza.RaftGroupID(11))
		shard.Unlock()
	})

	t.Run("transfer with timeout", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started

		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(12),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		select {
		case result := <-resultChan:
			assert.Error(t, result)
			assert.Contains(t, result.Error(), "context deadline exceeded")
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for context timeout")
		}

		shardIndex := ltc.getShardIndex(ibabuza.RaftGroupID(12))
		shard := ltc.shards[shardIndex]
		shard.Lock()
		assert.NotContains(t, shard.requests, ibabuza.RaftGroupID(12))
		shard.Unlock()
	})
}

func TestLeaderTransferChecker_MultipleConcurrentTransfers(t *testing.T) {
	closer := syncutil.NewCloser()
	defer closer.Close()

	checkInterval := time.Millisecond * 25
	ltc := newLeaderTransferChecker(2, checkInterval, closer)
	time.Sleep(checkInterval) // Ensure the checker has started

	const numTransfers = 5
	var wg sync.WaitGroup
	wg.Add(numTransfers)

	for i := 0; i < numTransfers; i++ {
		go func(id int) {
			defer wg.Done()

			mockStatus := status.New()
			mockStatus.SetSoftState(raft.SoftState{
				Lead: 1,
			})
			resultChan := make(chan error, 1)
			ctx := context.Background()

			req := &transferRequest{
				ctx:              ctx,
				groupID:          ibabuza.RaftGroupID(id + 100),
				status:           mockStatus,
				expectedLeaderID: uint64(id + 2),
				resultChan:       resultChan,
			}

			err := ltc.AddTransfer(req)
			assert.NoError(t, err)

			time.Sleep(time.Millisecond * 25)
			mockStatus.SetSoftState(raft.SoftState{
				Lead: uint64(id + 2),
			})

			select {
			case result := <-resultChan:
				assert.NoError(t, result)
			case <-time.After(time.Second):
				t.Errorf("timeout waiting for transfer completion for group %d", id+100)
			}
		}(i)
	}

	wg.Wait()

	for _, shard := range ltc.shards {
		shard.Lock()
		assert.Empty(t, shard.requests)
		shard.Unlock()
	}
}

func TestLeaderTransferChecker_LifecycleManagement(t *testing.T) {
	t.Run("checker stops when closer is closed", func(t *testing.T) {
		closer := syncutil.NewCloser()
		checkInterval := time.Millisecond * 50
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started

		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(200),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		closer.Close()

		select {
		case result := <-resultChan:
			assert.Error(t, result)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for checker to stop")
		}
	})

	t.Run("checker handles empty request map", func(t *testing.T) {
		closer := syncutil.NewCloser()
		defer closer.Close()

		checkInterval := time.Millisecond * 25
		ltc := newLeaderTransferChecker(2, checkInterval, closer)
		time.Sleep(checkInterval) // Ensure the checker has started

		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(300),
			status:           mockStatus,
			expectedLeaderID: 2,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		mockStatus.SetSoftState(raft.SoftState{
			Lead: 2,
		})

		select {
		case result := <-resultChan:
			assert.NoError(t, result)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for transfer completion")
		}

		time.Sleep(checkInterval * 3)

		for _, shard := range ltc.shards {
			shard.Lock()
			assert.Empty(t, shard.requests)
			shard.Unlock()
		}
	})
}

func TestLeaderTransferChecker_Sharding(t *testing.T) {
	closer := syncutil.NewCloser()
	defer closer.Close()

	numShards := 4
	checkInterval := time.Millisecond * 50
	ltc := newLeaderTransferChecker(numShards, checkInterval, closer)

	t.Run("group IDs are distributed across shards", func(t *testing.T) {
		groupIDs := []ibabuza.RaftGroupID{1, 2, 3, 4, 5, 6, 7, 8}

		for _, groupID := range groupIDs {
			shardIndex := ltc.getShardIndex(groupID)
			assert.True(t, shardIndex >= 0 && shardIndex < numShards,
				"shard index %d should be within range [0, %d)", shardIndex, numShards)
		}
	})

	t.Run("requests go to correct shards", func(t *testing.T) {
		time.Sleep(checkInterval)

		for i := 0; i < 8; i++ {
			mockStatus := status.New()
			mockStatus.SetSoftState(raft.SoftState{Lead: 1})
			resultChan := make(chan error, 1)
			ctx := context.Background()

			req := &transferRequest{
				ctx:              ctx,
				groupID:          ibabuza.RaftGroupID(i + 100),
				status:           mockStatus,
				expectedLeaderID: 2,
				resultChan:       resultChan,
			}

			err := ltc.AddTransfer(req)
			assert.NoError(t, err)

			expectedShardIndex := ltc.getShardIndex(req.groupID)
			shard := ltc.shards[expectedShardIndex]
			shard.Lock()
			assert.Contains(t, shard.requests, req.groupID)
			shard.Unlock()
		}
	})
}

func TestLeaderTransferChecker_EdgeCases(t *testing.T) {
	closer := syncutil.NewCloser()
	defer closer.Close()

	checkInterval := time.Millisecond * 50
	ltc := newLeaderTransferChecker(2, checkInterval, closer)
	time.Sleep(checkInterval) // Ensure the checker has started

	t.Run("leader changes multiple times", func(t *testing.T) {
		mockStatus := status.New()
		mockStatus.SetSoftState(raft.SoftState{
			Lead: 1,
		})
		resultChan := make(chan error, 1)
		ctx := context.Background()

		req := &transferRequest{
			ctx:              ctx,
			groupID:          ibabuza.RaftGroupID(500),
			status:           mockStatus,
			expectedLeaderID: 3,
			resultChan:       resultChan,
		}

		err := ltc.AddTransfer(req)
		assert.NoError(t, err)

		mockStatus.SetSoftState(raft.SoftState{
			Lead: 2,
		})
		time.Sleep(checkInterval * 2)

		mockStatus.SetSoftState(raft.SoftState{
			Lead: 4,
		})
		time.Sleep(checkInterval * 2)

		mockStatus.SetSoftState(raft.SoftState{
			Lead: 3,
		})

		select {
		case result := <-resultChan:
			assert.NoError(t, result)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for transfer completion after multiple leader changes")
		}
	})
}
