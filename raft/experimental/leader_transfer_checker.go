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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
)

type transferRequest struct {
	ctx              context.Context
	groupID          ibabuza.RaftGroupID
	status           ibabuza.Status
	expectedLeaderID uint64
	resultChan       chan error
}

type shardData struct {
	sync.Mutex
	requests map[ibabuza.RaftGroupID]*transferRequest
	started  bool
	startCh  chan struct{}
}

type leaderTransferChecker struct {
	shards        []*shardData
	numShards     int
	closer        *syncutil.Closer
	checkInterval time.Duration
}

func newLeaderTransferChecker(numShards int, checkInterval time.Duration, closer *syncutil.Closer) *leaderTransferChecker {
	ltc := &leaderTransferChecker{
		numShards:     numShards,
		shards:        make([]*shardData, numShards),
		closer:        closer,
		checkInterval: checkInterval,
	}

	for i := 0; i < numShards; i++ {
		ltc.shards[i] = &shardData{
			requests: make(map[ibabuza.RaftGroupID]*transferRequest),
			startCh:  make(chan struct{}),
		}
		go ltc.startShard(i)
	}

	return ltc
}

func (ltc *leaderTransferChecker) getShardIndex(groupID ibabuza.RaftGroupID) int {
	return int(groupID) % ltc.numShards
}

func (ltc *leaderTransferChecker) AddTransfer(req *transferRequest) error {
	if req == nil {
		return errors.New("transfer request cannot be nil")
	}

	shardIndex := ltc.getShardIndex(req.groupID)
	shard := ltc.shards[shardIndex]

	shard.Lock()
	defer shard.Unlock()

	_, exists := shard.requests[req.groupID]
	if exists {
		return fmt.Errorf("transfer request for group %d already exists", req.groupID)
	}

	shard.requests[req.groupID] = req

	if !shard.started {
		shard.started = true
		select {
		case shard.startCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (ltc *leaderTransferChecker) startShard(shardIndex int) {
	shard := ltc.shards[shardIndex]

	defer func() {
		shard.Lock()
		defer shard.Unlock()
		for _, v := range shard.requests {
			v.resultChan <- ltc.closer.Err()
		}
	}()

	for {
		select {
		case <-shard.startCh:
			ltc.shardChecker(shardIndex)
		case <-ltc.closer.CloseCh():
			return
		}
	}
}

func (ltc *leaderTransferChecker) shardChecker(shardIndex int) {
	shard := ltc.shards[shardIndex]
	ticker := time.NewTicker(ltc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Printf("Checking leader transfer for shard %d\n", shardIndex)
			shard.Lock()
			for _, v := range shard.requests {
				if v.ctx.Err() != nil {
					v.resultChan <- v.ctx.Err()
					delete(shard.requests, v.groupID)
				} else if v.status.CloneSoftState().Lead == v.expectedLeaderID {
					v.resultChan <- nil
					delete(shard.requests, v.groupID)
				}
			}

			if len(shard.requests) == 0 {
				shard.started = false
				shard.Unlock()
				return
			}
			shard.Unlock()

		case <-ltc.closer.CloseCh():
			return
		}
	}
}
