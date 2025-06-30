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
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"sync"
)

const maxBatchSize = 256

type shardedJobQueue struct {
	shards   []*queue.Queue[JobFunc]
	shardNum uint64
	log      ibabuza.Logger
	wg       sync.WaitGroup
}

func newShardedJobQueue(shardNum uint64, queueSize int64, log ibabuza.Logger) JobQueue {
	shards := make([]*queue.Queue[JobFunc], shardNum)
	for i := uint64(0); i < shardNum; i++ {
		shards[i] = queue.New[JobFunc](queueSize)
	}
	return &shardedJobQueue{
		shards:   shards,
		shardNum: shardNum,
		log:      log,
	}
}

func (j *shardedJobQueue) Put(groupID ibabuza.RaftGroupID, job JobFunc) error {
	shardIdx := uint64(groupID) % j.shardNum
	return j.shards[shardIdx].Put(job)
}

func (j *shardedJobQueue) Start() error {
	for i := uint64(0); i < j.shardNum; i++ {
		j.wg.Add(1)
		go j.worker(i)
	}
	return nil
}

func (j *shardedJobQueue) Stop() {
	for _, shard := range j.shards {
		shard.Dispose()
	}
	j.wg.Wait()
}

func (j *shardedJobQueue) worker(shardIdx uint64) {
	defer j.wg.Done()
	j.log.Debugf("Shard[%d] Starting job queue worker", shardIdx)
	defer j.log.Debugf("Shard[%d] Stopping job queue worker", shardIdx)

	shard := j.shards[shardIdx]
	jobItems := make([]JobFunc, maxBatchSize)
	for {
		v, err := shard.Get(maxBatchSize, jobItems)
		if err != nil {
			j.log.Debugf("Shard[%d] job queue get error: %v", shardIdx, err)
			return
		}
		for i := int64(0); i < v; i++ {
			jobItems[i]()
		}
	}
}
