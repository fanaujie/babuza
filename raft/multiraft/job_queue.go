package multiraft

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
