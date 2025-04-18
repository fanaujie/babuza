package shard

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
)

type ApplyJobQueue struct {
	workerNum int
	rb        []*queue.Queue
	log       ibabuza.Logger
}

func NewApplyJobQueue(workerNum int, log ibabuza.Logger) *ApplyJobQueue {
	j := &ApplyJobQueue{
		workerNum: workerNum,
		log:       log,
	}
	for i := 0; i < workerNum; i++ {
		q := queue.New(256)
		j.rb = append(j.rb, q)
		go j.worker(i, q)
	}
	return j
}

func (j *ApplyJobQueue) Put(groupID uint64, job ibabuza.MultiRaftApplyJob) error {
	return j.rb[int(groupID)%j.workerNum].Put(job)
}

func (j *ApplyJobQueue) Stop() {
	for i := 0; i < j.workerNum; i++ {
		j.rb[i].Dispose()
	}
}

func (j *ApplyJobQueue) worker(shardID int, q *queue.Queue) {
	for {
		v, err := q.Get(q.Len())
		if err != nil {
			j.log.Errorf("apply job queue[%d] dispose %v", shardID, err)
			return
		}
		for _, job := range v {
			job.(ibabuza.MultiRaftApplyJob)()
		}
	}
}
