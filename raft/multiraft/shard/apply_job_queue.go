package shard

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
)

type ApplyJobQueue struct {
	groupID ibabuza.RaftGroupID
	q       *queue.Queue
	log     ibabuza.Logger
}

func NewApplyJobQueue(groupID ibabuza.RaftGroupID, queueSize int64, log ibabuza.Logger) *ApplyJobQueue {
	j := &ApplyJobQueue{
		groupID: groupID,
		q:       queue.New(queueSize),
		log:     log,
	}
	go j.worker()
	return j
}

func (j *ApplyJobQueue) Put(groupID ibabuza.RaftGroupID, job ibabuza.MultiRaftReplicaApplyJob) error {
	return j.q.Put(job)
}

func (j *ApplyJobQueue) Stop() {
	j.q.Dispose()
}

func (j *ApplyJobQueue) worker() {
	for {
		v, err := j.q.Get(j.q.Len())
		if err != nil {
			j.log.Errorf("GroupID %d apply job queue dispose %v", j.groupID, err)
			return
		}
		for _, job := range v {
			job.(ibabuza.MultiRaftReplicaApplyJob)()
		}
	}
}
