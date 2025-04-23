package shard

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
)

type JobQueue struct {
	groupID ibabuza.RaftGroupID
	q       *queue.Queue
	log     ibabuza.Logger
}

func NewJobQueue(groupID ibabuza.RaftGroupID, queueSize int64, log ibabuza.Logger) *JobQueue {
	return &JobQueue{
		groupID: groupID,
		q:       queue.New(queueSize),
		log:     log,
	}
}

func (j *JobQueue) Put(job ibabuza.MultiRaftReplicaJob) error {
	return j.q.Put(job)
}

func (j *JobQueue) Start() error {
	j.log.Infof("[GroupID=%d] Starting job queue", j.groupID)
	go j.worker()
	return nil
}

func (j *JobQueue) Stop() {
	j.q.Dispose()
}

func (j *JobQueue) worker() {
	defer j.log.Infof("[GroupID=%d] Stopping job queue", j.groupID)
	for {
		v, err := j.q.Get(j.q.Len())
		if err != nil {
			j.log.Errorf("GroupID %d apply job queue dispose %v", j.groupID, err)
			return
		}
		for _, job := range v {
			job.(ibabuza.MultiRaftReplicaJob)()
		}
	}
}
