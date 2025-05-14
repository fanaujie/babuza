package multiraft

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/queue"
)

type jobQueue struct {
	groupID ibabuza.RaftGroupID
	q       *queue.Queue[JobFunc]
	log     ibabuza.Logger
}

func newJobQueue(groupID ibabuza.RaftGroupID, queueSize int64, log ibabuza.Logger) JobQueue {
	return &jobQueue{
		groupID: groupID,
		q:       queue.New[JobFunc](queueSize),
		log:     log,
	}
}

func (j *jobQueue) Put(job JobFunc) error {
	return j.q.Put(job)
}

func (j *jobQueue) Start() error {
	go j.worker()
	return nil
}

func (j *jobQueue) Stop() {
	j.q.Dispose()
}

func (j *jobQueue) worker() {
	j.log.Debugf("GroupID[%d] Starting job queue", j.groupID)
	defer j.log.Debugf("GroupID[%d] Stopping job queue", j.groupID)
	jobItems := make([]JobFunc, 64)
	for {
		v, err := j.q.Get(64, jobItems)
		if err != nil {
			j.log.Debugf("GroupID[%d] job queue get error: %v", j.groupID, err)
			if errors.Is(err, queue.ErrQueueDisposed) {
				return
			}
			continue
		}
		for i := int64(0); i < v; i++ {
			jobItems[i]()
		}
	}
}
