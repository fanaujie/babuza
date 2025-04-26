package multiraft

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
)

type jobQueue struct {
	groupID ibabuza.RaftGroupID
	q       *queue.Queue
	log     ibabuza.Logger
}

func newJobQueue(groupID ibabuza.RaftGroupID, queueSize int64, log ibabuza.Logger) JobQueue {
	return &jobQueue{
		groupID: groupID,
		q:       queue.New(queueSize),
		log:     log,
	}
}

func (j *jobQueue) Put(job JobFunc) error {
	return j.q.Put(job)
}

func (j *jobQueue) Start() error {
	j.log.Infof("GroupID[%d] Starting job queue", j.groupID)
	go j.worker()
	return nil
}

func (j *jobQueue) Stop() {
	j.log.Infof("GroupID[%d] Stopping job queue", j.groupID)
	j.q.Dispose()
}

func (j *jobQueue) worker() {
	defer j.log.Infof("GroupID[%d] Stopping job queue", j.groupID)
	for {
		v, err := j.q.Get(5)
		if err != nil {
			j.log.Errorf("GroupID[%d] job queue dispose %v", j.groupID, err)
			return
		}
		for _, job := range v {
			job.(JobFunc)()
		}
	}
}
