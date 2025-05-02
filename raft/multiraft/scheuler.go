package multiraft

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"sync"
)

const (
	stateQueue        = 1
	stateTick         = 2
	stateProposal     = 4
	stateConfigChange = 8
	stateStep         = 16
	stateReady        = 32
)

type internalState struct {
	state int
	ticks int
}

type schedulerConfig struct {
	shardNum       int
	shardWorkerNum int
	queueSize      uint64
	maxTicks       int
}

// raftScheduler manages the state processing of multiple Raft groups.
// The design is inspired by CockroachDB's Raft raftScheduler implementation,
// which improves performance and concurrency through sharded queues and
// batch state processing.
type raftScheduler struct {
	nodeID        uint64
	config        schedulerConfig
	raftProcessor StateProcessor
	log           ibabuza.Logger
	closer        *syncutil.Closer
	mu            sync.Mutex
	shardQueue    []*queue.RingBuffer
	groupState    map[ibabuza.RaftGroupID]internalState
}

func newScheduler(nodeID uint64, config schedulerConfig, raftProcessor StateProcessor,
	log ibabuza.Logger) Scheduler {
	return &raftScheduler{
		nodeID:        nodeID,
		config:        config,
		raftProcessor: raftProcessor,
		log:           log,
		closer:        syncutil.NewCloser(),
		groupState:    make(map[ibabuza.RaftGroupID]internalState),
	}
}

func (s *raftScheduler) Start() error {
	s.log.Infof("Node[%d] starting raftScheduler", s.nodeID)
	for i := 0; i < s.config.shardNum; i++ {
		q := queue.NewRingBuffer(s.config.queueSize)
		s.shardQueue = append(s.shardQueue, q)
		for j := 0; j < s.config.shardWorkerNum; j++ {
			s.closer.Run(func() {
				s.worker(i, j, q)
			})
		}
	}
	return nil
}

func (s *raftScheduler) Stop() {
	s.log.Infof("Node[%d] stopping raftScheduler", s.nodeID)
	for _, q := range s.shardQueue {
		q.Dispose()
	}
	s.closer.Close()
}

func (s *raftScheduler) EnqueueState(state int, groupID ibabuza.RaftGroupID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	shardQueue := s.shardQueue[groupID%ibabuza.RaftGroupID(s.config.shardNum)]
	oldState := s.groupState[groupID]
	if state&stateTick != stateTick {
		// if the state is not tick, we need to check if the state is already set
		if oldState.state&state == state {
			return
		}
	}
	oldState.state |= state
	if state&stateTick == stateTick {
		oldState.ticks++
		if oldState.ticks > s.config.maxTicks {
			oldState.ticks = s.config.maxTicks
		}
	}
	s.groupState[groupID] = oldState
	if oldState.state&stateQueue == 0 {
		oldState.state |= stateQueue
		if err := shardQueue.Put(groupID); err != nil {
			s.log.Panicf("GroupID[%d] raftScheduler enqueue state %d error: %v", groupID, state, err)
		}
	}
}

func (s *raftScheduler) EnqueueBatchState(state int, groupIDs []ibabuza.RaftGroupID) {
	for _, groupID := range groupIDs {
		s.EnqueueState(state, groupID)
	}
	return
}

func (s *raftScheduler) worker(shardID, workderID int, q *queue.RingBuffer) {
	s.log.Infof("Node[%d] starting raftScheduler worker %d-%d", s.nodeID, shardID, workderID)
	defer s.log.Infof("Node[%d] stopping raftScheduler worker %d-%d", s.nodeID, shardID, workderID)
	for {
		v, err := q.Get()
		if err != nil {
			s.log.Errorf("Node[%d] raftScheduler worker %d-%d get error: %v", s.nodeID, shardID, workderID, err)
			return
		}
		groupID := v.(ibabuza.RaftGroupID)
		s.mu.Lock()
		oldState := s.groupState[groupID]
		s.groupState[groupID] = internalState{
			state: stateQueue,
		}
		s.mu.Unlock()
		if oldState.state&stateProposal == stateProposal {
			s.raftProcessor.ProcessProposal(groupID)
			oldState.state |= stateReady
		}

		if oldState.state&stateConfigChange == stateConfigChange {
			s.raftProcessor.ProcessConfigChange(groupID)
			oldState.state |= stateReady
		}

		if oldState.state&stateStep == stateStep {
			s.raftProcessor.ProcessStep(groupID)
			oldState.state |= stateReady
		}

		if oldState.state&stateTick == stateTick {
			for i := 0; i < oldState.ticks; i++ {
				s.raftProcessor.ProcessTick(groupID)
			}
			oldState.state |= stateReady
		}
		if oldState.state&stateReady == stateReady {
			s.raftProcessor.ProcessReady(groupID)
		}
		s.mu.Lock()
		newState, _ := s.groupState[groupID]
		if newState.state == stateQueue {
			delete(s.groupState, groupID)
			s.mu.Unlock()
		} else {
			s.mu.Unlock()
			_ = q.Put(groupID)
		}
	}
}
