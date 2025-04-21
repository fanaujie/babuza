package shard

import (
	"errors"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

var (
	ErrSchedulerFull = errors.New("scheduler ringbuffer is full")
)

const (
	StateTick            = 1
	StateReady           = 2
	StateStep            = 4
	StateProposal        = 8
	StateConfigChange    = 16
	StateApplyConfChange = 32
)

type internalState struct {
	state int
	ticks int
}

type Scheduler struct {
	cfg           Config
	q             []*queue.Queue
	raftProcessor ibabuza.MultiRaftReplicaStateProcessor
	log           ibabuza.Logger
	mu            sync.Mutex
	groupState    map[ibabuza.RaftGroupID]internalState
	start         bool
}

func NewScheduler(cfg Config, raftProcessor ibabuza.MultiRaftReplicaStateProcessor, log ibabuza.Logger) *Scheduler {
	s := &Scheduler{
		cfg:           cfg,
		raftProcessor: raftProcessor,
		log:           log,
		groupState:    make(map[ibabuza.RaftGroupID]internalState),
	}
	for i := 0; i < s.cfg.WorkerNum; i++ {
		q := queue.New(256)
		s.q = append(s.q, q)
		go s.worker(i, q)
	}
	return s
}

func (s *Scheduler) Stop() {
	for i := 0; i < s.cfg.WorkerNum; i++ {
		s.q[i].Dispose()
	}
}

func (s *Scheduler) EnqueueState(groupID ibabuza.RaftGroupID, state int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.groupState[groupID]; !ok {
		s.groupState[groupID] = internalState{
			state: state,
		}
	} else {
		st.state = st.state | state
		if state&StateTick == StateTick {
			st.ticks++
			if st.ticks > s.cfg.MaxTicks {
				st.ticks = s.cfg.MaxTicks
			}
		}
		s.groupState[groupID] = st
	}
	return s.q[groupID%ibabuza.RaftGroupID(s.cfg.WorkerNum)].Put(groupID)
}

func (s *Scheduler) EnqueueBatchTickState(groupIDs []ibabuza.RaftGroupID) error {
	groupIDsLen := len(groupIDs)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < groupIDsLen; i++ {
		groupID := groupIDs[i]
		if st, ok := s.groupState[groupID]; !ok {
			s.groupState[groupID] = internalState{
				state: StateTick,
			}
		} else {
			st.state = st.state | StateTick
			st.ticks++
			if st.ticks > s.cfg.MaxTicks {
				st.ticks = s.cfg.MaxTicks
				s.log.Warningf("groupID=%d reach max ticks %d", groupID, st.ticks)
			}
			s.groupState[groupID] = st
		}
	}
	for i := 0; i < groupIDsLen; i++ {
		if err := s.q[groupIDs[i]%ibabuza.RaftGroupID(s.cfg.WorkerNum)].Put(groupIDs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) worker(shardID int, q *queue.Queue) {
	for {
		items, err := q.Get(q.Len())
		if err != nil {
			s.log.Errorf("scheduler get from ringbuffer failed: %v", err)
			return
		}
		for _, v := range items {
			groupID := v.(ibabuza.RaftGroupID)
			s.mu.Lock()
			oldState, ok := s.groupState[groupID]
			s.groupState[groupID] = internalState{}
			s.mu.Unlock()
			if !ok {
				continue
			}
			if oldState.state&StateProposal == StateProposal {
				s.raftProcessor.ProcessProposal(groupID)
				oldState.state |= StateReady
			}

			if oldState.state&StateConfigChange == StateConfigChange {
				s.raftProcessor.ProcessConfigChange(groupID)
				oldState.state |= StateReady
			}

			if oldState.state&StateApplyConfChange == StateApplyConfChange {
				s.raftProcessor.ProcessConfigChange(groupID)
				oldState.state |= StateReady
			}

			if oldState.state&StateStep == StateStep {
				s.raftProcessor.ProcessStep(groupID)
				oldState.state |= StateReady
			}

			if oldState.state&StateTick == StateTick {
				for i := 0; i < oldState.ticks; i++ {
					s.raftProcessor.ProcessTick(groupID)
				}
				oldState.state |= StateReady
			}
			if oldState.state&StateReady == StateReady {
				s.raftProcessor.ProcessReady(groupID)
			}
			s.mu.Lock()
			newState, _ := s.groupState[groupID]
			if newState.state == 0 {
				delete(s.groupState, groupID)
			}
			s.mu.Unlock()
		}
	}
}
