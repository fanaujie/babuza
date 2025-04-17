package shard

import (
	"errors"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"sync"
)

var (
	ErrSchedulerFull = errors.New("scheduler ringbuffer is full")
)

const (
	StateTick         = 1
	StateReady        = 2
	StateStep         = 4
	StateProposal     = 8
	StateConfigChange = 16
)

type internalState struct {
	state int
	ticks int
}

type Scheduler struct {
	cfg           Config
	rb            *queue.RingBuffer
	raftProcessor ibabuza.RaftStateProcessor
	log           ibabuza.Logger
	mu            sync.Mutex
	groupState    map[ibabuza.RaftGroupID]internalState
	closer        *syncutil.Closer
}

func NewScheduler(cfg Config, raftProcessor ibabuza.RaftStateProcessor, log ibabuza.Logger) *Scheduler {
	s := &Scheduler{
		cfg:           cfg,
		rb:            queue.NewRingBuffer(cfg.QueueSize),
		raftProcessor: raftProcessor,
		log:           log,
		groupState:    make(map[ibabuza.RaftGroupID]internalState),
		closer:        syncutil.NewCloser(),
	}
	for i := 0; i < cfg.WorkerNum; i++ {
		s.closer.Run(func() {
			s.worker()
		})
	}
	return s
}

func (s *Scheduler) Stop() {
	s.closer.Close()
}

func (s *Scheduler) EnqueueState(gid ibabuza.RaftGroupID, state int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st, ok := s.groupState[gid]; !ok {
		s.groupState[gid] = internalState{
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
		s.groupState[gid] = st
	}
	full, err := s.rb.Offer(gid)
	if err != nil {
		return err
	}
	if full {
		s.log.Warning("scheduler ringbuffer is full")
		return ErrSchedulerFull
	}
	return nil
}

func (s *Scheduler) EnqueueBatchTickState(gids []ibabuza.RaftGroupID) error {
	gidsLen := len(gids)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < gidsLen; i++ {
		gid := gids[i]
		if st, ok := s.groupState[gid]; !ok {
			s.groupState[gid] = internalState{
				state: StateTick,
			}
		} else {
			st.state = st.state | StateTick
			st.ticks++
			if st.ticks > s.cfg.MaxTicks {
				st.ticks = s.cfg.MaxTicks
				s.log.Warningf("gid=%d reach max ticks %d", gid, st.ticks)
			}
			s.groupState[gid] = st
		}
	}
	for i := 0; i < gidsLen; i++ {
		full, err := s.rb.Offer(gids[i])
		if err != nil {
			return err
		}
		if full {
			s.log.Warning("scheduler ringbuffer is full")
			return ErrSchedulerFull
		}
	}
	return nil
}

func (s *Scheduler) worker() {
	for {
		select {
		case <-s.closer.CloseCh():
			return
		default:
		}
		g, err := s.rb.Get()
		if err != nil {
			s.log.Errorf("scheduler get from ringbuffer failed: %v", err)
			return
		}
		gid := g.(ibabuza.RaftGroupID)
		s.mu.Lock()
		oldState, ok := s.groupState[gid]
		s.groupState[gid] = internalState{}
		s.mu.Unlock()
		if !ok {
			continue
		}
		if oldState.state&StateProposal == StateProposal {
			s.raftProcessor.ProcessProposal(gid)
			oldState.state |= StateReady
		}

		if oldState.state&StateConfigChange == StateConfigChange {
			s.raftProcessor.ProcessConfigChange(gid)
			oldState.state |= StateReady
		}

		if oldState.state&StateStep == StateStep {
			s.raftProcessor.ProcessStep(gid)
			oldState.state |= StateReady
		}

		if oldState.state&StateTick == StateTick {
			for i := 0; i < oldState.ticks; i++ {
				s.raftProcessor.ProcessTick(gid)
			}
			oldState.state |= StateReady
		}
		if oldState.state&StateReady == StateReady {
			s.raftProcessor.ProcessReady(gid)
		}
		s.mu.Lock()
		newState, _ := s.groupState[gid]
		if newState.state == 0 {
			delete(s.groupState, gid)
		}
		s.mu.Unlock()
	}
}
