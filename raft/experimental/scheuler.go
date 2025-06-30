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

type sharder struct {
	mu           sync.Mutex
	queue        *queue.Queue[ibabuza.RaftGroupID]
	groupState   map[ibabuza.RaftGroupID]internalState
	inProcessing sync.Map
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
	sharders      []*sharder
}

func newScheduler(nodeID uint64, config schedulerConfig, raftProcessor StateProcessor,
	log ibabuza.Logger) Scheduler {
	return &raftScheduler{
		nodeID:        nodeID,
		config:        config,
		raftProcessor: raftProcessor,
		log:           log,
		closer:        syncutil.NewCloser(),
	}
}

func (s *raftScheduler) Start() error {
	s.log.Infof("Node[%d] starting raftScheduler", s.nodeID)
	for i := 0; i < s.config.shardNum; i++ {
		q := queue.New[ibabuza.RaftGroupID](int64(s.config.queueSize))
		sh := &sharder{
			queue:      q,
			groupState: make(map[ibabuza.RaftGroupID]internalState),
		}
		s.sharders = append(s.sharders, sh)
		for j := 0; j < s.config.shardWorkerNum; j++ {
			s.closer.Run(func() {
				s.worker(i, j, sh)
			})
		}
	}
	return nil
}

func (s *raftScheduler) Stop() {
	s.log.Infof("Node[%d] stopping raftScheduler", s.nodeID)
	for _, sh := range s.sharders {
		sh.queue.Dispose()
	}
	s.closer.Close()
}

func (s *raftScheduler) EnqueueState(state int, groupID ibabuza.RaftGroupID) {
	shardIdx := groupID % ibabuza.RaftGroupID(s.config.shardNum)
	sh := s.sharders[shardIdx]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	oldState := sh.groupState[groupID]
	if state&stateTick != stateTick {
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
	sh.groupState[groupID] = oldState
	if oldState.state&stateQueue == 0 {
		oldState.state |= stateQueue
		if err := sh.queue.Put(groupID); err != nil {
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

func (s *raftScheduler) worker(shardID, workerID int, sh *sharder) {
	s.log.Debugf("Node[%d] starting raftScheduler worker %d-%d", s.nodeID, shardID, workerID)
	defer s.log.Debugf("Node[%d] stopping raftScheduler worker %d-%d", s.nodeID, shardID, workerID)
	for {
		groupID, err := sh.queue.GetOne()
		if err != nil {
			s.log.Debugf("Node[%d] raftScheduler worker %d-%d get error: %v", s.nodeID, shardID, workerID, err)
			return
		}
		if groupID == 0 { // empty queue
			continue
		}
		if _, loaded := sh.inProcessing.LoadOrStore(groupID, true); loaded {
			// drop the message if already in processing
			continue
		}
		sh.mu.Lock()
		oldState := sh.groupState[groupID]
		sh.groupState[groupID] = internalState{
			state: stateQueue,
		}
		sh.mu.Unlock()
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
		sh.inProcessing.Delete(groupID)
		sh.mu.Lock()
		newState, _ := sh.groupState[groupID]
		if newState.state == stateQueue {
			delete(sh.groupState, groupID)
			sh.mu.Unlock()
		} else {
			sh.mu.Unlock()
			_ = sh.queue.Put(groupID)
		}
	}
}
