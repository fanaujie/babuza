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


package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
)

type raftEventPublisher struct {
	ch chan ibabuza.RaftEvent
}

func newRaftEventPublisher() *raftEventPublisher {
	return &raftEventPublisher{
		ch: make(chan ibabuza.RaftEvent, 16),
	}
}

func (p *raftEventPublisher) Publish(e ibabuza.RaftEvent) {
	p.ch <- e
}

func (r *Raft) handleListenerEvent() {
	for {
		select {
		case <-r.closer.CloseCh():
			r.raftListener.OnRaftShutdown()
			return
		case e := <-r.raftEventPublisher.ch:
			switch e.Event {
			case ibabuza.LeaderChanged:
				r.raftListener.OnLeaderChange(r.status.GetHardStateTerm(), e.PeerID)
			case ibabuza.AcquiredLeader:
				r.raftListener.OnAcquiredLeader()
			case ibabuza.LostLeader:
				r.raftListener.OnLostLeader()
			case ibabuza.MemberJoined, ibabuza.MemberUpdated, ibabuza.MemberRemoved,
				ibabuza.LeanerAdded, ibabuza.LeanerPromoted:
				r.raftListener.OnMemberChange(e.Event, r.status.GetHardStateTerm(), e.PeerID)
			default:
				r.logger.Panicf("unexpected raft event: %v", e.Event)
			}

		}
	}
}
