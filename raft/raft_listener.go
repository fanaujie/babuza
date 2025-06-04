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
				r.raftListener.OnAcquiredLeader(r.status.GetHardStateTerm(), e.PeerID)
			case ibabuza.LostLeader:
				r.raftListener.OnLostLeader(r.status.GetHardStateTerm(), e.PeerID)
			case ibabuza.MemberJoined, ibabuza.MemberUpdated, ibabuza.MemberRemoved,
				ibabuza.LeanerAdded, ibabuza.LeanerPromoted:
				r.raftListener.OnMemberChange(e.Event, r.status.GetHardStateTerm(), e.PeerID)
			default:
				r.logger.Panicf("unexpected raft event: %v", e.Event)
			}

		}
	}
}
