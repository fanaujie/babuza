package experimental

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"time"
)

const (
	invalidNodeTimeSecond = 60 * 5
)

type raftEventPublisher struct {
	ch chan ibabuza.RaftEvent
}

func newRaftEventPublisher() *raftEventPublisher {
	return &raftEventPublisher{
		ch: make(chan ibabuza.RaftEvent, 256),
	}
}

func (p *raftEventPublisher) Publish(e ibabuza.RaftEvent) {
	p.ch <- e
}

func (s *Store) replicaRaftTick() {
	s.logger.Debugf("Store[%d] raft tick start", s.config.StoreID)
	defer s.logger.Debugf("Store[%d] raft tick end", s.config.StoreID)
	ticker := time.NewTicker(time.Duration(s.config.LogicalTickMs) * time.Millisecond)
	defer ticker.Stop()
	var groupIDs []ibabuza.RaftGroupID
	for {
		select {
		case <-s.closer.CloseCh():
			return
		case <-ticker.C:
			s.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
				groupIDs = append(groupIDs, key)
				return true
			})
			if len(groupIDs) > 0 {
				s.scheduler.EnqueueBatchState(stateTick, groupIDs)
			}
			groupIDs = groupIDs[:0]
		}
	}
}
func (s *Store) replicaCoalescedHeartbeat() {
	s.logger.Debugf("Store[%d] coalesced heartbeat start", s.config.StoreID)
	defer s.logger.Debugf("Store[%d] coalesced heartbeat end", s.config.StoreID)
	heartbeatTicker := time.NewTicker(time.Duration(s.config.CoalescedHeartbeatTickMs) * time.Millisecond)
	checkTicker := time.NewTicker(time.Minute)
	defer heartbeatTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case <-s.closer.CloseCh():
			return
		case <-checkTicker.C:
			// check heartbeat last active time
			s.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Range(func(peerIden string, lastActiveTime int64) bool {
				if lastActiveTime > 0 {
					if time.Now().Unix()-lastActiveTime > invalidNodeTimeSecond {
						s.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Delete(peerIden)
						s.coalescedHeartbeatQueue.heartbeatMsg.Delete(peerIden)
						s.coalescedHeartbeatQueue.heartbeatRespMsg.Delete(peerIden)
					}
				}
				return true
			})
		case <-heartbeatTicker.C:
			s.coalescedHeartbeatQueue.heartbeatMsg.Range(func(peerIden string, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					s.logger.Panicf("Store[%d] coalesced heartbeat get error: %v", s.config.StoreID, err)
				}
				if len(heartbeats.Data) > 0 {
					s.trans.SendHeartbeat(peerIden, heartbeats.Data, nil)
					heartbeats.Release()
				}
				return true
			})
			s.coalescedHeartbeatQueue.heartbeatRespMsg.Range(func(peerIden string, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					s.logger.Panicf("Store[%d] coalesced heartbeat response get error: %v", s.config.StoreID, err)
				}
				if len(heartbeats.Data) > 0 {
					s.trans.SendHeartbeat(peerIden, nil, heartbeats.Data)
					heartbeats.Release()
				}
				return true
			})
		}
	}
}

func (s *Store) replicaListener() {
	s.logger.Debugf("Store[%d] replica listener start", s.config.StoreID)
	defer s.logger.Debugf("Store[%d] replica listener end", s.config.StoreID)

	for {
		select {
		case <-s.closer.CloseCh():
			return
		case event := <-s.raftEventPublisher.ch:
			switch event.Event {
			case ibabuza.LeaderChanged:
				if s.raftListener != nil {
					r, ok := s.replicaSet.Load(event.GroupID)
					if ok {
						s.raftListener.OnLeaderChange(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						s.logger.Warningf("Store[%d] leader change event for group %d but replica not found", s.config.StoreID, event.GroupID)
					}
				}
			case ibabuza.AcquiredLeader:
				if s.raftListener != nil {
					r, ok := s.replicaSet.Load(event.GroupID)
					if ok {
						s.raftListener.OnAcquiredLeader(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						s.logger.Warningf("Store[%d] acquired leader event for group %d but replica not found", s.config.StoreID, event.GroupID)
					}
				}
			case ibabuza.LostLeader:
				if s.raftListener != nil {
					r, ok := s.replicaSet.Load(event.GroupID)
					if ok {
						s.raftListener.OnLostLeader(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						s.logger.Warningf("Store[%d] lost leader event for group %d but replica not found", s.config.StoreID, event.GroupID)
					}
				}
			case ibabuza.MemberJoined, ibabuza.MemberUpdated, ibabuza.MemberRemoved,
				ibabuza.LeanerAdded, ibabuza.LeanerPromoted:
				if s.raftListener != nil {
					r, ok := s.replicaSet.Load(event.GroupID)
					if ok {
						s.raftListener.OnMemberChange(event.Event, event.GroupID, r.status.GetHardStateTerm(), event.PeerID)
					} else {
						s.logger.Warningf("Store[%d] member change event for group %d but replica not found", s.config.StoreID, event.GroupID)
					}
				}
			case ibabuza.RemoveSelf:
				r, ok := s.replicaSet.Load(event.GroupID)
				if ok {
					r.Stop()
					s.requestQueues.Delete(event.GroupID)
					s.replicaSet.Delete(event.GroupID)
					s.logger.Infof("Store[%d] remove replica group %d", s.config.StoreID, event.GroupID)
				} else {
					s.logger.Warningf("Store[%d] remove self event for group %d but replica not found", s.config.StoreID, event.GroupID)
				}
			default:
			}
		}
	}
}
