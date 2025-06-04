package multiraft

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

func (n *Node) replicaRaftTick() {
	n.logger.Debugf("Node[%d] raft tick start", n.config.NodeID)
	defer n.logger.Debugf("Node[%d] raft tick end", n.config.NodeID)
	ticker := time.NewTicker(time.Duration(n.config.LogicalTickMs) * time.Millisecond)
	defer ticker.Stop()
	var groupIDs []ibabuza.RaftGroupID
	for {
		select {
		case <-n.closer.CloseCh():
			return
		case <-ticker.C:
			n.replicaSet.Range(func(key ibabuza.RaftGroupID, value *replica) bool {
				groupIDs = append(groupIDs, key)
				return true
			})
			if len(groupIDs) > 0 {
				n.scheduler.EnqueueBatchState(stateTick, groupIDs)
			}
			groupIDs = groupIDs[:0]
		}
	}
}
func (n *Node) replicaCoalescedHeartbeat() {
	n.logger.Debugf("Node[%d] coalesced heartbeat start", n.config.NodeID)
	defer n.logger.Debugf("Node[%d] coalesced heartbeat end", n.config.NodeID)
	heartbeatTicker := time.NewTicker(time.Duration(n.config.CoalescedHeartbeatTickMs) * time.Millisecond)
	checkTicker := time.NewTicker(time.Minute)
	defer heartbeatTicker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case <-n.closer.CloseCh():
			return
		case <-checkTicker.C:
			// check heartbeat last active time
			n.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Range(func(peerIden string, lastActiveTime int64) bool {
				if lastActiveTime > 0 {
					if time.Now().Unix()-lastActiveTime > invalidNodeTimeSecond {
						n.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Delete(peerIden)
						n.coalescedHeartbeatQueue.heartbeatMsg.Delete(peerIden)
						n.coalescedHeartbeatQueue.heartbeatRespMsg.Delete(peerIden)
					}
				}
				return true
			})
		case <-heartbeatTicker.C:
			n.coalescedHeartbeatQueue.heartbeatMsg.Range(func(peerIden string, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					n.logger.Panicf("Node[%d] coalesced heartbeat get error: %v", n.config.NodeID, err)
				}
				if len(heartbeats.Data) > 0 {
					n.trans.SendHeartbeat(peerIden, heartbeats.Data, nil)
					heartbeats.Release()
				}
				return true
			})
			n.coalescedHeartbeatQueue.heartbeatRespMsg.Range(func(peerIden string, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					n.logger.Panicf("Node[%d] coalesced heartbeat response get error: %v", n.config.NodeID, err)
				}
				if len(heartbeats.Data) > 0 {
					n.trans.SendHeartbeat(peerIden, nil, heartbeats.Data)
					heartbeats.Release()
				}
				return true
			})
		}
	}
}

func (n *Node) replicaListener() {
	n.logger.Debugf("Node[%d] replica listener start", n.config.NodeID)
	defer n.logger.Debugf("Node[%d] replica listener end", n.config.NodeID)

	for {
		select {
		case <-n.closer.CloseCh():
			return
		case event := <-n.raftEventPublisher.ch:
			switch event.Event {
			case ibabuza.LeaderChanged:
				if n.raftListener != nil {
					r, ok := n.replicaSet.Load(event.GroupID)
					if ok {
						n.raftListener.OnLeaderChange(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						n.logger.Warningf("Node[%d] leader change event for group %d but replica not found", n.config.NodeID, event.GroupID)
					}
				}
			case ibabuza.AcquiredLeader:
				if n.raftListener != nil {
					r, ok := n.replicaSet.Load(event.GroupID)
					if ok {
						n.raftListener.OnAcquiredLeader(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						n.logger.Warningf("Node[%d] acquired leader event for group %d but replica not found", n.config.NodeID, event.GroupID)
					}
				}
			case ibabuza.LostLeader:
				if n.raftListener != nil {
					r, ok := n.replicaSet.Load(event.GroupID)
					if ok {
						n.raftListener.OnLostLeader(event.GroupID, r.status.GetHardStateTerm(),
							event.PeerID)
					} else {
						n.logger.Warningf("Node[%d] lost leader event for group %d but replica not found", n.config.NodeID, event.GroupID)
					}
				}
			case ibabuza.MemberJoined, ibabuza.MemberUpdated, ibabuza.MemberRemoved,
				ibabuza.LeanerAdded, ibabuza.LeanerPromoted:
				if n.raftListener != nil {
					r, ok := n.replicaSet.Load(event.GroupID)
					if ok {
						n.raftListener.OnMemberChange(event.Event, event.GroupID, r.status.GetHardStateTerm(), event.PeerID)
					} else {
						n.logger.Warningf("Node[%d] member change event for group %d but replica not found", n.config.NodeID, event.GroupID)
					}
				}
			case ibabuza.RemoveSelf:
				r, ok := n.replicaSet.Load(event.GroupID)
				if ok {
					r.Stop()
					n.replicaSet.Delete(event.GroupID)
					n.logger.Infof("Node[%d] remove replica group %d", n.config.NodeID, event.GroupID)
				} else {
					n.logger.Warningf("Node[%d] remove self event for group %d but replica not found", n.config.NodeID, event.GroupID)
				}
			default:
			}
		}
	}
}
