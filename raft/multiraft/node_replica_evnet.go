package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/queue"
	"time"
)

const (
	eventRemovePeer = 1

	invalidNodeTimeSecond = 60 * 5
)

type replicaEvent struct {
	groupID ibabuza.RaftGroupID
	event   int
}

func (n *Node) replicaRaftTick() {
	n.logger.Infof("Node[%d] raft tick start", n.config.NodeID)
	defer n.logger.Infof("Node[%d] raft tick end", n.config.NodeID)
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
	n.logger.Infof("Node[%d] coalesced heartbeat start", n.config.NodeID)
	defer n.logger.Infof("Node[%d] coalesced heartbeat end", n.config.NodeID)
	ticker := time.NewTicker(time.Duration(n.config.CoalescedHeartbeatTickMs) * time.Millisecond)
	checkTicker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	defer checkTicker.Stop()
	for {
		select {
		case <-n.closer.CloseCh():
			return
		case <-checkTicker.C:
			// check heartbeat last active time
			n.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Range(func(to uint64, lastActiveTime int64) bool {
				if lastActiveTime > 0 {
					if time.Now().Unix()-lastActiveTime > invalidNodeTimeSecond {
						n.coalescedHeartbeatQueue.heartbeatLastActiveUnixSec.Delete(to)
						n.coalescedHeartbeatQueue.heartbeatMsg.Delete(to)
						n.coalescedHeartbeatQueue.heartbeatRespMsg.Delete(to)
					}
				}
				return true
			})
		case <-ticker.C:
			n.coalescedHeartbeatQueue.heartbeatMsg.Range(func(to uint64, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					n.logger.Panicf("Node[%d] coalesced heartbeat get error: %v", n.config.NodeID, err)
				}
				if len(heartbeats.Data) > 0 {
					n.trans.SendHeartbeat(to, heartbeats.Data, nil)
					heartbeats.Release()
				}
				return true
			})
			n.coalescedHeartbeatQueue.heartbeatRespMsg.Range(func(to uint64, q *queue.SwapBufferQueue[babuzapb.MultiRaftHeartbeatMessage]) bool {
				heartbeats, err := q.Get()
				if err != nil {
					n.logger.Panicf("Node[%d] coalesced heartbeat response get error: %v", n.config.NodeID, err)
				}
				if len(heartbeats.Data) > 0 {
					n.trans.SendHeartbeat(to, nil, heartbeats.Data)
					heartbeats.Release()
				}
				return true
			})
		}
	}
}

func (n *Node) replicaListener() {
	n.logger.Infof("Node[%d] replica listener start", n.config.NodeID)
	defer n.logger.Infof("Node[%d] replica listener end", n.config.NodeID)

	for {
		select {
		case <-n.closer.CloseCh():
			return
		case event := <-n.replicaEventCh:
			switch event.event {
			case eventRemovePeer:
				r, ok := n.replicaSet.Load(event.groupID)
				if ok {
					r.Stop()
					n.replicaSet.Delete(event.groupID)
					n.logger.Infof("Node[%d] remove replica group %d", n.config.NodeID, event.groupID)
				}
			}
		}
	}
}
