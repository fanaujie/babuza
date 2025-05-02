package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"time"
)

const (
	eventRemovePeer = 1
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
			n.replicaSet.mu.RLock()
			for k, _ := range n.replicaSet.replica {
				groupIDs = append(groupIDs, k)
			}
			n.replicaSet.mu.RUnlock()
			if len(groupIDs) > 0 {
				n.scheduler.EnqueueBatchState(stateTick, groupIDs)
			}
			groupIDs = groupIDs[:0]
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
				n.replicaSet.mu.Lock()
				if r, ok := n.replicaSet.replica[event.groupID]; ok {
					r.Stop()
					delete(n.replicaSet.replica, event.groupID)
					n.logger.Infof("Node[%d] remove replica group %d", n.config.NodeID, event.groupID)
				}
				n.replicaSet.mu.Unlock()
			}
		}
	}
}
