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
			n.replicaSet.Range(func(key, value any) bool {
				groupIDs = append(groupIDs, key.(ibabuza.RaftGroupID))
				return true
			})
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
				r, ok := n.replicaSet.Load(event.groupID)
				if ok {
					r.(*replica).Stop()
					n.replicaSet.Delete(event.groupID)
					n.logger.Infof("Node[%d] remove replica group %d", n.config.NodeID, event.groupID)
				}
			}
		}
	}
}
