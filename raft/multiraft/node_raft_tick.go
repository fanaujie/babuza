package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"time"
)

func (n *Node) raftTickStart() {
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
				if err := n.scheduler.EnqueueBatchState(stateTick, groupIDs); err != nil {
					n.logger.Errorf("Node[%d] raftScheduler enqueue tick state error: %v", n.config.NodeID, err)
					return
				}
			}
			groupIDs = groupIDs[:0]
		}
	}
}
