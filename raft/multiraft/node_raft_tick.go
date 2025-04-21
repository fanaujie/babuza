package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"time"
)

func (n *Node) raftTickStart() {
	ticker := time.NewTicker(time.Duration(n.config.LogicalTickMs) * time.Millisecond)
	defer ticker.Stop()
	var groupIDs []ibabuza.RaftGroupID
	for {
		select {
		case <-ticker.C:
			n.replicaSet.mu.RLock()
			for k, _ := range n.replicaSet.replica {
				groupIDs = append(groupIDs, k)
			}
			n.replicaSet.mu.RUnlock()
			if len(groupIDs) > 0 {
				if err := n.scheduler.EnqueueBatchTickState(groupIDs); err != nil {
					n.logger.Errorf("scheduler enqueue tick state error: %v", err)
					return
				}
			}
			groupIDs = groupIDs[:0]
		}
	}
}
