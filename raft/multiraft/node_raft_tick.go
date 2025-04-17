package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"time"
)

func (n *Node) raftTickStart() {
	ticker := time.NewTicker(time.Duration(n.config.LogicalTickMs) * time.Millisecond)
	defer ticker.Stop()
	var gids []ibabuza.RaftGroupID
	for {
		select {
		case <-ticker.C:
			n.replicaSet.mu.RLock()
			for k, _ := range n.replicaSet.replica {
				gids = append(gids, k)
			}
			n.replicaSet.mu.RUnlock()
			if len(gids) > 0 {
				if err := n.scheduler.EnqueueBatchTickState(gids); err != nil {
					n.log.Errorf("scheduler enqueue tick state error: %v", err)
					return
				}
			}
			gids = gids[:0]
		}
	}
}
