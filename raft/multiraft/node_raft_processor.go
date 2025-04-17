package multiraft

import "github.com/fanaujie/babuza/ibabuza"

func (n *Node) ProcessTick(gid ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessTick()
	}
}

func (n *Node) ProcessReady(gid ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessReady()
	}
}

func (n *Node) ProcessStep(gid ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessStep()
	}
}

func (n *Node) ProcessProposal(gid ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessProposal()
	}
}

func (n *Node) ProcessConfigChange(gid ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[gid]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessConfigChange()
	}
}
