package multiraft

import "github.com/fanaujie/babuza/ibabuza"

func (n *Node) ProcessTick(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessTick()
	}
}

func (n *Node) ProcessReady(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessReady()
	}
}

func (n *Node) ProcessStep(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessStep()
	}
}

func (n *Node) ProcessProposal(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessProposal()
	}
}

func (n *Node) ProcessConfigChange(groupID ibabuza.RaftGroupID) {
	n.replicaSet.mu.RLock()
	replica, ok := n.replicaSet.replica[groupID]
	n.replicaSet.mu.RUnlock()
	if ok {
		replica.ProcessConfigChange()
	}
}
