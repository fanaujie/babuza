package multiraft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"sort"
	"sync"
)

type NodeManager struct {
	nodeMap sync.Map // map[NodeID]Node
}

func NewNodeManager() *NodeManager {
	return &NodeManager{}
}

func (nm *NodeManager) Add(node *Node) error {
	nodeID := node.config.NodeID
	if _, loaded := nm.nodeMap.LoadOrStore(nodeID, node); loaded {
		return fmt.Errorf("node already exists: %d", nodeID)
	}
	return nil
}

func (nm *NodeManager) Clear() {
	nm.nodeMap.Clear()
}

func (nm *NodeManager) Remove(nodeID uint64) error {
	if _, loaded := nm.nodeMap.LoadAndDelete(nodeID); !loaded {
		return fmt.Errorf("node not found: %d", nodeID)
	}
	return nil
}

func (nm *NodeManager) GetNodeIDsByGroupID(groupID ibabuza.RaftGroupID) []uint64 {
	allNodes := make([]uint64, 0)
	nm.nodeMap.Range(func(key, value interface{}) bool {
		n := value.(*Node)
		if n.HasGroupID(groupID) {
			allNodes = append(allNodes, n.config.NodeID)
		}
		return true
	})
	if len(allNodes) > 1 {
		sort.Slice(allNodes, func(i, j int) bool { return allNodes[i] < allNodes[j] })
	}
	return allNodes
}

func (nm *NodeManager) GetAllNodes() []*Node {
	allNodes := make([]*Node, 0)
	nm.nodeMap.Range(func(key, value interface{}) bool {
		allNodes = append(allNodes, value.(*Node))
		return true
	})
	return allNodes
}

func (nm *NodeManager) GetNode(nodeID uint64) (*Node, error) {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return nil, fmt.Errorf("node not found: %d", nodeID)
	}
	return v.(*Node), nil
}

func (nm *NodeManager) CheckSameLeader(groupID ibabuza.RaftGroupID) (uint64, error) {
	nodes := nm.GetNodeIDsByGroupID(groupID)
	if len(nodes) == 0 {
		return 0, errors.New("no nodes found")
	}
	leaderID := uint64(0)
	for _, nodeID := range nodes {
		v, ok := nm.nodeMap.Load(nodeID)
		if !ok {
			return 0, fmt.Errorf("node not found: %d", nodeID)
		}
		n := v.(*Node)
		if !n.HasGroupID(groupID) {
			continue
		}
		s, err := n.Status(groupID)
		if err != nil {
			return 0, err
		}
		if s.LeaderID == 0 {
			return 0, fmt.Errorf("groupID %d has no leader", groupID)
		}
		if leaderID == 0 {
			leaderID = s.LeaderID
		}
		if s.LeaderID != leaderID {
			return 0, fmt.Errorf("groupID %d has different leader %d", groupID, s.LeaderID)
		}
	}
	return leaderID, nil
}
