package multiraft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
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

func (nm *NodeManager) GetGroupIDsByNodeID(nodeID uint64) ([]ibabuza.RaftGroupID, error) {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return nil, fmt.Errorf("node not found: %d", nodeID)
	}
	return v.(*Node).GetGroupIDs(), nil
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
		s, err := v.(*Node).Status(groupID)
		if err != nil {
			return 0, err
		}
		if s.LeaderID == 0 {
			return 0, fmt.Errorf("node %d has no leader", nodeID)
		}
		if leaderID == 0 {
			leaderID = s.LeaderID
		}
		if s.LeaderID != leaderID {
			return 0, fmt.Errorf("node %d has different leader %d", nodeID, s.LeaderID)
		}
	}
	return leaderID, nil
}

func (nm *NodeManager) Start(nodeID uint64) error {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return fmt.Errorf("node not found: %d", nodeID)
	}
	return v.(*Node).Start()
}

func (nm *NodeManager) Stop(nodeID uint64) error {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return fmt.Errorf("node not found: %d", nodeID)
	}
	v.(*Node).Stop()
	return nil
}

func (nm *NodeManager) CreateRaftGroup(nodeID uint64, groupID ibabuza.RaftGroupID, peersConfig *babuza.PeersConfiguration, join bool) error {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return fmt.Errorf("node not found: %d", nodeID)
	}
	return v.(*Node).CreateRaftGroup(groupID, peersConfig, join)
}

func (nm *NodeManager) Status(nodeID uint64, groupID ibabuza.RaftGroupID) (babuza.Status, error) {
	v, ok := nm.nodeMap.Load(nodeID)
	if !ok {
		return babuza.Status{}, fmt.Errorf("node not found: %d", nodeID)
	}
	return v.(*Node).Status(groupID)
}
