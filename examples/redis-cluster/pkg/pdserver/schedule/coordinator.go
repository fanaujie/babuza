package schedule

import (
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/infostore"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/operator"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/schedulers"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"sync"
	"time"
)

type Coordinator struct {
	stopCh       chan struct{}
	infoManager  infostore.InfoManager
	schedulerMgr struct {
		mu         sync.Mutex
		schedulers map[string]schedulers.Scheduler
	}
	raftGroupOpMgr struct {
		mu           sync.Mutex
		raftGroupOps map[uint64]operator.Operator
	}
	wg sync.WaitGroup
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		stopCh:      make(chan struct{}),
		infoManager: infostore.NewInfoManager(),
		schedulerMgr: struct {
			mu         sync.Mutex
			schedulers map[string]schedulers.Scheduler
		}{
			schedulers: make(map[string]schedulers.Scheduler),
		},
		raftGroupOpMgr: struct {
			mu           sync.Mutex
			raftGroupOps map[uint64]operator.Operator
		}{
			raftGroupOps: make(map[uint64]operator.Operator),
		},
	}
}

func (c *Coordinator) AddScheduleTask(scheduler schedulers.Scheduler) error {
	c.schedulerMgr.mu.Lock()
	defer c.schedulerMgr.mu.Unlock()
	if _, exists := c.schedulerMgr.schedulers[scheduler.Name()]; exists {
		return fmt.Errorf("scheduler %s already exists", scheduler.Name())
	}
	c.schedulerMgr.schedulers[scheduler.Name()] = scheduler
	c.wg.Add(1)
	go c.runScheduler(scheduler)
	return nil
}

func (c *Coordinator) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *Coordinator) DoStoreHeartbeat(store pb.StoreHeartbeatReq) (*pb.StoreHeartbeatResp, error) {
	fmt.Printf("Store %d heartbeat received, leader count: %d\n", store.StoreID, store.LeaderCount)
	c.infoManager.AddOrUpdateStore(store.StoreID, infostore.CreateStoreInfo(
		store.StoreID, store.LeaderCount))
	c.infoManager.UpdateRoutingTable(store.RedisListenAddr, store.LeaderGroupIDs)
	return &pb.StoreHeartbeatResp{
		ClusterID:         store.ClusterID,
		RedisRoutingTable: c.infoManager.RoutingTable(),
	}, nil
}

func (c *Coordinator) DoRaftGroupLeaderHeartbeat(group pb.RaftGroupLeaderHeartbeatReq) (*pb.RaftGroupLeaderHeartbeatResp, error) {
	//fmt.Printf("RaftGroupLeaderHeartbeat for group %d received, leader ID: %d, peers: %v\n",
	//	group.GroupID, group.LeaderID, group.Peers)
	groupInfo := infostore.CreateGroupInfo(
		group.StoreID, group.GroupID, group.LeaderID, group.Peers)
	c.infoManager.AddOrUpdateGroup(group.GroupID, groupInfo)
	c.raftGroupOpMgr.mu.Lock()
	defer c.raftGroupOpMgr.mu.Unlock()
	if op, exists := c.raftGroupOpMgr.raftGroupOps[groupInfo.GroupID()]; exists {
		if op.Finish(groupInfo) {
			fmt.Printf("Raft group operation for group %d finished, removing from manager\n", groupInfo.GroupID())
			delete(c.raftGroupOpMgr.raftGroupOps, groupInfo.GroupID())
			return &pb.RaftGroupLeaderHeartbeatResp{}, nil
		}
		newLeader := op.Payload().(babuzapb.RaftPeerAttribute)
		fmt.Printf("Transferring leader for group %d to new leader %d\n", groupInfo.GroupID(), newLeader.PeerID)
		return &pb.RaftGroupLeaderHeartbeatResp{
			TransferLeader: &pb.TransferLeader{
				NewLeaderID: newLeader.PeerID,
			},
		}, nil
	}
	return &pb.RaftGroupLeaderHeartbeatResp{}, nil
}

func (c *Coordinator) AddRaftGroupOp(op operator.Operator) bool {
	return c.addRaftGroupOp(op)
}

func (c *Coordinator) InfoManager() infostore.InfoManager {
	return c.infoManager
}

func (c *Coordinator) addRaftGroupOp(op operator.Operator) bool {
	c.raftGroupOpMgr.mu.Lock()
	defer c.raftGroupOpMgr.mu.Unlock()

	groupID := op.RaftGroupID()

	if _, exists := c.raftGroupOpMgr.raftGroupOps[groupID]; exists {
		return false
	}
	if len(c.raftGroupOpMgr.raftGroupOps) > 0 {
		return false
	}
	c.raftGroupOpMgr.raftGroupOps[groupID] = op
	return true
}

func (c *Coordinator) runScheduler(s schedulers.Scheduler) {
	defer c.wg.Done()
	ticker := time.NewTicker(s.NextCheckInterval())
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			ticker.Reset(s.NextCheckInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(c.infoManager); op != nil {
				_ = c.addRaftGroupOp(op)
			}
		}
	}
}
