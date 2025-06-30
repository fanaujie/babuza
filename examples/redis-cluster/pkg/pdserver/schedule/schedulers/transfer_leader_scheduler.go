package schedulers

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/infostore"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/operator"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"math"
	"time"
)

var _ Scheduler = (*transferLeaderScheduler)(nil)

type transferLeaderScheduler struct {
	name         string
	limit        int
	nextInterval time.Duration
}

func NewTransferLeaderScheduler(name string) Scheduler {
	return &transferLeaderScheduler{
		name:         name,
		nextInterval: minScheduleInterval,
	}
}

func (t *transferLeaderScheduler) Name() string {
	return t.name
}

func (t *transferLeaderScheduler) NextCheckInterval() time.Duration {
	return t.nextInterval
}

func (t *transferLeaderScheduler) AllowSchedule() bool {
	return true
}

func (t *transferLeaderScheduler) Schedule(manager infostore.InfoManager) operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		sourceGroupInfo, targetPeer, ok := t.doSchedule(manager)
		if ok {
			sourceStore, find := manager.Store(sourceGroupInfo.StoreID())
			if !find {
				return nil
			}
			targetStore, find := manager.Store(targetPeer.StoreID)
			if !find {
				return nil
			}
			if !shouldBalance(sourceStore, targetStore) {
				return nil
			}
			t.nextInterval = minScheduleInterval
			return operator.NewTransferLeaderOperator(
				sourceGroupInfo.GroupID(), targetPeer)
		}
	}
	// exponential growth of interval
	t.nextInterval = t.nextInterval * 2
	if t.nextInterval > maxScheduleInterval {
		t.nextInterval = maxScheduleInterval
	}
	return nil
}

func (t *transferLeaderScheduler) doSchedule(manager infostore.InfoManager) (infostore.GroupInfo, babuzapb.RaftPeerAttribute, bool) {
	stores := manager.Stores()
	if len(stores) == 0 {
		return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
	}
	var averageLeader float64
	for _, s := range stores {
		averageLeader += float64(s.LeaderCount()) / float64(len(stores))
	}
	mostLeaderStore, leastLeaderStore := findMostAndLeastStore(stores)
	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderStore != nil {
		mostLeaderDistance = math.Abs(float64(mostLeaderStore.LeaderCount()) - averageLeader)
	}
	if leastLeaderStore != nil {
		leastLeaderDistance = math.Abs(float64(leastLeaderStore.LeaderCount()) - averageLeader)
	}
	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
	}

	if mostLeaderDistance > leastLeaderDistance {
		// transfer out
		sourceGroupInfo, find := manager.RandomLeaderRaftGroupOnStore(mostLeaderStore.StoreID())
		if !find {
			return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
		}
		// randomly select a new leader from group peers
		targetPeer, find := sourceGroupInfo.RandomFollower()
		if !find {
			return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
		}
		return sourceGroupInfo, targetPeer, true
	}
	// transfer in
	// randomly select a follower group
	sourceGroupInfo, find := manager.RandomFollowerRaftGroupOnStore(leastLeaderStore.StoreID())
	if !find {
		return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
	}
	targetPeer, find := sourceGroupInfo.PeerOnStore(leastLeaderStore.StoreID())
	if !find {
		return infostore.GroupInfo{}, babuzapb.RaftPeerAttribute{}, false
	}
	return sourceGroupInfo, targetPeer, true
}

func findMostAndLeastStore(stores []infostore.StoreInfo) (*infostore.StoreInfo, *infostore.StoreInfo) {
	mostStoreLeader := uint64(0)
	leastStoreLeader := uint64(0)
	var mostLeaderStore, leastLeaderStore *infostore.StoreInfo
	for _, store := range stores {
		if store.LeaderCount() > mostStoreLeader {
			mostStoreLeader = store.LeaderCount()
			mostLeaderStore = &store
		}
		if leastStoreLeader == 0 || store.LeaderCount() < leastStoreLeader {
			leastStoreLeader = store.LeaderCount()
			leastLeaderStore = &store
		}
	}
	return mostLeaderStore, leastLeaderStore
}

func shouldBalance(source, target infostore.StoreInfo) bool {
	sourceScore := source.LeaderCount()
	targetScore := target.LeaderCount()
	if targetScore >= sourceScore {
		return false
	}
	return sourceScore > targetScore+2
}
